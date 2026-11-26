#!/usr/bin/env bash
# setup.sh — build pharos and install the JAR + Python client system-wide.
#
# What it does:
#   1. Builds the fat JAR with Maven (skipping tests for speed).
#   2. Copies the JAR to ~/.pharos/bin/pharos.jar.
#   3. Copies the pharos Python CLI script to ~/.pharos/bin/pharos-cli.py
#      and ensures ~/.local/bin/pharos is a symlink to it.
#   4. Restarts the daemon if it was already running (so the new JAR is picked up).
#
# Usage:
#   ./setup.sh            # build + install
#   ./setup.sh --skip-build   # install without rebuilding (uses existing target/ JAR)
#   ./setup.sh --restart-only # just restart a running daemon
#   ./setup.sh --release latest|vX.Y.Z   # install a GitHub release (no Maven needed)
#   ./setup.sh --yes          # skip the upgrade-warning prompt
#   ./setup.sh --upgrade-indexes  # re-index projects whose index format is behind
#                                 # (passes --yes through), then restart the daemon;
#                                 # with --release: install first, then upgrade
#
# Env vars honored:
#   PHAROS_PORT   override daemon port (default: 7171)
#   PHAROS_HEAP   pass -Xmx<value> to the daemon JVM (e.g. PHAROS_HEAP=16g)
#   PHAROS_RELEASE_BASE_URL  release download base (default
#                 https://github.com/nhimanen/pharos/releases/download); "latest" is then
#                 read from $PHAROS_RELEASE_BASE_URL/latest (a file holding a tag)
#   PHAROS_NO_DAEMON=1       do not start/restart the daemon (used by tests)
#   PHAROS_ARITHMETIC=deterministic|native  Lucene's vector arithmetic for the daemon (default
#                 deterministic on cluster.role reader, native otherwise; see README)
#
# Exit codes: 2 = upgrade warning declined / needs --yes, 3 = checksum mismatch;
# with --upgrade-indexes, the exit status of upgrade-indexes (0 done, 1 skipped/failed,
# 2 needs --yes, 5 refused on a cluster reader node).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── 0. Java check ─────────────────────────────────────────────────────────────

REQUIRED_JAVA=25

_java_version() {
  java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1
}

_install_java_sdkman() {
  local version="$1"
  echo "Java $version not found. Attempting to install via SDKMAN..."

  # Install SDKMAN if not present
  if [ ! -f "$HOME/.sdkman/bin/sdkman-init.sh" ]; then
    echo "Installing SDKMAN..."
    curl -s "https://get.sdkman.io" | bash
  fi

  # shellcheck disable=SC1091
  source "$HOME/.sdkman/bin/sdkman-init.sh"
  sdk install java "${version}-tem" || sdk install java "${version}.0.0-tem" || {
    echo "Error: could not install Java $version via SDKMAN." >&2
    echo "Please install Java $version manually: https://sdkman.io/usage" >&2
    exit 1
  }
  sdk default java "$(sdk list java 2>/dev/null | grep -oP '\d+\.\S+tem' | grep "^${version}" | head -1)"
  echo "Java $version installed via SDKMAN."
}

if ! command -v java &>/dev/null; then
  _install_java_sdkman "$REQUIRED_JAVA"
else
  JAVA_VER="$(_java_version)"
  if [ -z "$JAVA_VER" ] || [ "$JAVA_VER" -lt "$REQUIRED_JAVA" ] 2>/dev/null; then
    echo "Warning: Java $JAVA_VER found but Java $REQUIRED_JAVA+ is required."
    _install_java_sdkman "$REQUIRED_JAVA"
  else
    echo "Java $JAVA_VER found. ✓"
  fi
fi
PHAROS_BIN="$HOME/.pharos/bin"
PHAROS_DUMPS="$HOME/.pharos/dumps"
LOCAL_BIN="$HOME/.local/bin"
DAEMON_PID_FILE="$HOME/.pharos/daemon.pid"
DAEMON_PORT="${PHAROS_PORT:-7171}"

SKIP_BUILD=false
RESTART_ONLY=false
ASSUME_YES=false
UPGRADE_INDEXES=false
RELEASE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build)      SKIP_BUILD=true ;;
    --restart-only)    RESTART_ONLY=true ;;
    --yes|-y)          ASSUME_YES=true ;;
    --upgrade-indexes) UPGRADE_INDEXES=true ;;
    --release)
      if [ $# -lt 2 ]; then echo "Error: --release needs 'latest' or a tag like v1.4.0" >&2; exit 1; fi
      RELEASE="$2"; shift ;;
    --release=*)       RELEASE="${1#--release=}" ;;
    *) echo "Warning: ignoring unknown argument: $1" >&2 ;;
  esac
  shift
done

# The JVM resolves user.home from the passwd entry, not $HOME; pin it so the registry and
# index dirs seen by the jar are the ones under $HOME/.pharos.
# OPT_OUT_TRACKING: DJL telemetry would otherwise probe the EC2 metadata endpoint.
PHAROS_JVM_HOME_ARGS=("-Duser.home=$HOME" "-Dpharos.home=$HOME/.pharos" "-DOPT_OUT_TRACKING=true")

# ── Daemon helpers ────────────────────────────────────────────────────────────

# Ruling CPU-R1: Lucene's vector arithmetic flags (Lucene reads them once, at class load). A reader
# (cluster.role reader) runs Lucene's scalar code with FMA off, without the vector incubator module, so every
# CPU type scores with the same bits; anything else keeps Lucene's per-CPU SIMD choice.
# PHAROS_ARITHMETIC=deterministic|native overrides. Same rule as the Python client's _arithmetic_flags()
# and ArithmeticProfile.DETERMINISTIC_FLAGS.
_arithmetic_args() { # -> one JVM argument per line
  local mode="${PHAROS_ARITHMETIC:-}" native_property=native
  if [ "$mode" != deterministic ] && [ "$mode" != native ]; then
    # only the role's default: native-default, so a node that serves as a reader anyway still warns
    if [ "$(_cluster_role)" = reader ]; then mode=deterministic; else mode=native; native_property=native-default; fi
  fi
  if [ "$mode" = deterministic ]; then
    printf '%s\n' -Dlucene.useScalarFMA=false -Dlucene.useVectorFMA=false -Dpharos.arithmetic=deterministic
  else
    printf '%s\n' --add-modules jdk.incubator.vector "-Dpharos.arithmetic=$native_property"
  fi
}

_restart_daemon() {
  if [ "${PHAROS_NO_DAEMON:-}" = 1 ]; then
    echo "PHAROS_NO_DAEMON=1 — not starting the daemon."
    return 0
  fi
  # Stop any existing daemon
  if [ -f "$DAEMON_PID_FILE" ]; then
    OLD_PID="$(cat "$DAEMON_PID_FILE")"
    kill "$OLD_PID" 2>/dev/null || true
    rm -f "$DAEMON_PID_FILE"
  fi
  kill "$(lsof -ti:"$DAEMON_PORT" 2>/dev/null)" 2>/dev/null || true
  sleep 1

  mkdir -p "$PHAROS_DUMPS"
  # Build JVM args: shared flags + optional -Xmx from PHAROS_HEAP
  JVM_ARGS=(
    "--enable-native-access=ALL-UNNAMED"
    "--add-opens" "java.base/java.nio.channels.spi=ALL-UNNAMED"
    "-XX:+HeapDumpOnOutOfMemoryError"
    "-XX:HeapDumpPath=$PHAROS_DUMPS"
    "-DOPT_OUT_TRACKING=true"
  )
  local arithmetic
  while IFS= read -r arithmetic; do JVM_ARGS+=("$arithmetic"); done < <(_arithmetic_args)
  if [ -n "${PHAROS_HEAP:-}" ]; then
    JVM_ARGS=("-Xmx${PHAROS_HEAP}" "${JVM_ARGS[@]}")
    echo "Daemon heap: -Xmx${PHAROS_HEAP}"
  fi
  echo "Starting daemon on port $DAEMON_PORT..."
  nohup java "${JVM_ARGS[@]}" \
    -jar "$PHAROS_BIN/pharos.jar" web --port "$DAEMON_PORT" \
    >> "$HOME/.pharos/daemon.log" 2>&1 &
  NEW_PID=$!
  echo "$NEW_PID" > "$DAEMON_PID_FILE"

  echo -n "Waiting for daemon..."
  for _ in $(seq 1 60); do
    if _daemon_running; then
      echo " ready (pid=$NEW_PID, port=$DAEMON_PORT)"
      break
    fi
    echo -n "."
    sleep 0.5
  done
}

# Copy src to a fresh file next to dest, then rename over it: dest gets a new inode, so a
# running process (an open jar, or this very script) is never overwritten in place.
_install_file() { # src dest [mode]
  local src="$1" dest="$2" mode="${3:-}"
  local tmp="$dest.new.$$"
  cp "$src" "$tmp"
  [ -n "$mode" ] && chmod "$mode" "$tmp"
  mv -f "$tmp" "$dest"
}

# ── --upgrade-indexes: re-index behind-format projects with the installed jar ──

# Runs through the installed Python client, so the JVM gets the same flags as every other
# pharos command (heap sizing, --add-opens, vector module, native access, pharos.home).
# The client passes the terminal through, so the confirmation prompt still works.
# The configured cluster role ("all" when unset or unreadable); a reader refuses upgrade-indexes.
_cluster_role() {
  python3 - "$HOME/.pharos/config.json" 2>/dev/null <<'PY' || echo all
import json, sys
try:
    print((json.load(open(sys.argv[1])).get("cluster") or {}).get("role") or "all")
except Exception:
    print("all")
PY
}

# True on a work-queue writer: role writer without "writer": {"workQueue": false} (the default since
# Phase 4). Its work loop upgrades every shard itself; 'pharos upgrade-indexes' refuses there (exit 5).
_work_queue_writer() {
  python3 - "$HOME/.pharos/config.json" 2>/dev/null <<'PY'
import json, sys
try:
    c = json.load(open(sys.argv[1])).get("cluster") or {}
except Exception:
    sys.exit(1)
sys.exit(0 if c.get("role") == "writer" and (c.get("writer") or {}).get("workQueue", True) is not False else 1)
PY
}

_run_upgrade_indexes() { # -> exit status of upgrade-indexes
  if [ ! -f "$PHAROS_BIN/pharos.jar" ] || [ ! -f "$PHAROS_BIN/pharos-cli.py" ]; then
    echo "Error: pharos is not installed in $PHAROS_BIN; run setup first." >&2
    return 1
  fi
  # Same refusal as 'pharos upgrade-indexes' on a reader (exit 5), without starting a JVM.
  if [ "$(_cluster_role)" = reader ]; then
    echo "Error: this node has role reader; readers receive upgrades by syncing; upgrade the writer." >&2
    echo "  (cluster.role in $HOME/.pharos/config.json. Work-queue writers, the default, upgrade by themselves:" >&2
    echo "   restart them on the new release and their work loops re-index and re-publish every shard. A writer" >&2
    echo "   with \"workQueue\": false: ./setup.sh --upgrade-indexes there. This reader picks the new generations" >&2
    echo "   up on its next sync)" >&2
    return 5
  fi
  # Same refusal as 'pharos upgrade-indexes' on a work-queue writer (exit 5).
  if _work_queue_writer; then
    echo "Error: this writer runs in work-queue mode (cluster.writer.workQueue: true): its work loop re-indexes" >&2
    echo "  and re-publishes every shard whose index format, chunking version or model is out of date. To upgrade," >&2
    echo "  restart 'pharos web' on the new release; nothing to run by hand. (Set cluster.writer.workQueue to false" >&2
    echo "  in $HOME/.pharos/config.json to index and upgrade by hand.)" >&2
    return 5
  fi
  local args=()
  [ "$ASSUME_YES" = true ] && args+=(--yes)
  python3 "$PHAROS_BIN/pharos-cli.py" upgrade-indexes ${args[@]+"${args[@]}"}
}

# Alone, --upgrade-indexes only upgrades; with --release it runs after the install (step 5).
if [ "$UPGRADE_INDEXES" = true ] && [ -z "$RELEASE" ]; then
  WAS_RUNNING=false
  _daemon_running && WAS_RUNNING=true
  UP_RC=0
  _run_upgrade_indexes || UP_RC=$?
  # A reader's or work-queue writer's refusal changed nothing: leave its daemon alone.
  if [ "$WAS_RUNNING" = true ] && [ "$UP_RC" != 5 ]; then
    _restart_daemon
  fi
  exit "$UP_RC"
fi

# ── 1. Obtain the new files (build from source, or download a release) ───────

TMP_DIR=""
cleanup() { [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"; return 0; }
trap cleanup EXIT

NEW_JAR="" NEW_CLI="" NEW_SETUP="" NEW_SKILL_SRC=""
RELEASE_BASE="${PHAROS_RELEASE_BASE_URL:-https://github.com/nhimanen/pharos/releases/download}"

# Release tags are plain vX.Y.Z; anything else never reaches a path or URL.
_valid_tag() { [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; }

_resolve_tag() {
  local want="$1"
  [ "$want" != latest ] && { echo "$want"; return; }
  if [ -n "${PHAROS_RELEASE_BASE_URL:-}" ]; then
    curl -fsSL "$PHAROS_RELEASE_BASE_URL/latest" | tr -d '[:space:]'
  else
    curl -fsSL "https://api.github.com/repos/nhimanen/pharos/releases/latest" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"])'
  fi
}

if [ "$RESTART_ONLY" = false ]; then
  if [ -n "$RELEASE" ]; then
    TAG="$(_resolve_tag "$RELEASE")" || { echo "Error: could not resolve release '$RELEASE'." >&2; exit 1; }
    [ -n "$TAG" ] || { echo "Error: empty release tag for '$RELEASE'." >&2; exit 1; }
    if ! _valid_tag "$TAG"; then
      echo "Error: invalid release tag '$TAG' (expected 'latest' or vX.Y.Z) — nothing was installed." >&2
      exit 1
    fi
    VER="${TAG#v}"
    TMP_DIR="$(mktemp -d)"
    echo "Downloading pharos $TAG..."
    for asset in "pharos-$VER.jar" "pharos-$VER.jar.sha256" pharos setup.sh pharos-skill.tar.gz; do
      curl -fsSL -o "$TMP_DIR/$asset" "$RELEASE_BASE/$TAG/$asset" \
        || { echo "Error: failed to download $RELEASE_BASE/$TAG/$asset" >&2; exit 1; }
    done

    # Verify the jar BEFORE anything under ~/.pharos/bin is touched. The checksum file must
    # hold exactly one line, naming exactly this jar: "-c" would otherwise verify whichever
    # file the line names.
    SUM_FILE="$TMP_DIR/pharos-$VER.jar.sha256"
    SUM_NAME="$(awk 'NF { n++; f = $2 } END { if (n == 1) print f }' "$SUM_FILE")"
    SUM_NAME="${SUM_NAME#\*}"  # binary-mode marker
    if [ "$SUM_NAME" != "pharos-$VER.jar" ]; then
      echo "Error: pharos-$VER.jar.sha256 does not name pharos-$VER.jar — aborting; nothing was installed." >&2
      exit 3
    fi
    if command -v sha256sum >/dev/null 2>&1; then
      SUM_CMD=(sha256sum -c)
    else
      SUM_CMD=(shasum -a 256 -c)
    fi
    if ! (cd "$TMP_DIR" && "${SUM_CMD[@]}" "pharos-$VER.jar.sha256" >/dev/null 2>&1); then
      echo "Error: checksum mismatch for pharos-$VER.jar — aborting; nothing was installed." >&2
      exit 3
    fi
    echo "Checksum OK."

    NEW_JAR="$TMP_DIR/pharos-$VER.jar"
    NEW_CLI="$TMP_DIR/pharos"
    NEW_SETUP="$TMP_DIR/setup.sh"
    mkdir -p "$TMP_DIR/skill"
    tar -xzf "$TMP_DIR/pharos-skill.tar.gz" -C "$TMP_DIR/skill"
    NEW_SKILL_SRC="$TMP_DIR/skill/pharos"
  else
    if [ "$SKIP_BUILD" = false ]; then
      echo "Building pharos..."
      cd "$REPO_DIR"
      mvn clean package -DskipTests -q
      echo "Build complete."
    fi
    NEW_JAR="$(ls "$REPO_DIR"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -V | tail -1)"
    if [ -z "$NEW_JAR" ]; then
      echo "Error: no JAR found in $REPO_DIR/target/. Run without --skip-build first." >&2
      exit 1
    fi
    NEW_CLI="$REPO_DIR/pharos"
    NEW_SETUP="$REPO_DIR/setup.sh"
    NEW_SKILL_SRC="$REPO_DIR/.claude/skills/pharos"
  fi
  echo "Using JAR: $(basename "$NEW_JAR")"
fi

# Validate the staged files before anything is mutated.
if [ "$RESTART_ONLY" = false ]; then
  for f in "$NEW_JAR" "$NEW_CLI" "$NEW_SETUP"; do
    if [ ! -s "$f" ]; then
      echo "Error: staged file missing or empty: $f — nothing was installed." >&2
      exit 1
    fi
  done
  if [ ! -f "$NEW_SKILL_SRC/SKILL.md" ]; then
    echo "Error: skill bundle has no pharos/SKILL.md — nothing was installed." >&2
    exit 1
  fi
fi

# ── 2. Upgrade warning (both modes) ───────────────────────────────────────────
# Ask the NEW jar which registered projects its index format cannot serve.

# Confirm helper: prompts unless --yes; no TTY + no --yes => exit 2 with the old install untouched.
_confirm_or_abort() {
  if [ "$ASSUME_YES" = true ]; then return 0; fi
  if [ ! -t 0 ]; then
    echo "Aborting: no terminal to confirm. Re-run with --yes to install anyway. The existing install is untouched." >&2
    exit 2
  fi
  read -r -p "Continue installing? [y/N] " ANS
  case "$ANS" in
    y|Y|yes|YES) ;;
    *) echo "Aborted. The existing install is untouched."; exit 2 ;;
  esac
}

# A reader keeps its registry in the cluster cache dir, not in registry.json: run the check
# whenever there is a registry or a config (index-status reads the right registry per role).
if [ "$RESTART_ONLY" = false ] && { [ -f "$HOME/.pharos/registry.json" ] || [ -f "$HOME/.pharos/config.json" ]; }; then
  [ -n "$TMP_DIR" ] || TMP_DIR="$(mktemp -d)"
  STATUS_ERR="$TMP_DIR/index-status.err"
  STATUS_JSON=""
  STATUS_RC=0
  STATUS_JSON="$(java "${PHAROS_JVM_HOME_ARGS[@]}" -jar "$NEW_JAR" index-status --json 2>"$STATUS_ERR")" || STATUS_RC=$?
  WARN_RC=0
  WARN_TEXT=""
  CAUSE=""
  if [ "$STATUS_RC" -ne 0 ]; then
    CAUSE="the new jar's 'index-status --json' exited with status $STATUS_RC"
  else
    WQ=0
    _work_queue_writer && WQ=1
    WARN_TEXT="$(printf '%s' "$STATUS_JSON" | PHAROS_WQ="$WQ" python3 -c '
import json, os, sys
d = json.load(sys.stdin)
rows = d.get("projects", [])
# Readers do not re-index: the writer upgrades and re-publishes, readers sync.
reader = d.get("role") == "reader"
# A work-queue writer re-indexes them itself once it runs the new release.
wq = os.environ.get("PHAROS_WQ") == "1"

def human(b):
    v = float(b)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024 or u == "TB":
            return ("%d %s" % (v, u)) if u == "B" else ("%.0f %s" % (v, u))
        v /= 1024

def line(r, with_cmd):
    last = (r.get("lastIndexed") or "never")[:10]
    s = "  %-12s (%s, last indexed %s)" % (r["name"], human(r.get("indexBytes", 0)), last)
    if with_cmd and not reader and not wq:
        if r.get("rootExists"):
            s += "   pharos index --full " + r["name"]
        else:
            s += "   (root path gone; re-index manually)"
    return s

blocked = [r for r in rows if r["status"] in ("outdated", "newer")]
soft = [r for r in rows if r["status"] == "upgrade-available"]
missing = [r for r in rows if r["status"] == "missing"]
invalid = [r for r in rows if r["status"] == "invalid"]
if not (blocked or soft or missing or invalid):
    sys.exit(0)
out = []
if blocked:
    out.append("This version needs index format %s. These projects must be re-indexed before you can search them:" % d.get("requiredFormat"))
    out += [line(r, True) + ("   [index is newer than this pharos]" if r["status"] == "newer" else "") for r in blocked]
if soft:
    out.append("These projects stay searchable now (index format %s, upgrade available); re-index when convenient:" % (d.get("minReadableFormat")))
    out += [line(r, True) for r in soft]
if missing:
    out.append("These projects have no index on disk (index missing):")
    out += ["  " + r["name"] for r in missing]
if invalid:
    out.append("These registry entries have an invalid project name; pharos ignores them and will not delete anything for them.")
    out.append("Remove each from ~/.pharos/registry.json by hand, then re-index with: pharos index --project <name> <path>")
    out += ["  '" + r["name"] + "'" + ("  (root: " + r["rootPath"] + ")" if r.get("rootPath") else "") for r in invalid]
if reader:
    out.append("(this is a reader node: upgrade and re-publish these projects on the writer (a work-queue writer does it by itself once restarted on the new release); this reader picks the new generations up by syncing)")
elif wq:
    out.append("(this is a work-queue writer: after the install, restart pharos web; its work loop re-indexes and re-publishes these projects by itself)")
else:
    out.append("(re-index afterwards with: ./setup.sh --upgrade-indexes)")
print("\n".join(out))
sys.exit(10 if blocked else 0)
' 2>"$STATUS_ERR")" || WARN_RC=$?
    if [ "$WARN_RC" -ne 0 ] && [ "$WARN_RC" -ne 10 ]; then
      CAUSE="the output of 'index-status --json' could not be parsed"
    fi
  fi
  if [ -n "$CAUSE" ]; then
    {
      echo "WARNING: could not determine which project indexes this version can serve: $CAUSE."
      if [ -s "$STATUS_ERR" ]; then
        echo "  First lines of its error output:"
        head -n 5 "$STATUS_ERR" | sed 's/^/    /'
      fi
      echo "  Indexed projects may become unsearchable after installing."
    } >&2
    _confirm_or_abort
  elif [ -n "$WARN_TEXT" ]; then
    echo "$WARN_TEXT"
    if [ "$WARN_RC" -eq 10 ]; then _confirm_or_abort; fi
  fi
fi

# ── 3. Install files ──────────────────────────────────────────────────────────

if [ "$RESTART_ONLY" = false ]; then
  mkdir -p "$PHAROS_BIN" "$LOCAL_BIN"

  echo "Installing $PHAROS_BIN/pharos.jar..."
  _install_file "$NEW_JAR" "$PHAROS_BIN/pharos.jar"

  echo "Installing $PHAROS_BIN/pharos-cli.py..."
  _install_file "$NEW_CLI" "$PHAROS_BIN/pharos-cli.py" +x

  echo "Installing $PHAROS_BIN/setup.sh..."
  _install_file "$NEW_SETUP" "$PHAROS_BIN/setup.sh" +x

  LINK="$LOCAL_BIN/pharos"
  if [ -L "$LINK" ] || [ ! -e "$LINK" ]; then
    ln -sf "$PHAROS_BIN/pharos-cli.py" "$LINK"
    echo "Symlink: $LINK -> $PHAROS_BIN/pharos-cli.py"
  else
    echo "Warning: $LINK exists and is not a symlink — skipping. Remove it manually if needed."
  fi

  CLAUDE_SKILLS="$HOME/.claude/skills"
  echo "Installing Claude skill -> $CLAUDE_SKILLS/pharos/..."
  mkdir -p "$CLAUDE_SKILLS"
  rm -rf "$CLAUDE_SKILLS/pharos.new.$$" "$CLAUDE_SKILLS/pharos.old.$$"
  cp -r "$NEW_SKILL_SRC" "$CLAUDE_SKILLS/pharos.new.$$"
  if [ -e "$CLAUDE_SKILLS/pharos" ]; then mv "$CLAUDE_SKILLS/pharos" "$CLAUDE_SKILLS/pharos.old.$$"; fi
  if mv "$CLAUDE_SKILLS/pharos.new.$$" "$CLAUDE_SKILLS/pharos"; then
    rm -rf "$CLAUDE_SKILLS/pharos.old.$$"
  else
    echo "Error: could not install the skill; restoring the previous one." >&2
    [ -e "$CLAUDE_SKILLS/pharos.old.$$" ] && mv "$CLAUDE_SKILLS/pharos.old.$$" "$CLAUDE_SKILLS/pharos"
    exit 1
  fi
fi

# ── 4. Restart daemon (always, unless --restart-only was used with no daemon) ──

if [ "$RESTART_ONLY" = true ] && ! _daemon_running; then
  echo "Daemon is not running on port $DAEMON_PORT — nothing to restart."
else
  _restart_daemon
fi

# ── 5. --release --upgrade-indexes: upgrade with the newly installed version ──
# After the daemon restart above, so no reader of the old version is left running.

UP_RC=0
if [ "$UPGRADE_INDEXES" = true ]; then
  echo ""
  echo "Upgrading indexes..."
  _run_upgrade_indexes || UP_RC=$?
fi

echo ""
# `--version` already prints "pharos <version>", so do not prefix another "pharos".
echo "Done. $(java "${PHAROS_JVM_HOME_ARGS[@]}" -jar "$PHAROS_BIN/pharos.jar" --version 2>/dev/null || echo 'pharos (version unknown)') installed."
echo "  JAR:    $PHAROS_BIN/pharos.jar"
echo "  Client: $PHAROS_BIN/pharos-cli.py"
echo "  Path:   $LOCAL_BIN/pharos"
# Hint only when embeddings are not configured (first line of `embeddings status`).
EMB_STATE="$(java "${PHAROS_JVM_HOME_ARGS[@]}" -jar "$PHAROS_BIN/pharos.jar" embeddings status 2>/dev/null | head -1 || true)"
case "$EMB_STATE" in
  "Semantic search: off"*)
    echo ""
    echo "Semantic search is off; enable with \`pharos embeddings enable\`."
    ;;
esac
exit "$UP_RC"
