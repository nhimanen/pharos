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
#
# Env vars honored:
#   PHAROS_PORT   override daemon port (default: 7171)
#   PHAROS_HEAP   pass -Xmx<value> to the daemon JVM (e.g. PHAROS_HEAP=16g)

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
for arg in "$@"; do
  case "$arg" in
    --skip-build)   SKIP_BUILD=true ;;
    --restart-only) RESTART_ONLY=true ;;
  esac
done

# ── 1. Build ──────────────────────────────────────────────────────────────────

if [ "$RESTART_ONLY" = false ] && [ "$SKIP_BUILD" = false ]; then
  echo "Building pharos..."
  cd "$REPO_DIR"
  mvn clean package -DskipTests -q
  echo "Build complete."
fi

# ── 2. Locate JAR ─────────────────────────────────────────────────────────────

JAR="$(ls "$REPO_DIR"/target/pharos-*.jar 2>/dev/null | sort -V | tail -1)"
if [ -z "$JAR" ]; then
  echo "Error: no JAR found in $REPO_DIR/target/. Run without --skip-build first." >&2
  exit 1
fi
JAR_NAME="$(basename "$JAR")"
echo "Using JAR: $JAR_NAME"

# ── 3. Install files ──────────────────────────────────────────────────────────

if [ "$RESTART_ONLY" = false ]; then
  mkdir -p "$PHAROS_BIN" "$LOCAL_BIN"

  echo "Installing $PHAROS_BIN/pharos.jar..."
  cp "$JAR" "$PHAROS_BIN/pharos.jar"

  echo "Installing $PHAROS_BIN/pharos-cli.py..."
  cp "$REPO_DIR/pharos" "$PHAROS_BIN/pharos-cli.py"
  chmod +x "$PHAROS_BIN/pharos-cli.py"

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
  cp -r "$REPO_DIR/.claude/skills/pharos" "$CLAUDE_SKILLS/"
fi

# ── 4. Restart daemon (always, unless --restart-only was used with no daemon) ──

if [ "$RESTART_ONLY" = true ] && ! curl -s "http://localhost:$DAEMON_PORT/health" >/dev/null 2>&1; then
  echo "Daemon is not running on port $DAEMON_PORT — nothing to restart."
else
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
    "--add-modules" "jdk.incubator.vector"
    "-XX:+HeapDumpOnOutOfMemoryError"
    "-XX:HeapDumpPath=$PHAROS_DUMPS"
  )
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
  for i in $(seq 1 60); do
    if curl -s "http://localhost:$DAEMON_PORT/health" >/dev/null 2>&1; then
      echo " ready (pid=$NEW_PID, port=$DAEMON_PORT)"
      break
    fi
    echo -n "."
    sleep 0.5
  done
fi

echo ""
echo "Done. pharos $(java -jar "$PHAROS_BIN/pharos.jar" --version 2>/dev/null || echo '(version unknown)') installed."
echo "  JAR:    $PHAROS_BIN/pharos.jar"
echo "  Client: $PHAROS_BIN/pharos-cli.py"
echo "  Path:   $LOCAL_BIN/pharos"
