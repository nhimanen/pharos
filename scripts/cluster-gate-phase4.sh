#!/usr/bin/env bash
# Phase 4 gate: writer leases, failover, work-queue scaling, /cluster/status proposals and the writer
# disk guard, against a local MinIO. See docs/superpowers/plans/2026-11-17-multi-node-phase-4.md (Task 7).
#
# This script starts MinIO, builds four local bare git repos under $GATE/repos (snapshots, never the
# user's repositories themselves):
#   lucene   `git archive HEAD` of the registry's lucene rootPath (indexed --no-embed)
#   pharos   `git archive HEAD src/main pom.xml` of the registry's pharos rootPath (embedded)
#   alpha, beta   two small Java fixture projects written inline (--no-embed)
# A bootstrap writer (role writer, workQueue false: hand runs, implicit lease) indexes each from its
# edit clone and publishes it with `--source-git <bare repo> --ref main`, which records the source in
# the catalog. Then scripts/cluster_gate_phase4.py runs the gate: readers R1 R2 (all shards, lazy), the
# router, ~10 QPS of keyword load, and work-queue writers started and stopped one at a time, by PID.
#
# It NEVER downloads anything and never touches the real ~/.pharos or ~/.djl.ai: every JVM gets an
# isolated user.home and pharos.home under $GATE; the jina embedding model is COPIED from ~/.djl.ai
# into $GATE/models-home/.djl.ai (each node's .djl.ai is a symlink to that copy). The lucene and pharos
# source paths are only READ from ~/.pharos/registry.json, and the sources only through `git archive`.
#
# On a shared host, run it under scripts/gate-watchdog.sh: one hard memory cap for every gate process
# (a systemd user scope, or a cgroup-free fallback), and a watchdog that stops every recorded PID with
# SIGTERM, then SIGKILL, when MemAvailable runs low, e.g.
#   GATE=/home/me/gate4-scratch/run1 bash scripts/gate-watchdog.sh --state-dir /home/me/gate4-scratch/run1.wd \
#     --mem-max 9G --min-available 3072 -- bash scripts/cluster-gate-phase4.sh
# With GATE_KEYWORD_ONLY=1 (no embedding model anywhere) --mem-max 6G is enough.
#
# Env:
#   MINIO_BIN          minio server binary (default $HOME/.cache/pharos-gate/minio)
#   GATE               work dir (required unless --dry-run; on disk, not on a tmpfs /tmp)
#   GATE_JAR           pharos jar to run (default target/pharos-*.jar; copied to $GATE/pharos.jar)
#   PHAROS_INDEX_HEAP  heap of the bootstrap index runs (default 3g)
#   GATE_WRITER_HEAP   heap of each work-queue writer (default 2g)
#   GATE_WEB_HEAP      heap of each reader and the router (default 640m)
#   GATE_MODEL_URL     embedding model (default hf://jinaai/jina-embeddings-v2-base-code, 768 dims)
#   GATE_MODEL_ID      its provider modelId (default jina-code-v2)
#   GATE_REUSE=1       skip the repos and the bootstrap when $GATE already holds them (re-runs of the driver)
#   GATE_PHASES        driver phases (default "failover,scale,proposals,disk,check")
#   GATE_MIN_FREE_GIB  refuse to start below this much free space on $GATE's file system (default 18)
#   GATE_KEYWORD_ONLY  0 (default); 1: no embedding model on any node: pharos is bootstrapped --no-embed too,
#                      writers get no embedding provider, and K2 kills the GATE_K2_SHARD holder in any
#                      indexing phase instead of pharos's embed phase (failover, takeover, scaling, status
#                      and the disk guard without ~1.1 GB of jina per embedding writer; the "provider
#                      calls <= changed files" check is then trivially 0)
#   GATE_K2_SHARD      shard K2 changes and kills the holder of (default pharos; lucene with GATE_KEYWORD_ONLY=1)
#   GATE_K2_PHASE      heartbeat phase K2 kills in: embed|parse|commit|publish|any (default embed; any with
#                      GATE_KEYWORD_ONLY=1)
#   GATE_LUCENE_SRC / GATE_PHAROS_SRC  git work trees snapshotted with `git archive HEAD` (default: their
#                      rootPath in ~/.pharos/registry.json, only read)
#   GATE_MODELS_SRC    .djl.ai dir the model is copied from (default ~/.djl.ai)
#   GATE_SEED_FROM     dir holding pharos/embed-cache.<modelId>.{bin,meta} or pharos/embed-cache.{bin,meta}:
#                      a COPY seeds the bootstrap writer's pharos embedding cache (default unset: no seed,
#                      the bootstrap embeds pharos cold, ~17 min of CPU)
#   GATE_MINIO_PORT / GATE_MINIO_CONSOLE_PORT  MinIO ports (default 9000 / 9001)
#   GATE_ROUTER_PORT / GATE_WEBHOOK_PORT       router and the driver's webhook sink (default 7080 / 7390)
#   GATE_R1_PORT / GATE_R2_PORT                readers (default 7401 / 7402)
#   GATE_W1_PORT .. GATE_W6_PORT / GATE_WLOW_PORT  writers (default 7411..7416 / 7417)
#   GATE_PORT_PROBE_TIMEOUT  seconds per port probe and curl connect (default 2; on hosts where an
#                            unused port hangs instead of refusing, a timed-out probe counts as free)
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,59p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
DRY=0
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "cluster-gate-phase4: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_BIN="${MINIO_BIN:-$HOME/.cache/pharos-gate/minio}"
INDEX_HEAP="${PHAROS_INDEX_HEAP:-3g}"
MODEL_URL="${GATE_MODEL_URL:-hf://jinaai/jina-embeddings-v2-base-code}"
MODEL_DIR_NAME="$(echo "${MODEL_URL#hf://}" | tr '/' '-')"
MODEL_ID="${GATE_MODEL_ID:-jina-code-v2}"
PHASES="${GATE_PHASES:-failover,scale,proposals,disk,check}"
MIN_FREE_GIB="${GATE_MIN_FREE_GIB:-18}"
KEYWORD_ONLY="${GATE_KEYWORD_ONLY:-0}"
[[ "$KEYWORD_ONLY" == 0 || "$KEYWORD_ONLY" == 1 ]] || { echo "cluster-gate-phase4: GATE_KEYWORD_ONLY must be 0 or 1" >&2; exit 2; }
if (( KEYWORD_ONLY )); then
  K2_SHARD="${GATE_K2_SHARD:-lucene}"; K2_PHASE="${GATE_K2_PHASE:-any}"; MODE=keyword
else
  K2_SHARD="${GATE_K2_SHARD:-pharos}"; K2_PHASE="${GATE_K2_PHASE:-embed}"; MODE=embed
fi
MODELS_SRC="${GATE_MODELS_SRC:-$HOME/.djl.ai}"
SEED_FROM="${GATE_SEED_FROM:-}"
MINIO_PORT="${GATE_MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${GATE_MINIO_CONSOLE_PORT:-9001}"
ROUTER_PORT="${GATE_ROUTER_PORT:-7080}"; WEBHOOK_PORT="${GATE_WEBHOOK_PORT:-7390}"
R1_PORT="${GATE_R1_PORT:-7401}"; R2_PORT="${GATE_R2_PORT:-7402}"
W1_PORT="${GATE_W1_PORT:-7411}"; W2_PORT="${GATE_W2_PORT:-7412}"; W3_PORT="${GATE_W3_PORT:-7413}"
W4_PORT="${GATE_W4_PORT:-7414}"; W5_PORT="${GATE_W5_PORT:-7415}"; W6_PORT="${GATE_W6_PORT:-7416}"
WLOW_PORT="${GATE_WLOW_PORT:-7417}"
PROBE_TIMEOUT="${GATE_PORT_PROBE_TIMEOUT:-2}"
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -V -r | head -1 || true)}"
TOKEN="gate4-cluster-token-0123456789"
PORTS=("$MINIO_PORT" "$MINIO_CONSOLE_PORT" "$ROUTER_PORT" "$WEBHOOK_PORT" "$R1_PORT" "$R2_PORT" "$W1_PORT" "$W2_PORT"
       "$W3_PORT" "$W4_PORT" "$W5_PORT" "$W6_PORT" "$WLOW_PORT")
# the python driver reads the same settings
export GATE_MINIO_PORT="$MINIO_PORT" GATE_ROUTER_PORT="$ROUTER_PORT" GATE_WEBHOOK_PORT="$WEBHOOK_PORT"
export GATE_R1_PORT="$R1_PORT" GATE_R2_PORT="$R2_PORT" GATE_W1_PORT="$W1_PORT" GATE_W2_PORT="$W2_PORT"
export GATE_W3_PORT="$W3_PORT" GATE_W4_PORT="$W4_PORT" GATE_W5_PORT="$W5_PORT" GATE_W6_PORT="$W6_PORT"
export GATE_WLOW_PORT="$WLOW_PORT" GATE_KEYWORD_ONLY="$KEYWORD_ONLY" GATE_K2_SHARD="$K2_SHARD" GATE_K2_PHASE="$K2_PHASE"

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase4 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  minio       $MINIO_BIN on 127.0.0.1:$MINIO_PORT (console $MINIO_CONSOLE_PORT)
  gate dir    ${GATE:-<GATE unset>}
  heaps       bootstrap index $INDEX_HEAP, writers ${GATE_WRITER_HEAP:-2g}, readers/router ${GATE_WEB_HEAP:-640m}
  mode        $MODE (GATE_KEYWORD_ONLY=$KEYWORD_ONLY); K2 kills the $K2_SHARD holder in phase $K2_PHASE
  model       $( (( KEYWORD_ONLY )) && echo "none (keyword only)" || echo "$MODEL_URL as '$MODEL_ID' (copied from $MODELS_SRC/pharos; writers only, lazily; seed: ${SEED_FROM:-none})")
  sources     lucene ${GATE_LUCENE_SRC:-<registry rootPath>}, pharos ${GATE_PHAROS_SRC:-<registry rootPath>}
  repos       lucene (git archive, --no-embed), pharos (src/main + pom.xml, $( (( KEYWORD_ONLY )) && echo "--no-embed" || echo embedded)), alpha, beta (--no-embed)
  bootstrap   role writer, workQueue false: index + publish each with --source-git <bare> --ref main
  ports       ${PORTS[*]}
  phases      $PHASES
PLAN
  python3 -I "$REPO/scripts/cluster_gate_phase4.py" --dry-run --phases "$PHASES"
  exit 0
fi

[[ -n "${GATE:-}" ]] || { echo "cluster-gate-phase4: set GATE to a work dir on disk" >&2; exit 2; }
[[ -x "$MINIO_BIN" ]] || { echo "cluster-gate-phase4: MinIO binary not executable: $MINIO_BIN (this script never downloads)" >&2; exit 2; }
[[ -n "$JAR" && -f "$JAR" ]] || { echo "cluster-gate-phase4: no jar; run mvn clean package -DskipTests" >&2; exit 2; }
if (( ! KEYWORD_ONLY )); then
  [[ -d "$MODELS_SRC/pharos/$MODEL_DIR_NAME" ]] || { echo "cluster-gate-phase4: model not in $MODELS_SRC/pharos/$MODEL_DIR_NAME" >&2; exit 2; }
fi
command -v git >/dev/null || { echo "cluster-gate-phase4: git is required" >&2; exit 2; }

src_of() {  # $1 = project: its rootPath in ~/.pharos/registry.json (only read)
  python3 -I - "$HOME/.pharos/registry.json" "$1" <<'PY'
import json, os, sys
try:
    print(os.path.normpath(json.load(open(sys.argv[1]))[sys.argv[2]]["rootPath"]))
except (OSError, ValueError, KeyError, TypeError) as e:
    sys.exit("cluster-gate-phase4: no usable '%s' entry in %s (%r)" % (sys.argv[2], sys.argv[1], e))
PY
}
LUCENE_SRC="${GATE_LUCENE_SRC:-}"
[[ -n "$LUCENE_SRC" ]] || LUCENE_SRC="$(src_of lucene)" || exit 2
PHAROS_SRC="${GATE_PHAROS_SRC:-}"
[[ -n "$PHAROS_SRC" ]] || PHAROS_SRC="$(src_of pharos)" || exit 2
for s in "$LUCENE_SRC" "$PHAROS_SRC"; do
  git -C "$s" rev-parse --verify -q HEAD >/dev/null || { echo "cluster-gate-phase4: $s is not a git work tree" >&2; exit 2; }
done

for port in "${PORTS[@]}"; do
  if timeout "$PROBE_TIMEOUT" bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$port" 2>/dev/null; then
    echo "cluster-gate-phase4: port $port is already in use; refusing to start" >&2; exit 2
  fi
done

mkdir -p "$GATE"
free_gib() { df -Pk "$GATE" | awk 'NR==2{printf "%.1f", $4/1048576}'; }
if [[ "${GATE_REUSE:-0}" != 1 ]] && awk -v f="$(free_gib)" -v m="$MIN_FREE_GIB" 'BEGIN{exit !(f < m)}'; then
  echo "cluster-gate-phase4: only $(free_gib) GiB free on $GATE's file system (< $MIN_FREE_GIB)" >&2; exit 2
fi
[[ -f "$GATE/marker" ]] || touch "$GATE/marker"   # safety check: nothing under ~/.pharos or ~/.djl.ai newer

cp -f "$JAR" "$GATE/pharos.jar"   # a later `mvn package` in the repo does not change the running gate
JAR="$GATE/pharos.jar"
MINIO_DATA="$GATE/minio-data"
MODELS="$GATE/models-home/.djl.ai"
mkdir -p "$MINIO_DATA/pharos-gate" "$GATE/repos" "$GATE/edit" "$GATE/nodes"
mkdir -p "$MODELS/pharos"   # each node's .djl.ai links here, also in keyword-only mode (then empty)
if (( ! KEYWORD_ONLY )) && [[ ! -f "$MODELS/pharos/.copied" ]]; then
  cp -r "$MODELS_SRC/pharos/$MODEL_DIR_NAME" "$MODELS/pharos/"
  for d in cache tokenizers; do [[ -d "$MODELS_SRC/$d" ]] && cp -r "$MODELS_SRC/$d" "$MODELS/"; done
  touch "$MODELS/pharos/.copied"
fi

export PHAROS_GATE_ACCESS_KEY="gateaccess"
export PHAROS_GATE_SECRET_KEY="gatesecret-0123456789"
export MINIO_ROOT_USER="$PHAROS_GATE_ACCESS_KEY"
export MINIO_ROOT_PASSWORD="$PHAROS_GATE_SECRET_KEY"
export MALLOC_ARENA_MAX=2
LOGBACK="$REPO/scripts/cluster-gate-phase4-logback.xml"

PIDS=()
cleanup() {
  trap - EXIT INT TERM
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && wait "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

JVM_FLAGS=(--enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED
           --add-modules jdk.incubator.vector -Dpolyglot.engine.WarnInterpreterOnly=false
           -Dlogback.configurationFile="$LOGBACK")
wait_http() {
  local end=$((SECONDS + $2))
  until curl -fs --connect-timeout "$PROBE_TIMEOUT" --max-time 10 -o /dev/null "$1"; do
    (( SECONDS < end )) || { echo "cluster-gate-phase4: timeout waiting for $1" >&2; return 1; }
    sleep 1
  done
}
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }

echo "== gate dir $GATE; MemAvailable $(mem); free $(free_gib) GiB"
"$MINIO_BIN" server "$MINIO_DATA" --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$MINIO_CONSOLE_PORT" \
  > "$GATE/minio.log" 2>&1 &
PIDS+=($!)
echo "== MinIO pid $! on 127.0.0.1:$MINIO_PORT"
wait_http "http://127.0.0.1:$MINIO_PORT/minio/health/live" 60

# ---------------------------------------------------------------- repos (snapshots) and edit clones
gitq() { git -c user.name=pharos-gate -c user.email=gate@localhost -c init.defaultBranch=main "$@"; }
fixture() {  # $1 = name: a small Java project, three classes
  local d="$GATE/edit/$1" pkg="com/gate/$1"
  mkdir -p "$d/src/main/java/$pkg"
  cat > "$d/pom.xml" <<POM
<project><modelVersion>4.0.0</modelVersion><groupId>com.gate</groupId><artifactId>$1</artifactId><version>1.0</version></project>
POM
  for cls in Inventory Orders Pricing; do
    cat > "$d/src/main/java/$pkg/$cls.java" <<JAVA
package com.gate.$1;

/** $cls of the $1 gate fixture. */
public class $cls {
    private int count;

    /** Adds n to the $cls count. */
    public int add(int n) {
        count += n;
        return count;
    }

    /** Scales a value for $cls. */
    public int scale(int x) {
        return add(x) * 2;
    }
}
JAVA
  done
}
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/repos.done" ]]; then
  echo "== reusing the repos ($(cat "$GATE/repos.done"))"
else
  for n in lucene pharos alpha beta; do
    [[ -e "$GATE/edit/$n" || -e "$GATE/repos/$n.git" ]] && { echo "cluster-gate-phase4: $GATE/edit/$n or repos/$n.git exists; use a fresh GATE or GATE_REUSE=1" >&2; exit 2; }
  done
  mkdir -p "$GATE/edit/lucene" "$GATE/edit/pharos"
  git -C "$LUCENE_SRC" archive --format=tar HEAD | tar -x -C "$GATE/edit/lucene"
  git -C "$PHAROS_SRC" archive --format=tar HEAD src/main pom.xml | tar -x -C "$GATE/edit/pharos"
  fixture alpha
  fixture beta
  for n in lucene pharos alpha beta; do
    gitq init -q "$GATE/edit/$n"
    gitq -C "$GATE/edit/$n" add -A
    gitq -C "$GATE/edit/$n" commit -q -m "gate seed of $n"
    gitq init -q --bare "$GATE/repos/$n.git"
    gitq -C "$GATE/edit/$n" remote add origin "$GATE/repos/$n.git"
    gitq -C "$GATE/edit/$n" push -q origin main
    echo "   repo $n: $(git -C "$GATE/edit/$n" ls-files | wc -l) files, $(du -sh "$GATE/repos/$n.git" | cut -f1)"
  done
  date -Is > "$GATE/repos.done"
fi

# ---------------------------------------------------------------- bootstrap: hand-run writer publishes gen 1
B_HOME="$GATE/nodes/bootstrap"; B_PH="$B_HOME/.pharos"
mkdir -p "$B_PH"
ln -sfn "$MODELS" "$B_HOME/.djl.ai"
if (( KEYWORD_ONLY )); then
  PROVIDERS=""
else
  PROVIDERS='"embeddingProviders": [
    {"type": "djl", "modelId": "'"$MODEL_ID"'", "url": "'"$MODEL_URL"'", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "'"$MODEL_ID"'",'
fi
cat > "$B_PH/config.json" <<JSON
{
  $PROVIDERS
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer", "backend": "object-store", "nodeId": "gate-bootstrap",
    "cacheDir": "$B_PH/cluster-cache", "retentionGenerations": 1000, "retentionMinutes": 100000,
    "writer": {"workQueue": false},
    "objectStore": {
      "endpoint": "http://127.0.0.1:$MINIO_PORT", "region": "us-east-1", "bucket": "pharos-gate",
      "prefix": "pharos", "pathStyle": true,
      "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"
    }
  }
}
JSON
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/bootstrap.done" ]]; then
  prev_mode="$(cat "$GATE/bootstrap.mode" 2>/dev/null || echo embed)"   # runs before the setting were embed
  [[ "$prev_mode" == "$MODE" ]] || { echo "cluster-gate-phase4: $GATE was bootstrapped in $prev_mode mode, this run is $MODE (GATE_KEYWORD_ONLY); use a fresh GATE" >&2; exit 2; }
  echo "== reusing the bootstrap publish ($(cat "$GATE/bootstrap.done"), $prev_mode mode)"
else
  if (( ! KEYWORD_ONLY )) && [[ -n "$SEED_FROM" ]]; then
    seed=""
    for c in "$SEED_FROM/pharos/embed-cache.$MODEL_ID" "$SEED_FROM/pharos/embed-cache"; do
      [[ -f "$c.bin" && -f "$c.meta" ]] && { seed="$c"; break; }
    done
    [[ -n "$seed" ]] || { echo "cluster-gate-phase4: GATE_SEED_FROM has no pharos/embed-cache[.$MODEL_ID].{bin,meta}: $SEED_FROM" >&2; exit 2; }
    mkdir -p "$B_PH/indexes/pharos"
    cp "$seed.bin" "$B_PH/indexes/pharos/embed-cache.$MODEL_ID.bin"
    cp "$seed.meta" "$B_PH/indexes/pharos/embed-cache.$MODEL_ID.meta"
    echo "== seeded the bootstrap pharos embed cache from a copy of $seed ($(cat "$B_PH/indexes/pharos/embed-cache.$MODEL_ID.meta"))"
  fi
  PHAROS_FLAG=""; (( KEYWORD_ONLY )) && PHAROS_FLAG="--no-embed"
  for spec in "alpha|--no-embed" "beta|--no-embed" "lucene|--no-embed" "pharos|$PHAROS_FLAG"; do
    IFS='|' read -r name flag <<<"$spec"
    echo "== bootstrap: index $name ${flag:-(with embeddings)}; MemAvailable $(mem); free $(free_gib) GiB"
    t0=$SECONDS
    # shellcheck disable=SC2086
    java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" -Duser.home="$B_HOME" -Dpharos.home="$B_PH" \
      -jar "$JAR" index "$GATE/edit/$name" --single --project "$name" --no-progress $flag \
      --source-git "$GATE/repos/$name.git" --ref main > "$GATE/bootstrap-index-$name.log" 2>&1 \
      || { echo "cluster-gate-phase4: indexing $name failed; see $GATE/bootstrap-index-$name.log" >&2
           tail -n 20 "$GATE/bootstrap-index-$name.log" >&2; exit 1; }
    echo "   $name indexed + published in $((SECONDS - t0)) s"
    grep -E "Published shard|recorded source|Indexed project|Embedding cache" "$GATE/bootstrap-index-$name.log" | tail -n 4 || true
  done
  date -Is > "$GATE/bootstrap.done"
  echo "$MODE" > "$GATE/bootstrap.mode"
fi

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_MODELS="$MODELS" GATE_MODEL_URL="$MODEL_URL"
export GATE_MODEL_ID="$MODEL_ID" GATE_TOKEN="$TOKEN"
export GATE_WRITER_HEAP="${GATE_WRITER_HEAP:-2g}" GATE_WEB_HEAP="${GATE_WEB_HEAP:-640m}"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase4.py" --phases "$PHASES" || rc=$?
echo "== MinIO bucket: $(du -sh "$MINIO_DATA/pharos-gate" | cut -f1); free $(free_gib) GiB; gate exit $rc (logs in $GATE)"
exit "$rc"
