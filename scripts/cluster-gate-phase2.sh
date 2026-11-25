#!/usr/bin/env bash
# Phase 2 gate: lazy reader generations served through a block cache, against a local MinIO.
# See docs/superpowers/plans/2026-11-16-multi-node-phase-2.md (Task 6) and docs/superpowers/gates/.
#
# This script starts MinIO, has a writer index lucene (--no-embed) and pharos (with embeddings)
# and publish both, then hands over to scripts/cluster_gate_phase2.py, which starts and stops the
# reader JVMs for the cold, warm and cap steps one configuration at a time.
#
# It NEVER downloads anything and never touches the real ~/.pharos or ~/.djl.ai: every JVM gets an
# isolated user.home and pharos.home under $GATE, and the embedding model is COPIED from ~/.djl.ai
# into $GATE/models-home/.djl.ai (each node's .djl.ai is a symlink to that copy). The lucene
# source path is only READ from ~/.pharos/registry.json.
#
# Run under a memory cap, e.g.
#   GATE=/home/me/gate2-scratch/run1 systemd-run --user --scope -q -p MemoryMax=9G \
#     bash scripts/cluster-gate-phase2.sh
#
# Env:
#   MINIO_BIN          minio server binary (default $HOME/.cache/pharos-gate/minio)
#   GATE               work dir (required unless --dry-run; put it on disk, not on a tmpfs /tmp)
#   PHAROS_INDEX_HEAP  heap of the writer index runs (default 4g)
#   PHAROS_WEB_HEAP    heap of each reader web JVM (default 1500m)
#   GATE_MODEL_URL     embedding model (default hf://jinaai/jina-embeddings-v2-base-code, 768 dims)
#   GATE_MODEL_ID      its provider modelId (default jina-code-v2; never "legacy", see below)
#   GATE_REUSE_INDEX=1 skip indexing when $GATE already holds a published bucket (re-runs)
#   GATE_PHASES        python driver phases (default "cold,warm,cap"; diagnostics: cold-models, cold-full)
#   GATE_TRIALS        cold trials (default 20)
#   GATE_JAR           pharos jar to run (default target/pharos-*.jar; a copy lets you rebuild meanwhile)
#   GATE_WARM_RUNS     timed warm runs per side (default 3)
#   GATE_READER_JVM_OPTS  extra JVM options for the reader JVMs (e.g. JFR flags)
#   GATE_MINIO_PORT / GATE_MINIO_CONSOLE_PORT  MinIO ports (default 9000 / 9001)
#   GATE_LAZY_PORT / GATE_FULL_PORT            lazy and full reader web ports (default 7301 / 7302)
#   GATE_PORT_PROBE_TIMEOUT  seconds per port probe and curl connect (default 2; on hosts where an
#                            unused port hangs instead of refusing, a timed-out probe counts as free)
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,35p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
DRY=0
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "cluster-gate-phase2: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_BIN="${MINIO_BIN:-$HOME/.cache/pharos-gate/minio}"
INDEX_HEAP="${PHAROS_INDEX_HEAP:-4g}"
WEB_HEAP="${PHAROS_WEB_HEAP:-1500m}"
MODEL_URL="${GATE_MODEL_URL:-hf://jinaai/jina-embeddings-v2-base-code}"
MODEL_DIR_NAME="$(echo "${MODEL_URL#hf://}" | tr '/' '-')"
# An explicit (non-"legacy") model id: with the legacy top-level embeddingModelUrl, indexing
# writes the vectors to field vec.legacy while search reads vectorEmbedding, so every vector
# query answers nothing (pre-existing bug, also on main; see the Phase 2 gate doc).
MODEL_ID="${GATE_MODEL_ID:-jina-code-v2}"
PHASES="${GATE_PHASES:-cold,warm,cap}"
TRIALS="${GATE_TRIALS:-20}"
MINIO_PORT="${GATE_MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${GATE_MINIO_CONSOLE_PORT:-9001}"
LAZY_PORT="${GATE_LAZY_PORT:-7301}"
FULL_PORT="${GATE_FULL_PORT:-7302}"
PROBE_TIMEOUT="${GATE_PORT_PROBE_TIMEOUT:-2}"
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -r | head -1 || true)}"

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase2 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  minio       $MINIO_BIN on 127.0.0.1:$MINIO_PORT (console $MINIO_CONSOLE_PORT)
  readers     lazy :$LAZY_PORT, full :$FULL_PORT
  gate dir    ${GATE:-<GATE unset>}
  heaps       index $INDEX_HEAP, reader web $WEB_HEAP
  model       $MODEL_URL (copied from ~/.djl.ai/pharos/$MODEL_DIR_NAME)
  writer      index lucene --no-embed, pharos with embeddings; publish to s3://pharos-gate/pharos
  phases      $PHASES (cold trials: $TRIALS)
PLAN
  python3 -I "$REPO/scripts/cluster_gate_phase2.py" --dry-run --phases "$PHASES" --trials "$TRIALS"
  exit 0
fi

[[ -n "${GATE:-}" ]] || { echo "cluster-gate-phase2: set GATE to a work dir on disk" >&2; exit 2; }
[[ -x "$MINIO_BIN" ]] || { echo "cluster-gate-phase2: MinIO binary not executable: $MINIO_BIN (this script never downloads)" >&2; exit 2; }
[[ -n "$JAR" ]] || { echo "cluster-gate-phase2: no target/pharos-*.jar; run mvn clean package -DskipTests" >&2; exit 2; }
MODEL_SRC="$HOME/.djl.ai/pharos/$MODEL_DIR_NAME"
[[ -d "$MODEL_SRC" ]] || { echo "cluster-gate-phase2: model not in $MODEL_SRC" >&2; exit 2; }

LUCENE_SRC="$(python3 -I - "$HOME/.pharos/registry.json" <<'PY'
import json, os, sys
try:
    print(os.path.normpath(json.load(open(sys.argv[1]))["lucene"]["rootPath"]))
except (OSError, ValueError, KeyError, TypeError) as e:
    sys.exit("cluster-gate-phase2: no usable 'lucene' entry in %s (%r)" % (sys.argv[1], e))
PY
)" || exit 2

for port in "$MINIO_PORT" "$MINIO_CONSOLE_PORT" "$LAZY_PORT" "$FULL_PORT"; do
  if timeout "$PROBE_TIMEOUT" bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$port" 2>/dev/null; then
    echo "cluster-gate-phase2: port $port is already in use; refusing to start" >&2; exit 2
  fi
done

mkdir -p "$GATE"
W_HOME="$GATE/writer"; W_PH="$W_HOME/.pharos"
MINIO_DATA="$GATE/minio-data"
PHAROS_SRC="$GATE/pharos-src"
MODELS="$GATE/models-home/.djl.ai"
mkdir -p "$W_PH" "$MINIO_DATA/pharos-gate"

# The model copy: only the model dir and DJL's native-library caches, never written back.
if [[ ! -f "$MODELS/pharos/$MODEL_DIR_NAME/.copied" ]]; then
  mkdir -p "$MODELS/pharos"
  cp -r "$MODEL_SRC" "$MODELS/pharos/"
  for d in cache tokenizers; do [[ -d "$HOME/.djl.ai/$d" ]] && cp -r "$HOME/.djl.ai/$d" "$MODELS/"; done
  touch "$MODELS/pharos/$MODEL_DIR_NAME/.copied"
fi
ln -sfn "$MODELS" "$W_HOME/.djl.ai"

export PHAROS_GATE_ACCESS_KEY="gateaccess"
export PHAROS_GATE_SECRET_KEY="gatesecret-0123456789"
export MINIO_ROOT_USER="$PHAROS_GATE_ACCESS_KEY"
export MINIO_ROOT_PASSWORD="$PHAROS_GATE_SECRET_KEY"
LOGBACK="$REPO/scripts/cluster-gate-logback.xml"

cat > "$W_PH/config.json" <<JSON
{
  "embeddingProviders": [
    {"type": "djl", "modelId": "$MODEL_ID", "url": "$MODEL_URL", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "$MODEL_ID",
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer",
    "backend": "object-store",
    "nodeId": "gate-writer",
    "writer": {"workQueue": false},
    "cacheDir": "$W_PH/cluster-cache",
    "objectStore": {
      "endpoint": "http://127.0.0.1:$MINIO_PORT", "region": "us-east-1", "bucket": "pharos-gate",
      "prefix": "pharos", "pathStyle": true,
      "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"
    }
  }
}
JSON

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
    (( SECONDS < end )) || { echo "cluster-gate-phase2: timeout waiting for $1" >&2; return 1; }
    sleep 1
  done
}
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }

echo "== gate dir $GATE; MemAvailable $(mem)"
"$MINIO_BIN" server "$MINIO_DATA" --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$MINIO_CONSOLE_PORT" \
  > "$GATE/minio.log" 2>&1 &
PIDS+=($!)
echo "== MinIO pid $! on 127.0.0.1:$MINIO_PORT"
wait_http "http://127.0.0.1:$MINIO_PORT/minio/health/live" 60

if [[ "${GATE_REUSE_INDEX:-0}" == 1 && -f "$GATE/writer.done" ]]; then
  echo "== reusing the published bucket ($(cat "$GATE/writer.done"))"
else
  rm -rf "$PHAROS_SRC"; mkdir -p "$PHAROS_SRC"
  cp -r "$REPO/src" "$REPO/pom.xml" "$PHAROS_SRC/"
  for spec in "lucene|$LUCENE_SRC|--no-embed" "pharos|$PHAROS_SRC|"; do
    IFS='|' read -r name src flag <<<"$spec"
    echo "== writer: index $name ${flag:-(with embeddings)}; MemAvailable $(mem)"
    t0=$SECONDS
    # shellcheck disable=SC2086
    java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" -Duser.home="$W_HOME" -Dpharos.home="$W_PH" \
      -jar "$JAR" index "$src" --project "$name" $flag > "$GATE/writer-index-$name.log" 2>&1 \
      || { echo "cluster-gate-phase2: indexing $name failed; see $GATE/writer-index-$name.log" >&2
           tail -n 20 "$GATE/writer-index-$name.log" >&2; exit 1; }
    echo "   $name indexed + published in $((SECONDS - t0)) s"
    grep -E "Published shard|methods|classes" "$GATE/writer-index-$name.log" | tail -n 3 || true
  done
  date -Is > "$GATE/writer.done"
fi

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_WEB_HEAP="$WEB_HEAP" GATE_MODELS="$MODELS"
export GATE_MODEL_URL="$MODEL_URL" GATE_MODEL_ID="$MODEL_ID"
export GATE_MINIO_PORT="$MINIO_PORT" GATE_LAZY_PORT="$LAZY_PORT" GATE_FULL_PORT="$FULL_PORT"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase2.py" --phases "$PHASES" --trials "$TRIALS" || rc=$?
echo "== MinIO bucket: $(du -sh "$MINIO_DATA/pharos-gate" | cut -f1); gate exit $rc (logs in $GATE)"
exit "$rc"
