#!/usr/bin/env bash
# Phase 3 gate: multi-shard fan-out (pinned readers, distributed coordinator, router, draining)
# against a local MinIO. See docs/superpowers/plans/2026-11-17-multi-node-phase-3.md (Task 7).
#
# This script starts MinIO, has a writer index lucene (--no-embed), pharos and mypal (both with
# embeddings, so the multi-shard vector merge is measured; GATE_WITH_MYPAL=0 leaves mypal out) and
# publish them, builds a single-node reference (role all) from a copy of the writer's local indexes,
# and hands over to scripts/cluster_gate_phase3.py, which starts and stops the reference, the readers
# R1 {pharos}, R2 {lucene, mypal}, R3 (all), R4 (all) and the router one at a time, by PID.
#
# It NEVER downloads anything and never touches the real ~/.pharos or ~/.djl.ai: every JVM gets an
# isolated user.home and pharos.home under $GATE; the embedding and cross-encoder models are COPIED
# from ~/.djl.ai into $GATE/models-home/.djl.ai (each node's .djl.ai is a symlink to that copy). The
# lucene and mypal source paths are only READ from ~/.pharos/registry.json (the sources are only read).
# With GATE_SEED_EMBED_CACHE=1 (default) the writer's pharos and mypal embedding caches are seeded from
# COPIES of ~/.pharos/indexes/<project>/embed-cache.* (same model and vectors), which are only read.
#
# Run under a memory cap, e.g.
#   GATE=/home/me/gate3-scratch/run1 systemd-run --user --scope -q -p MemoryMax=9G \
#     bash scripts/cluster-gate-phase3.sh
#
# Env:
#   MINIO_BIN          minio server binary (default $HOME/.cache/pharos-gate/minio)
#   GATE               work dir (required unless --dry-run; on disk, not on a tmpfs /tmp)
#   GATE_JAR           pharos jar to run (default target/pharos-*.jar)
#   PHAROS_INDEX_HEAP  heap of the writer index runs (default 3g)
#   PHAROS_WEB_HEAP    heap of each reader and the router (default 1g)
#   PHAROS_REF_HEAP    heap of the single-node reference (default 2g)
#   GATE_MODEL_URL     embedding model (default hf://jinaai/jina-embeddings-v2-base-code, 768 dims)
#   GATE_MODEL_ID      its provider modelId (default jina-code-v2; never "legacy", see the Phase 2 gate)
#   GATE_REUSE_INDEX=1 skip indexing when $GATE already holds a published bucket (re-runs)
#   GATE_SEED_EMBED_CACHE  1 (default) or 0
#   GATE_WITH_MYPAL    1 (default): also index and publish mypal (embedded); 0: lucene and pharos only
#   GATE_PHASES        python driver phases (default "reference,ranking,elasticity")
#   GATE_LOAD_SECONDS / GATE_QPS / GATE_JOIN_AT / GATE_DRAIN_AT  elasticity timeline (180 / 20 / 60 / 120)
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,37p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
DRY=0
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "cluster-gate-phase3: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_BIN="${MINIO_BIN:-$HOME/.cache/pharos-gate/minio}"
INDEX_HEAP="${PHAROS_INDEX_HEAP:-3g}"
WEB_HEAP="${PHAROS_WEB_HEAP:-1g}"
REF_HEAP="${PHAROS_REF_HEAP:-2g}"
MODEL_URL="${GATE_MODEL_URL:-hf://jinaai/jina-embeddings-v2-base-code}"
MODEL_DIR_NAME="$(echo "${MODEL_URL#hf://}" | tr '/' '-')"
CE_DIR_NAME="Xenova-ms-marco-MiniLM-L-6-v2"   # CrossEncoderProvider default (hybrid-reranked)
MODEL_ID="${GATE_MODEL_ID:-jina-code-v2}"
PHASES="${GATE_PHASES:-reference,ranking,elasticity}"
SEED="${GATE_SEED_EMBED_CACHE:-1}"
WITH_MYPAL="${GATE_WITH_MYPAL:-1}"
PROJECTS="lucene,pharos"; R2_SHARDS="lucene"
if (( WITH_MYPAL )); then PROJECTS="lucene,pharos,mypal"; R2_SHARDS="lucene, mypal"; fi
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -r | head -1 || true)}"
TOKEN="gate3-cluster-token-0123456789"

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase3 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  minio       $MINIO_BIN on 127.0.0.1:9000
  gate dir    ${GATE:-<GATE unset>}
  heaps       index $INDEX_HEAP, readers/router $WEB_HEAP, reference $REF_HEAP
  models      $MODEL_URL as '$MODEL_ID' + cross-encoder $CE_DIR_NAME (copied from ~/.djl.ai/pharos)
  writer      index lucene --no-embed; pharos$( (( WITH_MYPAL )) && echo " and mypal") with embeddings (embed caches seeded: $SEED); publish
  projects    $PROJECTS
  reference   copy of the writer's local indexes, role all, :7305
  readers     R1 {pharos} :7301, R2 {$R2_SHARDS} :7302, R3 all :7303, R4 all :7304 (lazy); router :7080
  phases      $PHASES
PLAN
  GATE_PROJECTS="$PROJECTS" python3 -I "$REPO/scripts/cluster_gate_phase3.py" --dry-run --phases "$PHASES"
  exit 0
fi

[[ -n "${GATE:-}" ]] || { echo "cluster-gate-phase3: set GATE to a work dir on disk" >&2; exit 2; }
[[ -x "$MINIO_BIN" ]] || { echo "cluster-gate-phase3: MinIO binary not executable: $MINIO_BIN (this script never downloads)" >&2; exit 2; }
[[ -n "$JAR" && -f "$JAR" ]] || { echo "cluster-gate-phase3: no jar; run mvn clean package -DskipTests" >&2; exit 2; }
for d in "$MODEL_DIR_NAME" "$CE_DIR_NAME"; do
  [[ -d "$HOME/.djl.ai/pharos/$d" ]] || { echo "cluster-gate-phase3: model not in ~/.djl.ai/pharos/$d" >&2; exit 2; }
done

src_of() {  # $1 = project: its rootPath in ~/.pharos/registry.json (only read)
  python3 -I - "$HOME/.pharos/registry.json" "$1" <<'PY'
import json, os, sys
try:
    print(os.path.normpath(json.load(open(sys.argv[1]))[sys.argv[2]]["rootPath"]))
except (OSError, ValueError, KeyError, TypeError) as e:
    sys.exit("cluster-gate-phase3: no usable '%s' entry in %s (%r)" % (sys.argv[2], sys.argv[1], e))
PY
}
LUCENE_SRC="$(src_of lucene)" || exit 2
MYPAL_SRC=""
if (( WITH_MYPAL )); then
  MYPAL_SRC="$(src_of mypal)" || exit 2
  [[ -d "$MYPAL_SRC" ]] || { echo "cluster-gate-phase3: mypal rootPath not a directory: $MYPAL_SRC" >&2; exit 2; }
fi

for port in 9000 9001 7080 7301 7302 7303 7304 7305; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "cluster-gate-phase3: port $port is already in use; refusing to start" >&2; exit 2
  fi
done

mkdir -p "$GATE"
W_HOME="$GATE/writer"; W_PH="$W_HOME/.pharos"
MINIO_DATA="$GATE/minio-data"
PHAROS_SRC="$GATE/pharos-src"
MODELS="$GATE/models-home/.djl.ai"
mkdir -p "$W_PH" "$MINIO_DATA/pharos-gate"

if [[ ! -f "$MODELS/pharos/.copied" ]]; then
  mkdir -p "$MODELS/pharos"
  cp -r "$HOME/.djl.ai/pharos/$MODEL_DIR_NAME" "$HOME/.djl.ai/pharos/$CE_DIR_NAME" "$MODELS/pharos/"
  for d in cache tokenizers; do [[ -d "$HOME/.djl.ai/$d" ]] && cp -r "$HOME/.djl.ai/$d" "$MODELS/"; done
  touch "$MODELS/pharos/.copied"
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
    "cacheDir": "$W_PH/cluster-cache",
    "objectStore": {
      "endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "pharos-gate",
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
  until curl -fs -o /dev/null "$1"; do
    (( SECONDS < end )) || { echo "cluster-gate-phase3: timeout waiting for $1" >&2; return 1; }
    sleep 1
  done
}
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }

echo "== gate dir $GATE; MemAvailable $(mem)"
"$MINIO_BIN" server "$MINIO_DATA" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 \
  > "$GATE/minio.log" 2>&1 &
PIDS+=($!)
echo "== MinIO pid $! on 127.0.0.1:9000"
wait_http http://127.0.0.1:9000/minio/health/live 60

if [[ "${GATE_REUSE_INDEX:-0}" == 1 && -f "$GATE/writer.done" ]]; then
  echo "== reusing the published bucket ($(cat "$GATE/writer.done"))"
else
  rm -rf "$PHAROS_SRC"; mkdir -p "$PHAROS_SRC"
  cp -r "$REPO/src" "$REPO/pom.xml" "$PHAROS_SRC/"
  for p in pharos mypal; do
    [[ "$p" == mypal ]] && (( ! WITH_MYPAL )) && continue
    if [[ "$SEED" == 1 && -f "$HOME/.pharos/indexes/$p/embed-cache.bin" ]]; then
      mkdir -p "$W_PH/indexes/$p"
      cp "$HOME/.pharos/indexes/$p/embed-cache.bin" "$W_PH/indexes/$p/embed-cache.$MODEL_ID.bin"
      cp "$HOME/.pharos/indexes/$p/embed-cache.meta" "$W_PH/indexes/$p/embed-cache.$MODEL_ID.meta"
      echo "== seeded the $p embed cache from a copy ($(cat "$W_PH/indexes/$p/embed-cache.$MODEL_ID.meta"))"
    fi
  done
  SPECS=("lucene|$LUCENE_SRC|--no-embed" "pharos|$PHAROS_SRC|")
  (( WITH_MYPAL )) && SPECS+=("mypal|$MYPAL_SRC|")
  for spec in "${SPECS[@]}"; do
    IFS='|' read -r name src flag <<<"$spec"
    echo "== writer: index $name ${flag:-(with embeddings)}; MemAvailable $(mem)"
    t0=$SECONDS
    # shellcheck disable=SC2086
    java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" -Duser.home="$W_HOME" -Dpharos.home="$W_PH" \
      -jar "$JAR" index "$src" --project "$name" $flag > "$GATE/writer-index-$name.log" 2>&1 \
      || { echo "cluster-gate-phase3: indexing $name failed; see $GATE/writer-index-$name.log" >&2
           tail -n 20 "$GATE/writer-index-$name.log" >&2; exit 1; }
    echo "   $name indexed + published in $((SECONDS - t0)) s"
    grep -E "Published shard|methods|classes|Embedd" "$GATE/writer-index-$name.log" | tail -n 4 || true
  done
  date -Is > "$GATE/writer.done"
fi

# The single-node reference: the writer's local indexes (exactly what was published), role all.
REF_PH="$GATE/reference/.pharos"
if [[ ! -f "$GATE/reference.done" ]]; then
  rm -rf "$GATE/reference"; mkdir -p "$REF_PH"
  cp -r "$W_PH/." "$REF_PH/"
  rm -rf "$REF_PH/cluster-cache" "$REF_PH/config.json" "$REF_PH/logs" "$REF_PH"/*.pid "$REF_PH"/*.lock
  python3 -I - "$REF_PH/registry.json" "$W_PH" "$REF_PH" <<'PY'
import sys
p, old, new = sys.argv[1:4]
s = open(p).read()
open(p, "w").write(s.replace(old + "/", new + "/"))
PY
  date -Is > "$GATE/reference.done"
fi

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_WEB_HEAP="$WEB_HEAP" GATE_REF_HEAP="$REF_HEAP"
export GATE_MODELS="$MODELS" GATE_MODEL_URL="$MODEL_URL" GATE_MODEL_ID="$MODEL_ID" GATE_TOKEN="$TOKEN"
export GATE_PROJECTS="$PROJECTS"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase3.py" --phases "$PHASES" || rc=$?
echo "== MinIO bucket: $(du -sh "$MINIO_DATA/pharos-gate" | cut -f1); gate exit $rc (logs in $GATE)"
exit "$rc"
