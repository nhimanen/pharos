#!/usr/bin/env bash
# Phase 5 gate: the Phase 1-3 gates with NO bucket (cluster.backend peer-pull), plus writer down and
# writer load. See docs/superpowers/plans/2026-11-17-multi-node-phase-5.md (Task 4).
#
# No MinIO and no object store anywhere. Writer W1 (role writer, backend peer-pull) indexes lucene
# (--no-embed), pharos and mypal (both embedded with jina, as in the Phase 2 and 3 gates) into its own
# peer store; `pharos web` on W1 serves /replication/* to readers. Readers find writers ONLY through
# cluster.peers.discovery `seeds:http://127.0.0.1:7300` (W1); W1's peer list names the second writer
# slot :7310, so W2 (a small fixture project, started mid-run) is learned by gossip. Then
# scripts/cluster_gate_phase5.py runs the phases, starting and stopping every JVM by PID; it reuses the
# Phase 1-3 drivers (cluster_gate.py, cluster_gate_phase2.py, cluster_gate_phase3.py) with
# GATE_BACKEND=peer-pull, which makes their reader configs peer-pull ones.
#
# Sources are snapshots (`git archive HEAD` of the registry's lucene and mypal rootPaths, and this
# repo's src + pom.xml for pharos), never the user's trees themselves. It NEVER downloads anything and
# never touches the real ~/.pharos or ~/.djl.ai: every JVM gets an isolated user.home and pharos.home
# under $GATE; the jina and cross-encoder models are COPIED from ~/.djl.ai into $GATE/models-home/.djl.ai
# (each node's .djl.ai is a symlink to that copy). Only ~/.pharos/registry.json is read.
#
# Run under a memory cap, e.g.
#   GATE=/home/me/gate5-scratch systemd-run --user --scope -q -p MemoryMax=6G \
#     bash scripts/cluster-gate-phase5.sh
#
# Env:
#   GATE               work dir (required unless --dry-run; on disk, not on a tmpfs /tmp)
#   GATE_JAR           pharos jar (default target/pharos-*.jar; copied to $GATE/pharos.jar unless GATE_REUSE=1)
#   PHAROS_INDEX_HEAP  heap of the writer index runs (default 3g)
#   GATE_WRITER_HEAP   heap of `pharos web` on the writers (default 512m)
#   GATE_WEB_HEAP      heap of each reader and the router (default 640m)
#   GATE_REF_HEAP      heap of the single-node reference (default 2g)
#   GATE_MODEL_URL     embedding model (default hf://jinaai/jina-embeddings-v2-base-code, 768 dims)
#   GATE_MODEL_ID      its provider modelId (default jina-code-v2)
#   GATE_REUSE=1       keep the sources, the index runs and the reference when $GATE holds them
#   GATE_PHASES        driver phases (default "p1,p2cold,p2warm,p2cap,reference,ranking,router,down,load,check";
#                      "none" stops after indexing)
#   GATE_MIN_FREE_GIB  refuse to start below this much free space on $GATE's file system (default 10)
#   GATE_DJL           where the models are copied from (default ~/.djl.ai; another host: a copy of it)
#   GATE_LUCENE_SRC, GATE_MYPAL_SRC  git repos to snapshot (default: the rootPaths in ~/.pharos/registry.json)
#   GATE_WITH_CE=0     do not copy the cross-encoder (no hybrid-reranked in the phases run)
#   GATE_LOAD_ONLY=1   step 4 alone on another host (no models, no registry): W1 indexes lucene --no-embed
#                      from the git repo GATE_LUCENE_GIT (a bare clone), then the driver runs "load,check"
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,43p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
DRY=0
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "cluster-gate-phase5: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INDEX_HEAP="${PHAROS_INDEX_HEAP:-3g}"
MODEL_URL="${GATE_MODEL_URL:-hf://jinaai/jina-embeddings-v2-base-code}"
MODEL_DIR_NAME="$(echo "${MODEL_URL#hf://}" | tr '/' '-')"
CE_DIR_NAME="Xenova-ms-marco-MiniLM-L-6-v2"   # CrossEncoderProvider default (hybrid-reranked)
DJL="${GATE_DJL:-$HOME/.djl.ai}"
MODEL_DIRS=("$MODEL_DIR_NAME"); [[ "${GATE_WITH_CE:-1}" == 1 ]] && MODEL_DIRS+=("$CE_DIR_NAME")
MODEL_ID="${GATE_MODEL_ID:-jina-code-v2}"
PHASES="${GATE_PHASES:-p1,p2cold,p2warm,p2cap,reference,ranking,router,down,load,check}"
MIN_FREE_GIB="${GATE_MIN_FREE_GIB:-10}"
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -r | head -1 || true)}"
TOKEN="gate5-cluster-token-0123456789"
PORTS=(7080 7300 7301 7302 7303 7304 7305 7310 7321 7322 7323 7324 7325 7326 7327)

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase5 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  gate dir    ${GATE:-<GATE unset>}
  backend     peer-pull; no MinIO, no bucket; token-guarded /replication on the writers
  models      $MODEL_URL as '$MODEL_ID' + cross-encoder $CE_DIR_NAME (copied from ~/.djl.ai/pharos)
  writer W1   :7300; index lucene --no-embed, pharos and mypal with embeddings (heap $INDEX_HEAP)
  writer W2   :7310; fixture project alpha (--no-embed), started mid-run (phase p1)
  discovery   readers: seeds:http://127.0.0.1:7300 only; W1 lists :7300 and :7310
  ports       ${PORTS[*]}
  phases      $PHASES
PLAN
  GATE_BACKEND=peer-pull python3 -I "$REPO/scripts/cluster_gate_phase5.py" --dry-run --phases "$PHASES"
  exit 0
fi

[[ -n "${GATE:-}" ]] || { echo "cluster-gate-phase5: set GATE to a work dir on disk" >&2; exit 2; }
[[ -n "$JAR" && -f "$JAR" ]] || { echo "cluster-gate-phase5: no jar; run mvn clean package -DskipTests" >&2; exit 2; }

if [[ "${GATE_LOAD_ONLY:-0}" == 1 ]]; then
  # Step 4 (writer load) alone, e.g. on a host with more cores: lucene --no-embed, no models.
  [[ -d "${GATE_LUCENE_GIT:-}" ]] || { echo "cluster-gate-phase5: GATE_LUCENE_GIT must name a git repo" >&2; exit 2; }
  for port in "${PORTS[@]}"; do
    if timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      echo "cluster-gate-phase5: port $port is already in use; refusing to start" >&2; exit 2
    fi
  done
  mkdir -p "$GATE/src/lucene" "$GATE/nodes/w1/.pharos" "$GATE/models-home/.djl.ai"
  [[ -f "$GATE/marker" ]] || touch "$GATE/marker"
  [[ -f "$GATE/pharos.jar" ]] || cp -f "$JAR" "$GATE/pharos.jar"
  JAR="$GATE/pharos.jar"
  export MALLOC_ARENA_MAX=2
  LOGBACK="$REPO/scripts/cluster-gate-phase4-logback.xml"
  W1_PH="$GATE/nodes/w1/.pharos"
  cat > "$W1_PH/config.json" <<JSON
{
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer", "backend": "peer-pull", "nodeId": "gate-w1",
    "token": "$TOKEN", "advertiseUrl": "http://127.0.0.1:7300",
    "cacheDir": "$W1_PH/cluster-cache",
    "peers": {"discovery": "seeds:http://127.0.0.1:7300", "storeDir": "$W1_PH/peer-store", "refreshSeconds": 10}
  }
}
JSON
  if [[ ! -f "$GATE/index-lucene.done" ]]; then
    git --git-dir="$GATE_LUCENE_GIT" archive --format=tar HEAD | tar -x -C "$GATE/src/lucene"
    t0=$SECONDS
    java -Xmx"$INDEX_HEAP" --enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED \
      --add-modules jdk.incubator.vector -Dlogback.configurationFile="$LOGBACK" \
      -Duser.home="$GATE/nodes/w1" -Dpharos.home="$W1_PH" -jar "$JAR" index "$GATE/src/lucene" --project lucene \
      --no-progress --no-embed > "$GATE/index-w1-lucene.log" 2>&1 \
      || { echo "cluster-gate-phase5: indexing lucene failed; see $GATE/index-w1-lucene.log" >&2; exit 1; }
    echo "== lucene indexed + published in $((SECONDS - t0)) s"
    date -Is > "$GATE/index-lucene.done"
  fi
  export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_MODELS="$GATE/models-home/.djl.ai" GATE_TOKEN="$TOKEN"
  export GATE_BACKEND=peer-pull GATE_SRC="$GATE/src" GATE_MODEL_ID=none GATE_MODEL_URL=none
  export GATE_WRITER_HEAP="${GATE_WRITER_HEAP:-512m}"
  rc=0
  python3 -I -u "$REPO/scripts/cluster_gate_phase5.py" --phases "${GATE_PHASES_LOAD:-load,check}" || rc=$?
  echo "== gate exit $rc (logs in $GATE)"
  exit "$rc"
fi
for d in "${MODEL_DIRS[@]}"; do
  [[ -d "$DJL/pharos/$d" ]] || { echo "cluster-gate-phase5: model not in $DJL/pharos/$d" >&2; exit 2; }
done

src_of() {  # $1 = project: its rootPath in ~/.pharos/registry.json (only read)
  python3 -I - "$HOME/.pharos/registry.json" "$1" <<'PY'
import json, os, sys
try:
    print(os.path.normpath(json.load(open(sys.argv[1]))[sys.argv[2]]["rootPath"]))
except (OSError, ValueError, KeyError, TypeError) as e:
    sys.exit("cluster-gate-phase5: no usable '%s' entry in %s (%r)" % (sys.argv[2], sys.argv[1], e))
PY
}
LUCENE_SRC="${GATE_LUCENE_SRC:-}"; [[ -n "$LUCENE_SRC" ]] || LUCENE_SRC="$(src_of lucene)" || exit 2
MYPAL_SRC="${GATE_MYPAL_SRC:-}"; [[ -n "$MYPAL_SRC" ]] || MYPAL_SRC="$(src_of mypal)" || exit 2
for s in "$LUCENE_SRC" "$MYPAL_SRC"; do
  git -C "$s" rev-parse --verify -q HEAD >/dev/null || { echo "cluster-gate-phase5: $s is not a git work tree" >&2; exit 2; }
done

for port in "${PORTS[@]}"; do
  if timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    echo "cluster-gate-phase5: port $port is already in use; refusing to start" >&2; exit 2
  fi
done

mkdir -p "$GATE"
free_gib() { df -Pk "$GATE" | awk 'NR==2{printf "%.1f", $4/1048576}'; }
if awk -v f="$(free_gib)" -v m="$MIN_FREE_GIB" 'BEGIN{exit !(f < m)}'; then
  echo "cluster-gate-phase5: only $(free_gib) GiB free on $GATE's file system (< $MIN_FREE_GIB)" >&2; exit 2
fi
[[ -f "$GATE/marker" ]] || touch "$GATE/marker"   # safety check: nothing under ~/.pharos or ~/.djl.ai newer
if [[ "${GATE_REUSE:-0}" != 1 || ! -f "$GATE/pharos.jar" ]]; then
  cp -f "$JAR" "$GATE/pharos.jar"   # a later `mvn package` in the repo does not change the running gate
fi
JAR="$GATE/pharos.jar"
MODELS="$GATE/models-home/.djl.ai"
SRC="$GATE/src"
mkdir -p "$SRC" "$GATE/nodes"

if [[ ! -f "$MODELS/pharos/.copied" ]]; then
  mkdir -p "$MODELS/pharos"
  for d in "${MODEL_DIRS[@]}"; do cp -r "$DJL/pharos/$d" "$MODELS/pharos/"; done
  for d in cache tokenizers; do [[ -d "$DJL/$d" ]] && cp -r "$DJL/$d" "$MODELS/"; done
  touch "$MODELS/pharos/.copied"
fi
export MALLOC_ARENA_MAX=2
LOGBACK="$REPO/scripts/cluster-gate-phase4-logback.xml"   # com.pharos.cluster INFO + the indexer's lines

# ---------------------------------------------------------------- source snapshots
fixture() {  # $1 = name: a small Java project, three classes
  local d="$SRC/$1" pkg="com/gate/$1"
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
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/src.done" ]]; then
  echo "== reusing the source snapshots ($(cat "$GATE/src.done"))"
else
  for n in lucene pharos mypal alpha; do
    [[ -e "$SRC/$n" ]] && { echo "cluster-gate-phase5: $SRC/$n exists; use a fresh GATE or GATE_REUSE=1" >&2; exit 2; }
  done
  mkdir -p "$SRC/lucene" "$SRC/mypal" "$SRC/pharos"
  git -C "$LUCENE_SRC" archive --format=tar HEAD | tar -x -C "$SRC/lucene"
  git -C "$MYPAL_SRC" archive --format=tar HEAD | tar -x -C "$SRC/mypal"
  cp -r "$REPO/src" "$REPO/pom.xml" "$SRC/pharos/"
  fixture alpha
  { echo "lucene $(git -C "$LUCENE_SRC" rev-parse --short HEAD)"; echo "mypal $(git -C "$MYPAL_SRC" rev-parse --short HEAD)";
    echo "pharos $(git -C "$REPO" rev-parse --short HEAD) (src + pom.xml)"; } > "$GATE/src.done"
  for n in lucene pharos mypal alpha; do echo "   source $n: $(find "$SRC/$n" -type f | wc -l) files, $(du -sh "$SRC/$n" | cut -f1)"; done
fi

# ---------------------------------------------------------------- writers W1 (:7300) and W2 (:7310)
writer_config() {  # $1 = name, $2 = port, $3 = discovery
  local home="$GATE/nodes/$1" ph="$GATE/nodes/$1/.pharos"
  mkdir -p "$ph"
  ln -sfn "$MODELS" "$home/.djl.ai"
  cat > "$ph/config.json" <<JSON
{
  "embeddingProviders": [
    {"type": "djl", "modelId": "$MODEL_ID", "url": "$MODEL_URL", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "$MODEL_ID",
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer", "backend": "peer-pull", "nodeId": "gate-$1",
    "token": "$TOKEN", "advertiseUrl": "http://127.0.0.1:$2",
    "cacheDir": "$ph/cluster-cache", "heartbeatSeconds": 5,
    "peers": {"discovery": "$3", "storeDir": "$ph/peer-store", "refreshSeconds": 10}
  }
}
JSON
}
writer_config w1 7300 "seeds:http://127.0.0.1:7300,http://127.0.0.1:7310"
writer_config w2 7310 "seeds:http://127.0.0.1:7300,http://127.0.0.1:7310"

JVM_FLAGS=(--enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED
           --add-modules jdk.incubator.vector -Dpolyglot.engine.WarnInterpreterOnly=false
           -Dlogback.configurationFile="$LOGBACK")
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }
index_on() {  # $1 = writer, $2 = project, $3 = flags; one JVM per run
  local home="$GATE/nodes/$1" t0=$SECONDS
  echo "== $1: index $2 ${3:-(with embeddings)}; MemAvailable $(mem); free $(free_gib) GiB"
  # shellcheck disable=SC2086
  java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" -Duser.home="$home" -Dpharos.home="$home/.pharos" \
    -jar "$JAR" index "$SRC/$2" --project "$2" --no-progress $3 > "$GATE/index-$1-$2.log" 2>&1 \
    || { echo "cluster-gate-phase5: indexing $2 failed; see $GATE/index-$1-$2.log" >&2
         tail -n 20 "$GATE/index-$1-$2.log" >&2; exit 1; }
  echo "   $2 indexed + published in $((SECONDS - t0)) s"
  grep -E "Published shard|Indexed project|Embedding cache|methods" "$GATE/index-$1-$2.log" | tail -n 4 || true
}
for spec in "w1|lucene|--no-embed" "w2|alpha|--no-embed" "w1|pharos|" "w1|mypal|"; do
  IFS='|' read -r w name flag <<<"$spec"
  if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/index-$name.done" ]]; then
    echo "== reusing $w's index of $name ($(cat "$GATE/index-$name.done"))"
  else
    index_on "$w" "$name" "$flag"
    date -Is > "$GATE/index-$name.done"
  fi
done

# The single-node reference: W1's local indexes (exactly what it published), role all, no cluster.
W1_PH="$GATE/nodes/w1/.pharos"
REF_PH="$GATE/reference/.pharos"
if [[ "${GATE_REUSE:-0}" != 1 || ! -f "$GATE/reference.done" ]]; then
  rm -rf "$GATE/reference"; mkdir -p "$REF_PH"
  (cd "$W1_PH" && tar -cf - --exclude=./peer-store --exclude=./cluster-cache --exclude=./logs \
     --exclude=./config.json --exclude='./*.pid' --exclude='./*.lock' .) | tar -xf - -C "$REF_PH"
  python3 -I - "$REF_PH/registry.json" "$W1_PH" "$REF_PH" <<'PY'
import sys
p, old, new = sys.argv[1:4]
s = open(p).read()
open(p, "w").write(s.replace(old + "/", new + "/"))
PY
  date -Is > "$GATE/reference.done"
fi

[[ "$PHASES" == none ]] && { echo "== indexing done; GATE_PHASES=none"; exit 0; }

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_MODELS="$MODELS" GATE_MODEL_URL="$MODEL_URL"
export GATE_MODEL_ID="$MODEL_ID" GATE_TOKEN="$TOKEN" GATE_BACKEND=peer-pull GATE_SRC="$SRC"
export GATE_PROJECTS="lucene,pharos,mypal"
export GATE_WRITER_HEAP="${GATE_WRITER_HEAP:-512m}" GATE_WEB_HEAP="${GATE_WEB_HEAP:-640m}"
export GATE_REF_HEAP="${GATE_REF_HEAP:-2g}" GATE_INDEX_HEAP="$INDEX_HEAP"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase5.py" --phases "$PHASES" || rc=$?
echo "== free $(free_gib) GiB; gate exit $rc (logs in $GATE)"
exit "$rc"
