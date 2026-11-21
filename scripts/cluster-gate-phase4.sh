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
# Run under a memory cap, e.g.
#   GATE=/home/me/gate4-scratch systemd-run --user --scope -q -p MemoryMax=9G \
#     bash scripts/cluster-gate-phase4.sh
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
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,37p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
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
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -r | head -1 || true)}"
TOKEN="gate4-cluster-token-0123456789"
PORTS=(9000 9001 7080 7390 7401 7402 7411 7412 7413 7414 7415 7416 7417)

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase4 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  minio       $MINIO_BIN on 127.0.0.1:9000
  gate dir    ${GATE:-<GATE unset>}
  model       $MODEL_URL as '$MODEL_ID' (copied from ~/.djl.ai/pharos; writers only, lazily)
  repos       lucene (git archive, --no-embed), pharos (src/main + pom.xml, embedded), alpha, beta (--no-embed)
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
[[ -d "$HOME/.djl.ai/pharos/$MODEL_DIR_NAME" ]] || { echo "cluster-gate-phase4: model not in ~/.djl.ai/pharos/$MODEL_DIR_NAME" >&2; exit 2; }
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
LUCENE_SRC="$(src_of lucene)" || exit 2
PHAROS_SRC="$(src_of pharos)" || exit 2
for s in "$LUCENE_SRC" "$PHAROS_SRC"; do
  git -C "$s" rev-parse --verify -q HEAD >/dev/null || { echo "cluster-gate-phase4: $s is not a git work tree" >&2; exit 2; }
done

for port in "${PORTS[@]}"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
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
if [[ ! -f "$MODELS/pharos/.copied" ]]; then
  mkdir -p "$MODELS/pharos"
  cp -r "$HOME/.djl.ai/pharos/$MODEL_DIR_NAME" "$MODELS/pharos/"
  for d in cache tokenizers; do [[ -d "$HOME/.djl.ai/$d" ]] && cp -r "$HOME/.djl.ai/$d" "$MODELS/"; done
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
  until curl -fs -o /dev/null "$1"; do
    (( SECONDS < end )) || { echo "cluster-gate-phase4: timeout waiting for $1" >&2; return 1; }
    sleep 1
  done
}
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }

echo "== gate dir $GATE; MemAvailable $(mem); free $(free_gib) GiB"
"$MINIO_BIN" server "$MINIO_DATA" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 \
  > "$GATE/minio.log" 2>&1 &
PIDS+=($!)
echo "== MinIO pid $! on 127.0.0.1:9000"
wait_http http://127.0.0.1:9000/minio/health/live 60

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
cat > "$B_PH/config.json" <<JSON
{
  "embeddingProviders": [
    {"type": "djl", "modelId": "$MODEL_ID", "url": "$MODEL_URL", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "$MODEL_ID",
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer", "backend": "object-store", "nodeId": "gate-bootstrap",
    "cacheDir": "$B_PH/cluster-cache", "retentionGenerations": 1000, "retentionMinutes": 100000,
    "writer": {"workQueue": false},
    "objectStore": {
      "endpoint": "http://127.0.0.1:9000", "region": "us-east-1", "bucket": "pharos-gate",
      "prefix": "pharos", "pathStyle": true,
      "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"
    }
  }
}
JSON
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/bootstrap.done" ]]; then
  echo "== reusing the bootstrap publish ($(cat "$GATE/bootstrap.done"))"
else
  for spec in "alpha|--no-embed" "beta|--no-embed" "lucene|--no-embed" "pharos|"; do
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
fi

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_MODELS="$MODELS" GATE_MODEL_URL="$MODEL_URL"
export GATE_MODEL_ID="$MODEL_ID" GATE_TOKEN="$TOKEN"
export GATE_WRITER_HEAP="${GATE_WRITER_HEAP:-2g}" GATE_WEB_HEAP="${GATE_WEB_HEAP:-640m}"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase4.py" --phases "$PHASES" || rc=$?
echo "== MinIO bucket: $(du -sh "$MINIO_DATA/pharos-gate" | cut -f1); free $(free_gib) GiB; gate exit $rc (logs in $GATE)"
exit "$rc"
