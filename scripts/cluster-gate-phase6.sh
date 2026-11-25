#!/usr/bin/env bash
# Phase 6 gate: shard specs and live resharding against a local MinIO. See
# docs/superpowers/plans/2026-11-17-multi-node-phase-6.md (Task 6) and the results in
# docs/superpowers/gates/2026-11-22-phase-6.md.
#
# This script starts MinIO, builds two local bare git repos under $GATE/repos (one-commit snapshots,
# never the source trees themselves):
#   lucene   `git archive HEAD` of $GATE_LUCENE_SRC (a git clone; indexed --no-embed)
#   pharos   src/main, src/test/resources/test-projects and pom.xml of $GATE_PHAROS_SRC (embedded;
#            the test projects are build modules, so [project, module] gives pharos several shards)
# A bootstrap writer (role writer, workQueue false) indexes each and publishes it as generation 1 with
# `--source-git <bare repo> --ref main`, which records the source in the catalog. Then
# scripts/cluster_gate_phase6.py runs the gate: work-queue writers W1 W2, readers R1-R3 (all shards,
# lazy, jina for the hybrid and vector queries), the router, ~10 QPS of mixed queries, three reshards,
# the pruning traces and a writer killed mid-reshard. Every JVM is started and stopped by its PID.
#
# It NEVER downloads anything and never touches a real ~/.pharos or ~/.djl.ai: every JVM gets an
# isolated user.home and pharos.home under $GATE. The embedding model must already be in
# $GATE_MODELS_HOME/.djl.ai/pharos/<model dir> (each node's .djl.ai is a symlink to it).
#
# Run under scripts/gate-watchdog.sh (one hard memory cap; it stops the gate when MemAvailable runs low):
#   G=~/builds/phase6-gate/run1
#   GATE=$G bash scripts/gate-watchdog.sh --state-dir $G.wd --mem-max 9G --min-available 3072 -- \
#     env GATE_LUCENE_SRC=<lucene clone> GATE_MODELS_HOME=<dir with .djl.ai> bash scripts/cluster-gate-phase6.sh
# Keyword only (no embedding model anywhere; --mem-max 6G is enough):
#   GATE=$G bash scripts/gate-watchdog.sh --state-dir $G.wd --mem-max 6G --min-available 3072 -- \
#     env GATE_KEYWORD_ONLY=1 GATE_LUCENE_SRC=<lucene clone> bash scripts/cluster-gate-phase6.sh
#
# Env:
#   GATE               work dir (required unless --dry-run; on disk)
#   GATE_LUCENE_SRC    lucene git clone (required)
#   GATE_PHAROS_SRC    pharos source tree with src/ and pom.xml (default: this repo)
#   GATE_MODELS_HOME   dir holding .djl.ai/pharos/<model dir> (required unless GATE_KEYWORD_ONLY=1)
#   GATE_KEYWORD_ONLY  0 (default); 1: no embedding model on any node and none linked: pharos is indexed
#                      --no-embed too, and the ranking, the load and the pruning traces use the keyword
#                      pipeline only (hybrid and vector parity after a reshard is then not checked). A GATE
#                      dir set up in the other mode is refused ($GATE/gate.mode).
#   MINIO_BIN          minio server binary (default $HOME/.cache/pharos-gate/minio)
#   GATE_S3_PORT       MinIO port (default 19000: WSL hosts may hold 9000 from the Windows side; the
#                      console takes the next port)
#   GATE_ROUTER_PORT   router port (default 7690)
#   GATE_R1_PORT GATE_R2_PORT GATE_R3_PORT   reader ports (default 7601 7602 7603)
#   GATE_W1_PORT GATE_W2_PORT                writer ports (default 7611 7612)
#   GATE_PORT_PROBE_TIMEOUT  seconds per busy-port probe (default 2)
#   GATE_JAR           pharos jar to run (default target/pharos-*.jar; copied to $GATE/pharos.jar)
#   PHAROS_INDEX_HEAP  heap of the bootstrap index runs (default 3g)
#   GATE_WRITER_HEAP   heap of each work-queue writer (default 2g)
#   GATE_WEB_HEAP      heap of each reader and the router (default 1g)
#   GATE_MODEL_URL     embedding model (default hf://jinaai/jina-embeddings-v2-base-code, 768 dims)
#   GATE_MODEL_ID      its provider modelId (default jina-code-v2)
#   GATE_REUSE=1       skip the repos and the bootstrap when $GATE already holds them (re-runs of the driver)
#   GATE_PHASES        driver phases (default "setup,before,reshard,after,pruning,interrupt,check")
#   GATE_SEED_EMBED_CACHE  a dir holding embed-cache.<model>.{bin,meta} of an earlier gate's bootstrap pharos
#                      index (the same pharos source): copied into the bootstrap's pharos index dir, so the
#                      bootstrap embeds nothing again (the gate's own reshard checks do not depend on it)
# Flags: --help, --dry-run (print the plan and exit; starts nothing)
set -euo pipefail

usage() { sed -n '2,/^set -euo pipefail/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'; }
DRY=0
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "cluster-gate-phase6: unknown argument $a" >&2; exit 2 ;;
  esac
done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_BIN="${MINIO_BIN:-$HOME/.cache/pharos-gate/minio}"
S3_PORT="${GATE_S3_PORT:-19000}"
INDEX_HEAP="${PHAROS_INDEX_HEAP:-3g}"
MODEL_URL="${GATE_MODEL_URL:-hf://jinaai/jina-embeddings-v2-base-code}"
MODEL_DIR_NAME="$(echo "${MODEL_URL#hf://}" | tr '/' '-')"
MODEL_ID="${GATE_MODEL_ID:-jina-code-v2}"
PHASES="${GATE_PHASES:-setup,before,reshard,after,pruning,interrupt,check}"
PHAROS_SRC="${GATE_PHAROS_SRC:-$REPO}"
# by version, not by name: a leftover pharos-1.9.0.jar must not beat pharos-1.11.0.jar
JAR="${GATE_JAR:-$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -V -r | head -1 || true)}"
TOKEN="gate6-cluster-token-0123456789"
KEYWORD_ONLY="${GATE_KEYWORD_ONLY:-0}"
[[ "$KEYWORD_ONLY" == 0 || "$KEYWORD_ONLY" == 1 ]] || { echo "cluster-gate-phase6: GATE_KEYWORD_ONLY must be 0 or 1" >&2; exit 2; }
MODE=embed; (( KEYWORD_ONLY )) && MODE=keyword
EMBED_FLAG=""; (( KEYWORD_ONLY )) && EMBED_FLAG="--no-embed"   # pharos (lucene is always --no-embed)
export GATE_ROUTER_PORT="${GATE_ROUTER_PORT:-7690}" GATE_R1_PORT="${GATE_R1_PORT:-7601}" \
  GATE_R2_PORT="${GATE_R2_PORT:-7602}" GATE_R3_PORT="${GATE_R3_PORT:-7603}" \
  GATE_W1_PORT="${GATE_W1_PORT:-7611}" GATE_W2_PORT="${GATE_W2_PORT:-7612}"
PROBE_TIMEOUT="${GATE_PORT_PROBE_TIMEOUT:-2}"
PORTS=("$S3_PORT" "$((S3_PORT + 1))" "$GATE_ROUTER_PORT" "$GATE_R1_PORT" "$GATE_R2_PORT" "$GATE_R3_PORT"
       "$GATE_W1_PORT" "$GATE_W2_PORT")

if (( DRY )); then
  cat <<PLAN
cluster-gate-phase6 (dry run): nothing is started.
  repo        $REPO
  jar         ${JAR:-<missing: mvn clean package -DskipTests>}
  minio       $MINIO_BIN on 127.0.0.1:$S3_PORT
  gate dir    ${GATE:-<GATE unset>}
  mode        $MODE (GATE_KEYWORD_ONLY=$KEYWORD_ONLY)
  sources     lucene ${GATE_LUCENE_SRC:-<GATE_LUCENE_SRC unset>} (--no-embed), pharos $PHAROS_SRC ($( (( KEYWORD_ONLY )) && echo "--no-embed" || echo embedded))
  model       $( (( KEYWORD_ONLY )) && echo "none (keyword only)" || echo "$MODEL_URL as '$MODEL_ID' from ${GATE_MODELS_HOME:-<GATE_MODELS_HOME unset>}")
  bootstrap   role writer, workQueue false: index + publish each with --source-git <bare> --ref main
  ports       ${PORTS[*]}
  phases      $PHASES
PLAN
  GATE_KEYWORD_ONLY="$KEYWORD_ONLY" python3 -I "$REPO/scripts/cluster_gate_phase6.py" --dry-run --phases "$PHASES"
  exit 0
fi

[[ -n "${GATE:-}" ]] || { echo "cluster-gate-phase6: set GATE to a work dir on disk" >&2; exit 2; }
[[ -n "${GATE_LUCENE_SRC:-}" ]] || { echo "cluster-gate-phase6: set GATE_LUCENE_SRC to a lucene git clone" >&2; exit 2; }
(( KEYWORD_ONLY )) || [[ -n "${GATE_MODELS_HOME:-}" ]] || { echo "cluster-gate-phase6: set GATE_MODELS_HOME (or GATE_KEYWORD_ONLY=1)" >&2; exit 2; }
[[ -x "$MINIO_BIN" ]] || { echo "cluster-gate-phase6: MinIO binary not executable: $MINIO_BIN (this script never downloads)" >&2; exit 2; }
[[ -n "$JAR" && -f "$JAR" ]] || { echo "cluster-gate-phase6: no jar; run mvn clean package -DskipTests" >&2; exit 2; }
if (( ! KEYWORD_ONLY )); then
  MODELS="$(cd "$GATE_MODELS_HOME" && pwd)/.djl.ai"
  [[ -f "$MODELS/pharos/$MODEL_DIR_NAME/model.onnx" ]] || { echo "cluster-gate-phase6: model not in $MODELS/pharos/$MODEL_DIR_NAME" >&2; exit 2; }
fi
git -C "$GATE_LUCENE_SRC" rev-parse --verify -q HEAD >/dev/null || { echo "cluster-gate-phase6: $GATE_LUCENE_SRC is not a git work tree" >&2; exit 2; }
[[ -d "$PHAROS_SRC/src/main" && -f "$PHAROS_SRC/pom.xml" ]] || { echo "cluster-gate-phase6: $PHAROS_SRC has no src/main and pom.xml" >&2; exit 2; }

for port in "${PORTS[@]}"; do   # bounded: on WSL a connect to a closed port can hang for minutes
  if timeout "$PROBE_TIMEOUT" bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    echo "cluster-gate-phase6: port $port is already in use; refusing to start" >&2; exit 2
  fi
done

mkdir -p "$GATE"
if [[ -f "$GATE/gate.mode" ]]; then
  prev_mode="$(cat "$GATE/gate.mode")"
  [[ "$prev_mode" == "$MODE" ]] || { echo "cluster-gate-phase6: $GATE was set up in $prev_mode mode, this run is $MODE (GATE_KEYWORD_ONLY); use a fresh GATE" >&2; exit 2; }
else
  echo "$MODE" > "$GATE/gate.mode"
fi
[[ -f "$GATE/marker" ]] || touch "$GATE/marker"   # safety check: nothing under ~/.pharos or ~/.djl.ai newer
if (( KEYWORD_ONLY )); then   # each node's .djl.ai points at an empty gate dir: nothing to load, nothing shared
  MODELS="$GATE/no-models/.djl.ai"
  mkdir -p "$MODELS"
fi
cp -f "$JAR" "$GATE/pharos.jar"   # a later `mvn package` does not change the running gate
JAR="$GATE/pharos.jar"
MINIO_DATA="$GATE/minio-data"
mkdir -p "$MINIO_DATA/pharos-gate" "$GATE/repos" "$GATE/edit" "$GATE/nodes" "$GATE/logs"

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
mem() { awk '/MemAvailable/{printf "%.1f GiB", $2/1048576}' /proc/meminfo; }

echo "== gate dir $GATE; MemAvailable $(mem); load $(cut -d' ' -f1-3 /proc/loadavg)"
"$MINIO_BIN" server "$MINIO_DATA" --address "127.0.0.1:$S3_PORT" --console-address "127.0.0.1:$((S3_PORT + 1))" \
  > "$GATE/logs/minio.log" 2>&1 &
PIDS+=($!)
echo "$!" > "$GATE/minio.pid"
echo "== MinIO pid $! on 127.0.0.1:$S3_PORT"
end=$((SECONDS + 60))
until curl -fs -m 2 -o /dev/null "http://127.0.0.1:$S3_PORT/minio/health/live"; do
  (( SECONDS < end )) || { echo "cluster-gate-phase6: MinIO not live after 60 s" >&2; exit 1; }
  sleep 1
done

# ---------------------------------------------------------------- repos (snapshots)
gitq() { git -c user.name=pharos-gate -c user.email=gate@localhost -c init.defaultBranch=main "$@"; }
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/repos.done" ]]; then
  echo "== reusing the repos ($(cat "$GATE/repos.done"))"
else
  for n in lucene pharos; do
    [[ -e "$GATE/edit/$n" || -e "$GATE/repos/$n.git" ]] && { echo "cluster-gate-phase6: $GATE/edit/$n exists; use a fresh GATE or GATE_REUSE=1" >&2; exit 2; }
  done
  mkdir -p "$GATE/edit/lucene" "$GATE/edit/pharos/src/test/resources"
  git -C "$GATE_LUCENE_SRC" archive --format=tar HEAD | tar -x -C "$GATE/edit/lucene"
  cp -r "$PHAROS_SRC/src/main" "$GATE/edit/pharos/src/main"
  cp -r "$PHAROS_SRC/src/test/resources/test-projects" "$GATE/edit/pharos/src/test/resources/test-projects"
  cp "$PHAROS_SRC/pom.xml" "$GATE/edit/pharos/pom.xml"
  for n in lucene pharos; do
    gitq init -q "$GATE/edit/$n"
    gitq -C "$GATE/edit/$n" add -A
    gitq -C "$GATE/edit/$n" commit -q -m "gate seed of $n"
    gitq init -q --bare "$GATE/repos/$n.git"
    gitq -C "$GATE/edit/$n" remote add origin "$GATE/repos/$n.git"
    gitq -C "$GATE/edit/$n" push -q origin main
    echo "   repo $n: $(git -C "$GATE/edit/$n" ls-files | wc -l) files, $(du -sh "$GATE/repos/$n.git" | cut -f1)"
  done
  echo "lucene $(git -C "$GATE_LUCENE_SRC" rev-parse --short HEAD); $(date -Is)" > "$GATE/repos.done"
fi

# ---------------------------------------------------------------- bootstrap: hand-run writer publishes gen 1
B_HOME="$GATE/nodes/bootstrap"; B_PH="$B_HOME/.pharos"
mkdir -p "$B_PH"
ln -sfn "$MODELS" "$B_HOME/.djl.ai"
models=""
(( KEYWORD_ONLY )) || models='"embeddingProviders": [
    {"type": "djl", "modelId": "'"$MODEL_ID"'", "url": "'"$MODEL_URL"'", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "'"$MODEL_ID"'",'
cat > "$B_PH/config.json" <<JSON
{
  $models
  "crossEncoderEnabled": false,
  "cluster": {
    "role": "writer", "backend": "object-store", "nodeId": "gate-bootstrap",
    "cacheDir": "$B_PH/cluster-cache", "retentionGenerations": 1000, "retentionMinutes": 100000,
    "writer": {"workQueue": false},
    "objectStore": {
      "endpoint": "http://127.0.0.1:$S3_PORT", "region": "us-east-1", "bucket": "pharos-gate",
      "prefix": "pharos", "pathStyle": true,
      "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY", "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"
    }
  }
}
JSON
if [[ "${GATE_REUSE:-0}" == 1 && -f "$GATE/bootstrap.done" ]]; then
  echo "== reusing the bootstrap publish ($(cat "$GATE/bootstrap.done"))"
else
  if (( ! KEYWORD_ONLY )) && [[ -n "${GATE_SEED_EMBED_CACHE:-}" ]]; then
    mkdir -p "$B_PH/indexes/pharos"
    cp "$GATE_SEED_EMBED_CACHE/embed-cache.$MODEL_ID.bin" "$GATE_SEED_EMBED_CACHE/embed-cache.$MODEL_ID.meta" \
      "$B_PH/indexes/pharos/"
    echo "== seeded the bootstrap's pharos embedding cache from $GATE_SEED_EMBED_CACHE"
  fi
  for spec in "lucene|--no-embed" "pharos|$EMBED_FLAG"; do
    IFS='|' read -r name flag <<<"$spec"
    echo "== bootstrap: index $name ${flag:-(with embeddings)}; MemAvailable $(mem); load $(cut -d' ' -f1-3 /proc/loadavg)"
    t0=$SECONDS
    # shellcheck disable=SC2086
    java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" -Duser.home="$B_HOME" -Dpharos.home="$B_PH" \
      -jar "$JAR" index "$GATE/edit/$name" --single --project "$name" --no-progress $flag \
      --source-git "$GATE/repos/$name.git" --ref main > "$GATE/logs/bootstrap-index-$name.log" 2>&1 \
      || { echo "cluster-gate-phase6: indexing $name failed; see $GATE/logs/bootstrap-index-$name.log" >&2
           tail -n 20 "$GATE/logs/bootstrap-index-$name.log" >&2; exit 1; }
    echo "   $name indexed + published in $((SECONDS - t0)) s"
    grep -E "Published shard|recorded source|Indexed project|Embedding cache" "$GATE/logs/bootstrap-index-$name.log" | tail -n 4 || true
  done
  date -Is > "$GATE/bootstrap.done"
fi

export GATE GATE_JAR="$JAR" GATE_LOGBACK="$LOGBACK" GATE_MODELS="$MODELS" GATE_MODEL_URL="$MODEL_URL"
export GATE_MODEL_ID="$MODEL_ID" GATE_TOKEN="$TOKEN" GATE_S3_PORT="$S3_PORT" GATE_KEYWORD_ONLY="$KEYWORD_ONLY"
export GATE_WRITER_HEAP="${GATE_WRITER_HEAP:-2g}" GATE_WEB_HEAP="${GATE_WEB_HEAP:-1g}"
rc=0
python3 -I -u "$REPO/scripts/cluster_gate_phase6.py" --phases "$PHASES" || rc=$?
echo "== MinIO bucket: $(du -sh "$MINIO_DATA/pharos-gate" | cut -f1); gate exit $rc (logs in $GATE)"
exit "$rc"
