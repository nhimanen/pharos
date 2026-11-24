#!/usr/bin/env bash
# Real A/B of embedding models through pharos's own pipelines (decision gate, see
# docs/superpowers/specs/2026-11-16-embedding-pooling-design.md, "Decision gate (A/B)").
#
#   model A "jina"    jina-embeddings-v2-base-code (today's default; seeded with the user's embed caches)
#   model B "granite" granite-embedding-english-r2, CLS pooling (embedded from scratch, hours on CPU)
#
# Both models index the SAME pharos checkout and lucene tree (refuses to run if either has uncommitted
# tracked changes, or if HEAD moves during the run), each in an isolated -Dpharos.home / -Duser.home under
# the scratch root. The user's ~/.pharos and ~/.djl.ai are read-only inputs and are never written; a
# marker file + `find -newer` verifies that at the end (also: --check-marker).
# Models run sequentially: index -> start daemon -> benchmark -> stop daemon, then the next model.
# The whole run is wrapped in `systemd-run --user --scope -p MemoryMax=...`.
set -euo pipefail

SELF="$(readlink -f "${BASH_SOURCE[0]}")"
REPO="$(dirname "$(dirname "$SELF")")"

SCRATCH=/tmp/claude-1000/-home-nhimanen-projects-pharos/1cc0da30-3fb2-4012-b656-b98e06fc9241/scratchpad/ab
LUCENE_SRC=/home/nhimanen/projects/lucene/lucene
PHAROS_SRC=/home/nhimanen/projects/pharos
USER_PHAROS="$HOME/.pharos"
USER_DJL="$HOME/.djl.ai"
JINA_PORT=7181
GRANITE_PORT=7182
INDEX_HEAP=5g
DAEMON_HEAP=4g
MEMORY_MAX=10G
MIN_FREE_GB=8
BENCH_TIMEOUT=300
PIPELINES="vector,hybrid,hybrid-rrf,hybrid-reranked"
MODELS="jina,granite"
PROJECTS="pharos,lucene"
ORIG_ARGS=("$@")   # parsing below shifts $@ away; the scope re-exec needs the originals
PRINT_CONFIG=0
DRY_RUN=0; NO_BUILD=0; FRESH=0; CHECK_MARKER=0
JINA_URL="hf://jinaai/jina-embeddings-v2-base-code"
GRANITE_URL="hf://onnx-community/granite-embedding-english-r2-ONNX"
# Model dirs under ~/.djl.ai/pharos needed besides the embedding model (default reranker is ms-marco L6)
EXTRA_MODEL_DIRS="Xenova-ms-marco-MiniLM-L-6-v2 Xenova-ms-marco-MiniLM-L-12-v2"
JINA_DIR=jinaai-jina-embeddings-v2-base-code
GRANITE_DIR=onnx-community-granite-embedding-english-r2-ONNX

usage() {
  cat <<EOF
Usage: scripts/embedding-ab.sh [options]
  --scratch DIR        scratch root (run.log, summary.txt, per-model homes go here)
                       default: $SCRATCH
  --lucene-src DIR     lucene source tree, read-only (default $LUCENE_SRC)
  --pharos-src DIR     pharos checkout to index, read-only (default $PHAROS_SRC)
  --jina-port N        daemon port for jina    (default $JINA_PORT; 7171 is refused)
  --granite-port N     daemon port for granite (default $GRANITE_PORT)
  --index-heap SIZE    -Xmx for indexing JVMs  (default $INDEX_HEAP)
  --daemon-heap SIZE   -Xmx for daemons        (default $DAEMON_HEAP)
  --memory-max SIZE    systemd scope MemoryMax (default $MEMORY_MAX)
  --min-free-gb N      required free space on the scratch filesystem (default $MIN_FREE_GB)
  --models LIST        comma list, subset of jina,granite (default $MODELS)
  --pipelines LIST     benchmark pipelines (default $PIPELINES)
  --bench-timeout S    per-request timeout for benchmark.py (default $BENCH_TIMEOUT)
  --fresh              wipe the per-model dirs inside the scratch root first (otherwise resume:
                       finished projects/benchmarks are skipped)
  --no-build           do not run 'mvn -q -DskipTests package' even if the jar looks stale
  --dry-run            run the preflight checks and print the plan; no copies, no JVMs, no scope
  --print-config       print the resolved options (after the scope re-exec) and exit; no JVM
  --check-marker       only check that ~/.pharos and ~/.djl.ai were not modified since the run started
  -h, --help
Env: INDEX_EXTRA_FLAGS  extra flags for 'pharos index' (e.g. --no-synonyms)
Exit status is non-zero on any failure.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scratch) SCRATCH="$2"; shift 2;;
    --lucene-src) LUCENE_SRC="$2"; shift 2;;
    --pharos-src) PHAROS_SRC="$2"; shift 2;;
    --jina-port) JINA_PORT="$2"; shift 2;;
    --granite-port) GRANITE_PORT="$2"; shift 2;;
    --index-heap) INDEX_HEAP="$2"; shift 2;;
    --daemon-heap) DAEMON_HEAP="$2"; shift 2;;
    --memory-max) MEMORY_MAX="$2"; shift 2;;
    --min-free-gb) MIN_FREE_GB="$2"; shift 2;;
    --models) MODELS="$2"; shift 2;;
    --pipelines) PIPELINES="$2"; shift 2;;
    --bench-timeout) BENCH_TIMEOUT="$2"; shift 2;;
    --fresh) FRESH=1; shift;;
    --no-build) NO_BUILD=1; shift;;
    --dry-run) DRY_RUN=1; shift;;
    --print-config) PRINT_CONFIG=1; shift;;
    --check-marker|--marker) CHECK_MARKER=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2;;
  esac
done

RUN_LOG="$SCRATCH/run.log"
MARKER="$SCRATCH/start.marker"
SUMMARY="$SCRATCH/summary.txt"

log() {
  local line; line="$(date '+%Y-%m-%d %H:%M:%S') $*"
  echo "$line"
  if [[ -d "$SCRATCH" && $DRY_RUN -eq 0 ]]; then echo "$line" >> "$RUN_LOG"; fi
}
die() { log "FATAL: $*"; exit 1; }

# ---- marker check: the user's real data must be untouched -------------------------------------
check_marker() {
  [[ -f "$MARKER" ]] || die "no marker at $MARKER (run not started?)"
  local rc=0 hard soft
  # Hard failures: anything under ~/.djl.ai, or the user's embed caches/indexes of the A/B projects.
  hard="$(find "$USER_DJL" -newer "$MARKER" 2>/dev/null; for p in ${PROJECTS//,/ }; do find "$USER_PHAROS/indexes/$p" -newer "$MARKER" 2>/dev/null; done)"
  # Soft: the rest of ~/.pharos (the user's own daemon on 7171 legitimately writes logs/pid).
  soft="$(find "$USER_PHAROS" -newer "$MARKER" -not -path "$USER_PHAROS/daemon.log*" -not -path "$USER_PHAROS/logs*" \
            -not -path "$USER_PHAROS/daemon.pid" -not -path "$USER_PHAROS/dumps*" 2>/dev/null | grep -v -F -x -f <(printf '%s\n' "$hard") || true)"
  if [[ -n "$hard" ]]; then log "MARKER CHECK FAILED: real data modified:"; echo "$hard" | head -20 | sed 's/^/    /'; rc=1; fi
  if [[ -n "$soft" ]]; then log "marker check WARNING: other ~/.pharos paths changed since start (user's own daemon/CLI?):"; echo "$soft" | head -20 | sed 's/^/    /'; fi
  [[ $rc -eq 0 ]] && log "marker check OK: ~/.djl.ai and the user's indexes/{${PROJECTS}} untouched"
  return $rc
}
if [[ $CHECK_MARKER -eq 1 ]]; then check_marker; exit $?; fi

# ---- re-exec inside a memory-capped systemd scope ----------------------------------------------
if [[ $DRY_RUN -eq 0 && -z "${AB_IN_SCOPE:-}" ]]; then   # (also taken by --print-config, to test the re-exec)
  command -v systemd-run >/dev/null || { echo "systemd-run not found" >&2; exit 1; }
  exec systemd-run --user --scope --quiet -p "MemoryMax=$MEMORY_MAX" --setenv=AB_IN_SCOPE=1 -- bash "$SELF" "${ORIG_ARGS[@]}"
fi

print_config() {
  local probe="$SCRATCH"; while [[ ! -d "$probe" && "$probe" != / ]]; do probe="$(dirname "$probe")"; done
  echo "resolved options (AB_IN_SCOPE=${AB_IN_SCOPE:-no}):"
  echo "  scratch=$SCRATCH  (fs=$(stat -f -c %T "$probe") free=$(( $(df -Pk "$probe" | awk 'NR==2{print $4}')/1024/1024 ))GB, checked on $probe)"
  echo "  lucene-src=$LUCENE_SRC pharos-src=$PHAROS_SRC jina-port=$JINA_PORT granite-port=$GRANITE_PORT"
  echo "  index-heap=$INDEX_HEAP daemon-heap=$DAEMON_HEAP memory-max=$MEMORY_MAX min-free-gb=$MIN_FREE_GB"
  echo "  models=$MODELS pipelines=$PIPELINES bench-timeout=$BENCH_TIMEOUT fresh=$FRESH no-build=$NO_BUILD dry-run=$DRY_RUN"
}
if [[ $PRINT_CONFIG -eq 1 ]]; then print_config; exit 0; fi
[[ $DRY_RUN -eq 0 ]] || print_config

# ---- preflight ---------------------------------------------------------------------------------
declare -A SRC_COMMIT
git_clean_check() {  # name dir
  local name="$1" dir="$2"
  [[ -d "$dir" ]] || die "$name source not found: $dir"
  git -C "$dir" rev-parse --git-dir >/dev/null 2>&1 || die "$name source is not a git checkout: $dir"
  local dirty; dirty="$(git -C "$dir" status --porcelain --untracked-files=no -- . | head -5)"
  [[ -z "$dirty" ]] || die "$name source has uncommitted changes in tracked files (both models must index identical content):
$dirty"
  local c; c="$(git -C "$dir" rev-parse HEAD)"
  if [[ -n "${SRC_COMMIT[$name]:-}" && "${SRC_COMMIT[$name]}" != "$c" ]]; then die "$name HEAD moved during the run: ${SRC_COMMIT[$name]} -> $c"; fi
  SRC_COMMIT[$name]="$c"
}
port_free() { ! ss -ltn "sport = :$1" 2>/dev/null | grep -q LISTEN; }

preflight() {
  git_clean_check pharos "$PHAROS_SRC"
  git_clean_check lucene "$LUCENE_SRC"
  for p in "$JINA_PORT" "$GRANITE_PORT"; do
    [[ "$p" != 7171 ]] || die "port 7171 is the user's daemon"
    port_free "$p" || die "port $p is already in use"
  done
  [[ "$JINA_PORT" != "$GRANITE_PORT" ]] || die "ports must differ"
  command -v java >/dev/null || die "java not found"
  local jv; jv="$(java -version 2>&1 | head -1)"
  command -v python3 >/dev/null || die "python3 not found"
  [[ -d "$USER_PHAROS/indexes" && -d "$USER_DJL/pharos" ]] || die "expected $USER_PHAROS/indexes and $USER_DJL/pharos"
  local m dirs="$EXTRA_MODEL_DIRS"
  [[ ",$MODELS," == *,jina,* ]] && dirs="$dirs $JINA_DIR"
  [[ ",$MODELS," == *,granite,* ]] && dirs="$dirs $GRANITE_DIR"
  for m in $dirs; do [[ -f "$USER_DJL/pharos/$m/model.onnx" ]] || die "missing model dir $USER_DJL/pharos/$m"; done
  if [[ ",$MODELS," == *,jina,* ]]; then
    for p in ${PROJECTS//,/ }; do
      [[ -f "$USER_PHAROS/indexes/$p/embed-cache.bin" && -f "$USER_PHAROS/indexes/$p/embed-cache.meta" ]] \
        || die "missing jina cache $USER_PHAROS/indexes/$p/embed-cache.{bin,meta}"
    done
  fi
  mkdir -p "$SCRATCH" 2>/dev/null || true
  local probe="$SCRATCH"; [[ -d "$probe" ]] || probe="$(dirname "$SCRATCH")"
  local fstype free_kb; fstype="$(stat -f -c %T "$probe")"; free_kb="$(df -Pk "$probe" | awk 'NR==2{print $4}')"
  log "preflight: $jv; scratch fs=$fstype free=$((free_kb/1024/1024))GB; MemAvailable=$(awk '/MemAvailable/{printf "%.1f", $2/1048576}' /proc/meminfo)GB"
  [[ "$fstype" != tmpfs ]] || log "WARNING: scratch is on tmpfs (RAM-backed); indexes + model copies (~6-8 GB) count against memory. Prefer a disk path via --scratch."
  (( free_kb/1024/1024 >= MIN_FREE_GB )) || die "only $((free_kb/1024/1024))GB free on scratch fs, need >= ${MIN_FREE_GB}GB (--min-free-gb)"
  log "preflight: pharos=${SRC_COMMIT[pharos]} lucene=${SRC_COMMIT[lucene]} (sources clean)"
}

JAR=""
find_jar() {
  JAR="$(ls -t "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | head -1 || true)"
  local stale=0
  if [[ -z "$JAR" ]]; then stale=1
  elif [[ -n "$(find "$REPO/src" "$REPO/pom.xml" -newer "$JAR" -type f 2>/dev/null | head -1)" ]]; then stale=1; fi
  if [[ $stale -eq 1 ]]; then
    [[ $NO_BUILD -eq 0 ]] || die "jar missing/stale and --no-build given"
    if [[ $DRY_RUN -eq 1 ]]; then log "dry-run: would build jar: (cd $REPO && mvn -q -DskipTests package)"; return; fi
    log "building jar (sources newer than jar)"
    (cd "$REPO" && mvn -q -DskipTests package) >> "$RUN_LOG" 2>&1 || die "mvn package failed (see $RUN_LOG)"
    JAR="$(ls -t "$REPO"/target/pharos-*.jar | grep -v '/original-' | head -1)"
  fi
  log "jar: $JAR ($(git -C "$REPO" rev-parse --short HEAD) $(git -C "$REPO" branch --show-current))"
}

preflight
find_jar
BENCH_MODELS=","; for m in ${MODELS//,/ }; do BENCH_MODELS="$BENCH_MODELS$m,"; done

if [[ $DRY_RUN -eq 1 ]]; then
  log "dry-run plan:"
  for m in ${MODELS//,/ }; do
    port=$JINA_PORT; [[ $m == granite ]] && port=$GRANITE_PORT
    echo "  [$m] home=$SCRATCH/$m/{pharos-home,user-home} port=$port"
    echo "    index (-Xmx$INDEX_HEAP, --single --project NAME): pharos <- $PHAROS_SRC ; lucene <- $LUCENE_SRC"
    [[ $m == jina ]] && echo "    seed: copies of $USER_PHAROS/indexes/{${PROJECTS}}/embed-cache.{bin,meta}"
    echo "    daemon (-Xmx$DAEMON_HEAP) -> benchmark.py --projects $PROJECTS --pipelines $PIPELINES -> $SCRATCH/$m/bench.json"
  done
  echo "  compare: eval/ab_compare.py jina/bench.json granite/bench.json -> $SCRATCH/ab-report.md, decision.json"
  echo "  scope: systemd-run --user --scope -p MemoryMax=$MEMORY_MAX"
  log "dry-run OK"
  exit 0
fi

# ---- run bookkeeping ----------------------------------------------------------------------------
mkdir -p "$SCRATCH"
RUN_STARTED=$SECONDS
if [[ ! -f "$MARKER" ]]; then touch "$MARKER"; fi   # resume keeps the original marker
log "=== embedding A/B start; scratch=$SCRATCH models=$MODELS pipelines=$PIPELINES ==="
log "memory scope MemoryMax=$MEMORY_MAX, index heap=$INDEX_HEAP, daemon heap=$DAEMON_HEAP"

DAEMON_PID=""
stop_daemon() {
  [[ -n "$DAEMON_PID" ]] || return 0
  if kill -0 "$DAEMON_PID" 2>/dev/null; then
    log "stopping daemon pid $DAEMON_PID"
    kill "$DAEMON_PID" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$DAEMON_PID" 2>/dev/null || break; sleep 1; done
    kill -0 "$DAEMON_PID" 2>/dev/null && { log "daemon did not exit, SIGKILL pid $DAEMON_PID"; kill -9 "$DAEMON_PID" 2>/dev/null || true; }
  fi
  DAEMON_PID=""
}
INDEX_PID=""
cleanup() {
  local rc=$?
  stop_daemon
  if [[ -n "$INDEX_PID" ]] && kill -0 "$INDEX_PID" 2>/dev/null; then log "killing index pid $INDEX_PID"; kill "$INDEX_PID" 2>/dev/null || true; fi
  [[ $rc -eq 0 ]] || log "=== FAILED (exit $rc) after $((SECONDS-RUN_STARTED))s ==="
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT TERM

JVM_FLAGS=(--enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED
           --add-modules jdk.incubator.vector -XX:+ExitOnOutOfMemoryError -Dpolyglot.engine.WarnInterpreterOnly=false)

model_url()  { [[ $1 == jina ]] && echo "$JINA_URL" || echo "$GRANITE_URL"; }
model_dir()  { [[ $1 == jina ]] && echo "$JINA_DIR" || echo "$GRANITE_DIR"; }
model_port() { [[ $1 == jina ]] && echo "$JINA_PORT" || echo "$GRANITE_PORT"; }

setup_home() {  # model
  local m="$1" base="$SCRATCH/$1" ph="$SCRATCH/$1/pharos-home" uh="$SCRATCH/$1/user-home"
  case "$base" in "$SCRATCH"/*) ;; *) die "refusing to operate outside scratch: $base";; esac
  if [[ $FRESH -eq 1 && -d "$base" ]]; then log "[$m] --fresh: removing $base"; rm -rf -- "$base"; fi
  mkdir -p "$ph/indexes" "$uh/.djl.ai/pharos"
  if [[ ! -f "$base/.home-ready" ]]; then
    log "[$m] creating isolated home (config, model copies)"
    if [[ $m == jina ]]; then
      # modelId "legacy" == what the user's legacy embeddingModelUrl config migrates to, so the
      # unsuffixed embed-cache.{bin,meta} copies below are valid for this run.
      cat > "$ph/config.json" <<EOF
{
  "embeddingProviders": [
    {"type": "djl", "modelId": "legacy", "url": "$JINA_URL", "dimensions": 768, "maxTokens": 512}
  ],
  "searchEmbeddingModel": "legacy",
  "crossEncoderEnabled": true
}
EOF
    else
      cat > "$ph/config.json" <<EOF
{
  "embeddingProviders": [
    {"type": "djl", "modelId": "granite-r2", "url": "$GRANITE_URL", "dimensions": 768, "maxTokens": 512, "pooling": "cls"}
  ],
  "searchEmbeddingModel": "granite-r2",
  "crossEncoderEnabled": true
}
EOF
    fi
    local d
    for d in "$(model_dir "$m")" $EXTRA_MODEL_DIRS; do
      [[ -d "$uh/.djl.ai/pharos/$d" ]] || cp -a --reflink=auto "$USER_DJL/pharos/$d" "$uh/.djl.ai/pharos/$d"
    done
    for d in tokenizers cache; do   # DJL native libs; avoid re-downloading under the fake user.home
      [[ -d "$USER_DJL/$d" && ! -d "$uh/.djl.ai/$d" ]] && cp -a --reflink=auto "$USER_DJL/$d" "$uh/.djl.ai/$d"
    done
    if [[ $m == jina ]]; then
      local p
      for p in ${PROJECTS//,/ }; do
        mkdir -p "$ph/indexes/$p"
        cp -a --reflink=auto "$USER_PHAROS/indexes/$p/embed-cache.bin" "$USER_PHAROS/indexes/$p/embed-cache.meta" "$ph/indexes/$p/"
      done
      log "[$m] seeded embed caches: $(du -ch "$ph"/indexes/*/embed-cache.bin | tail -1 | cut -f1)"
    fi
    touch "$base/.home-ready"
  fi
}

jvm_common() {  # model -> prints args
  local m="$1"
  echo "-Dpharos.home=$SCRATCH/$m/pharos-home" "-Duser.home=$SCRATCH/$m/user-home"
}

index_project() {  # model project srcdir
  local m="$1" proj="$2" src="$3" base="$SCRATCH/$1"
  local done_mark="$base/.indexed.$proj" ilog="$base/index-$proj.log"
  if [[ -f "$done_mark" ]]; then log "[$m] index $proj: already done, skipping"; return; fi
  local key=pharos; [[ $proj == lucene ]] && key=lucene
  git_clean_check "$key" "$src"
  local commit="${SRC_COMMIT[$key]}"
  log "[$m] index $proj: start (source ${commit:0:10}); log $ilog"
  local t0=$SECONDS
  # shellcheck disable=SC2046,SC2086
  java -Xmx"$INDEX_HEAP" "${JVM_FLAGS[@]}" $(jvm_common "$m") -jar "$JAR" index "$src" --single \
       --project "$proj" --no-progress ${INDEX_EXTRA_FLAGS:-} > "$ilog" 2>&1 &
  INDEX_PID=$!
  local last=$SECONDS
  while kill -0 "$INDEX_PID" 2>/dev/null; do
    sleep 5
    if (( SECONDS - last >= 60 )); then
      last=$SECONDS
      local cache; cache="$(du -sh "$base/pharos-home/indexes/$proj" 2>/dev/null | cut -f1)"
      log "[$m] index $proj: running $((SECONDS-t0))s, index dir ${cache:-?}, last: $(tail -n1 "$ilog" | tr '\r' '\n' | tail -n1 | cut -c1-160)"
    fi
  done
  local rc=0; wait "$INDEX_PID" || rc=$?
  INDEX_PID=""
  [[ $rc -eq 0 ]] || { tail -20 "$ilog" | sed 's/^/    | /'; die "[$m] index $proj failed (exit $rc), see $ilog"; }
  { grep -iE "indexed project|embedd|cache" "$ilog" || true; } | tail -5 | while read -r l; do log "    [$m $proj] $l"; done
  touch "$done_mark"
  log "[$m] index $proj: done in $((SECONDS-t0))s"
}

start_daemon() {  # model
  local m="$1" port; port="$(model_port "$m")"
  port_free "$port" || die "port $port in use"
  # shellcheck disable=SC2046
  java -Xmx"$DAEMON_HEAP" "${JVM_FLAGS[@]}" $(jvm_common "$m") -jar "$JAR" web --port "$port" --no-open \
       > "$SCRATCH/$m/daemon.log" 2>&1 &
  DAEMON_PID=$!
  log "[$m] daemon pid $DAEMON_PID on port $port"
  for _ in $(seq 1 120); do
    kill -0 "$DAEMON_PID" 2>/dev/null || die "[$m] daemon exited early, see $SCRATCH/$m/daemon.log"
    curl -fsS -m 2 "http://localhost:$port/health" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "[$m] daemon not healthy within 120s"
}

smoke_search() {  # model project pipeline  -> fails if zero results
  local m="$1" proj="$2" pl="$3" port; port="$(model_port "$m")"
  local n; n="$(curl -fsS -m 300 -G "http://localhost:$port/api/search" --data-urlencode "q=index writer" \
      --data-urlencode "project=$proj" --data-urlencode "pipeline=$pl" --data-urlencode limit=10 \
      | python3 -I -c 'import json,sys; d=json.load(sys.stdin); d=d.get("results",[]) if isinstance(d,dict) else d; print(len(d))')" \
      || die "[$m] smoke search $proj/$pl failed"
  [[ "$n" -gt 0 ]] || die "[$m] smoke search $proj/$pl returned 0 results (index/embeddings missing?)"
  log "[$m] smoke $proj/$pl: $n results"
}

benchmark() {  # model
  local m="$1" base="$SCRATCH/$1" port; port="$(model_port "$m")"
  if [[ -f "$base/bench.json" ]]; then log "[$m] benchmark: already done, skipping"; return; fi
  start_daemon "$m"
  local t0=$SECONDS p
  for p in ${PROJECTS//,/ }; do smoke_search "$m" "$p" vector; smoke_search "$m" "$p" hybrid-reranked; done
  log "[$m] benchmark: start (warm-up done in $((SECONDS-t0))s); pipelines=$PIPELINES"
  python3 "$REPO/eval/benchmark.py" --base-url "http://localhost:$port" --cache-file "$base/bench-cache.json" \
      --projects "$PROJECTS" --pipelines "$PIPELINES" --timeout "$BENCH_TIMEOUT" --json-out "$base/bench.json.tmp" \
      2> >(while read -r l; do log "[$m] bench: $l"; done) || die "[$m] benchmark.py failed"
  python3 -I - "$base/bench.json.tmp" "$m" <<'PY' || die "[$m] benchmark produced empty results"
import json, sys
d = json.load(open(sys.argv[1]))
bad = {p: n for p, n in d["empty_result_queries"].items() if n > 0.2 * len(d["queries"])}
if bad:
    print(f"{sys.argv[2]}: >20% queries with empty results: {bad}", file=sys.stderr); sys.exit(1)
PY
  mv "$base/bench.json.tmp" "$base/bench.json"
  log "[$m] benchmark: done in $((SECONDS-t0))s -> $base/bench.json"
  stop_daemon
}

declare -A PHASE
for m in ${MODELS//,/ }; do
  [[ $m == jina || $m == granite ]] || die "unknown model '$m'"
  t0=$SECONDS
  log "=== model $m ($(model_url "$m")) ==="
  setup_home "$m"
  index_project "$m" pharos "$PHAROS_SRC"
  index_project "$m" lucene "$LUCENE_SRC"
  PHASE[$m.index]=$((SECONDS-t0))
  t1=$SECONDS
  benchmark "$m"
  PHASE[$m.bench]=$((SECONDS-t1))
  log "=== model $m finished: index phase ${PHASE[$m.index]}s, benchmark phase ${PHASE[$m.bench]}s ==="
done

git_clean_check pharos "$PHAROS_SRC"; git_clean_check lucene "$LUCENE_SRC"   # sources unchanged for the whole run

DECISION="n/a (needs both models)"
if [[ ",$MODELS," == *,jina,* && ",$MODELS," == *,granite,* ]]; then
  log "=== compare ==="
  python3 -I "$REPO/eval/ab_compare.py" "$SCRATCH/jina/bench.json" "$SCRATCH/granite/bench.json" \
      --label-a jina --label-b granite --out "$SCRATCH/ab-report.md" --json-out "$SCRATCH/decision.json" > "$SCRATCH/ab-report.stdout" \
      || die "ab_compare.py failed"
  DECISION="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["decision"])' "$SCRATCH/decision.json")"
  log "DECISION: $DECISION (report $SCRATCH/ab-report.md)"
fi

MARKER_RESULT=OK
check_marker || MARKER_RESULT=FAILED
{
  echo "embedding A/B summary $(date '+%Y-%m-%d %H:%M:%S')"
  echo "pharos source: $PHAROS_SRC @ ${SRC_COMMIT[pharos]}"
  echo "lucene source: $LUCENE_SRC @ ${SRC_COMMIT[lucene]}"
  echo "jar: $JAR (worktree $(git -C "$REPO" rev-parse HEAD))"
  echo "models: $MODELS   projects: $PROJECTS   pipelines: $PIPELINES"
  for m in ${MODELS//,/ }; do echo "$m: index ${PHASE[$m.index]}s, benchmark ${PHASE[$m.bench]}s, result $SCRATCH/$m/bench.json"; done
  echo "total: $((SECONDS-RUN_STARTED))s"
  echo "decision: $DECISION"
  echo "report: $SCRATCH/ab-report.md  decision.json: $SCRATCH/decision.json"
  echo "marker check (~/.pharos, ~/.djl.ai untouched): $MARKER_RESULT"
} > "$SUMMARY"
log "summary: $SUMMARY"
[[ "$MARKER_RESULT" == OK ]] || die "marker check failed: real data was modified"
log "=== embedding A/B complete in $((SECONDS-RUN_STARTED))s ==="
