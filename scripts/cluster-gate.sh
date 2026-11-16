#!/usr/bin/env bash
# Phase 1 gate: index pharos + lucene on a writer node, search on a separate reader node,
# both talking to a local MinIO (S3-compatible) object store. See
# docs/superpowers/specs/2026-11-15-multi-node-design.md and docs/superpowers/gates/.
#
# This script NEVER downloads anything. Fetch the MinIO binary yourself (see message below).
# It never touches the real ~/.pharos: writer and reader get isolated homes under $GATE
# (-Duser.home AND -Dpharos.home). The lucene source path is only READ from
# ~/.pharos/registry.json.
#
# Env:
#   MINIO_BIN   path to the minio server binary (default $HOME/.cache/pharos-gate/minio)
#   GATE        work dir (default: fresh mktemp -d)
#   PHAROS_HEAP JVM heap for pharos processes (default 8g)
set -euo pipefail

MINIO_BIN="${MINIO_BIN:-$HOME/.cache/pharos-gate/minio}"
if [[ ! -x "$MINIO_BIN" ]]; then
  cat >&2 <<MSG
cluster-gate: MinIO binary not found or not executable: $MINIO_BIN
This script never downloads. Fetch it yourself with:

  mkdir -p "$(dirname "$MINIO_BIN")" && curl -fL -o "$MINIO_BIN" https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x "$MINIO_BIN"

then re-run scripts/cluster-gate.sh (or point MINIO_BIN at an existing binary).
MSG
  exit 2
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$(ls -1 "$REPO"/target/pharos-*.jar 2>/dev/null | grep -v '/original-' | sort -r | head -1 || true)"
[[ -n "$JAR" ]] || { echo "cluster-gate: no target/pharos-*.jar; run mvn clean package -DskipTests" >&2; exit 2; }

REAL_REGISTRY="$HOME/.pharos/registry.json"
LUCENE_SRC="$(python3 -I - "$REAL_REGISTRY" <<'PY'
import json, os, sys
try:
    r = json.load(open(sys.argv[1]))
    print(os.path.normpath(r["lucene"]["rootPath"]))
except (OSError, ValueError, KeyError, TypeError) as e:
    sys.exit("cluster-gate: no usable 'lucene' entry in %s (%r)" % (sys.argv[1], e))
PY
)" || exit 2
[[ -d "$LUCENE_SRC" ]] || { echo "cluster-gate: lucene rootPath not a directory: $LUCENE_SRC" >&2; exit 2; }

for port in 9000 7300 7301; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "cluster-gate: port $port is already in use (another MinIO or a pharos daemon?); refusing to start" >&2
    exit 2
  fi
done

GATE="${GATE:-$(mktemp -d -t pharos-gate.XXXXXX)}"
mkdir -p "$GATE"
W_HOME="$GATE/writer"; R_HOME="$GATE/reader"
W_PH="$W_HOME/.pharos"; R_PH="$R_HOME/.pharos"
MINIO_DATA="$GATE/minio-data"
PHAROS_SRC="$GATE/pharos-src"      # copy of this repo's sources: the freshness check edits it
mkdir -p "$W_PH" "$R_PH" "$MINIO_DATA"

# MinIO (single-drive mode) exposes every top-level directory of its data dir as a bucket at
# startup, so pre-creating the directory replaces an S3 CreateBucket call (which would need
# SigV4 signing; not available from plain shell / stdlib python).
mkdir -p "$MINIO_DATA/pharos-gate"

export PHAROS_GATE_ACCESS_KEY="gateaccess"
export PHAROS_GATE_SECRET_KEY="gatesecret-0123456789"
export MINIO_ROOT_USER="$PHAROS_GATE_ACCESS_KEY"
export MINIO_ROOT_PASSWORD="$PHAROS_GATE_SECRET_KEY"

POLL=10
write_config() {  # $1 = role, $2 = node id, $3 = pharos home
  cat > "$3/config.json" <<JSON
{
  "cluster": {
    "role": "$1",
    "backend": "object-store",
    "nodeId": "$2",
    "pollIntervalSeconds": $POLL,
    "cacheDir": "$3/cluster-cache",
    "objectStore": {
      "endpoint": "http://127.0.0.1:9000",
      "region": "us-east-1",
      "bucket": "pharos-gate",
      "prefix": "pharos",
      "pathStyle": true,
      "accessKeyEnv": "PHAROS_GATE_ACCESS_KEY",
      "secretKeyEnv": "PHAROS_GATE_SECRET_KEY"
    }
  }
}
JSON
}
write_config writer gate-writer "$W_PH"
write_config reader gate-reader "$R_PH"

PIDS=()
cleanup() {
  trap - EXIT INT TERM
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && wait "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

jvm() {  # $1 = home dir (user.home); rest = pharos args. Always call in a subshell: ( jvm ... )
  local home="$1"; shift
  exec java -Xmx"${PHAROS_HEAP:-8g}" --enable-native-access=ALL-UNNAMED \
    --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED --add-modules jdk.incubator.vector \
    -Dpolyglot.engine.WarnInterpreterOnly=false \
    -Duser.home="$home" -Dpharos.home="$home/.pharos" \
    -jar "$JAR" "$@"
}
wait_http() {  # $1 = url, $2 = timeout seconds
  local end=$((SECONDS + $2))
  until curl -fs -o /dev/null "$1"; do
    (( SECONDS < end )) || { echo "cluster-gate: timeout waiting for $1" >&2; return 1; }
    sleep 1
  done
}

echo "== gate dir: $GATE"
echo "== starting MinIO on 127.0.0.1:9000"
"$MINIO_BIN" server "$MINIO_DATA" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 \
  > "$GATE/minio.log" 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:9000/minio/health/live 60

echo "== copying pharos sources to $PHAROS_SRC"
mkdir -p "$PHAROS_SRC"
cp -r "$REPO/src" "$REPO/pom.xml" "$PHAROS_SRC/"

echo "== indexing pharos + lucene on the writer (publishes to MinIO)"
( jvm "$W_HOME" index "$PHAROS_SRC" --project pharos --no-embed ) > "$GATE/writer-index-pharos.log" 2>&1 \
  || { echo "cluster-gate: indexing pharos failed; see $GATE/writer-index-pharos.log" >&2; tail -n 20 "$GATE/writer-index-pharos.log" >&2; exit 1; }
tail -n 5 "$GATE/writer-index-pharos.log"
( jvm "$W_HOME" index "$LUCENE_SRC" --project lucene --no-embed ) > "$GATE/writer-index-lucene.log" 2>&1 \
  || { echo "cluster-gate: indexing lucene failed; see $GATE/writer-index-lucene.log" >&2; tail -n 20 "$GATE/writer-index-lucene.log" >&2; exit 1; }
tail -n 5 "$GATE/writer-index-lucene.log"

echo "== starting writer web :7300 and reader web :7301"
( jvm "$W_HOME" web --port 7300 --no-open ) > "$GATE/writer-web.log" 2>&1 &
PIDS+=($!)
( jvm "$R_HOME" web --port 7301 --no-open ) > "$GATE/reader-web.log" 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:7300/health 120
wait_http http://127.0.0.1:7301/health 120

echo "== waiting for the reader to materialise both projects"
end=$((SECONDS + 900))
until curl -fs http://127.0.0.1:7301/api/projects | python3 -I -c '
import json, sys
names = {p.get("name") for p in json.load(sys.stdin)}
sys.exit(0 if {"pharos", "lucene"} <= names else 1)'; do
  (( SECONDS < end )) || { echo "cluster-gate: reader never showed both projects" >&2; exit 1; }
  sleep 2
done
# The reader binds its port before its first sync; wait until /health reports ready.
until curl -fs http://127.0.0.1:7301/health | python3 -I -c '
import json, sys
sys.exit(0 if json.load(sys.stdin).get("ready") else 1)'; do
  (( SECONDS < end )) || { echo "cluster-gate: reader never reported ready" >&2; exit 1; }
  sleep 2
done

# The freshness check re-indexes the edited copy on the writer with this command.
export GATE_REINDEX_CMD="java -Xmx${PHAROS_HEAP:-8g} --enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED --add-modules jdk.incubator.vector -Duser.home=$W_HOME -Dpharos.home=$W_PH -jar $JAR index $PHAROS_SRC --project pharos --no-embed"
export GATE_REINDEX_LOG="$GATE/freshness-reindex.log"
export GATE_PHAROS_SRC="$PHAROS_SRC"
export GATE_READER_LOG="$GATE/reader-web.log"
export GATE_POLL_INTERVAL="$POLL"
export GATE_READER_CACHE="$R_PH/cluster-cache"

rc=0
python3 -I "$REPO/scripts/cluster_gate.py" --writer http://127.0.0.1:7300 --reader http://127.0.0.1:7301 || rc=$?

echo "== reader cache size:"; du -sh "$R_PH/cluster-cache" || true
echo "== MinIO bucket size:"; du -sh "$MINIO_DATA/pharos-gate" || true
echo "== gate exit code: $rc (logs in $GATE)"
exit "$rc"
