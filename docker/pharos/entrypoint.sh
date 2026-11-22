#!/bin/sh
# Entrypoint of the pharos image.
#   PHAROS_ROLE   writer | reader | all -> `pharos web`; router -> `pharos router`.
#                 The node's cluster role itself comes from cluster.role in the config.
#   PHAROS_HOME   state directory (default /data/pharos, on the PVC)
#   PHAROS_PORT   HTTP port (default 7070 for web, 7080 for router)
#   PHAROS_CONFIG config file to use (default /etc/pharos/config.json, a ConfigMap mount)
#   JAVA_OPTS     extra JVM options
# Secrets (the cluster token, object-store keys) arrive only as env vars named by the config;
# nothing here writes them anywhere. The final `exec` makes the JVM PID 1, so SIGTERM reaches it.
set -eu

role="${PHAROS_ROLE:-all}"
home="${PHAROS_HOME:-/data/pharos}"
config="${PHAROS_CONFIG:-/etc/pharos/config.json}"

mkdir -p "$home"
# pharos reads $PHAROS_HOME/config.json and has no option for another path; link the mounted
# file in rather than copying, so a ConfigMap update is seen on the next start.
if [ -f "$config" ]; then
    ln -sf "$config" "$home/config.json"
elif [ ! -f "$home/config.json" ]; then
    echo "pharos-entrypoint: no config at $config; starting with defaults" >&2
fi

jar=/opt/pharos/pharos.jar
set -- -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Dpharos.home="$home" ${JAVA_OPTS:-} -jar "$jar"

case "$role" in
    writer|reader|all)
        exec java "$@" web --no-open --port "${PHAROS_PORT:-7070}" ;;
    router)
        exec java "$@" router --port "${PHAROS_PORT:-7080}" ;;
    *)
        echo "pharos-entrypoint: PHAROS_ROLE must be writer, reader, router or all (got '$role')" >&2
        exit 64 ;;
esac
