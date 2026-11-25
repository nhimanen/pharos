#!/bin/sh
# Entrypoint of the pharos image.
#   PHAROS_ROLE   writer | reader | all -> `pharos web`; router -> `pharos router`.
#                 The node's cluster role itself comes from cluster.role in the config.
#   PHAROS_HOME   state directory (default /data/pharos, on the PVC); also the JVM's user.home, so
#                 the embedding model cache (~/.djl.ai) lands on the PVC: uid 10001 has no home dir
#   PHAROS_PORT   HTTP port (default 7070 for web, 7080 for router)
#   PHAROS_CONFIG config file to use (default /etc/pharos/config.json, a ConfigMap mount)
#   JAVA_OPTS     extra JVM options
# Arguments, when given, are one pharos command to run instead of the server (an operator Job:
# `index --source-git <url> --ref <ref> --project <name>`, `remove-index --from-catalog <name>`);
# the container exits with its code. The config is linked in the same way.
# Secrets (the cluster token, object-store keys) arrive only as env vars named by the config;
# nothing here writes them anywhere. The final `exec` makes the JVM PID 1, so SIGTERM reaches it.
set -eu

role="${PHAROS_ROLE:-all}"
home="${PHAROS_HOME:-/data/pharos}"
config="${PHAROS_CONFIG:-/etc/pharos/config.json}"

mkdir -p "$home"

# A cluster role must never fall back to pharos's default single node: no config is an error.
if [ ! -f "$config" ]; then
    case "$role" in
        writer|reader|router)
            echo "pharos-entrypoint: PHAROS_ROLE=$role needs a config at $config (mount the ConfigMap)" >&2
            exit 78 ;;
    esac
fi

# pharos reads $PHAROS_HOME/config.json and has no option for another path; link the mounted
# file in rather than copying, so a ConfigMap update is seen on the next start.
if [ -f "$config" ]; then
    ln -sf "$config" "$home/config.json"
else
    echo "pharos-entrypoint: no config at $config; starting with defaults" >&2
fi

jar=/opt/pharos/pharos.jar
command_args=$#
# The flags ./pharos always passes, plus container-aware heap sizing; a command's own arguments
# stay after them.
set -- -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError \
    --enable-native-access=ALL-UNNAMED \
    --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED \
    --add-modules jdk.incubator.vector \
    -Dpolyglot.engine.WarnInterpreterOnly=false \
    -Dpharos.home="$home" -Duser.home="$home" ${JAVA_OPTS:-} -jar "$jar" "$@"

if [ "$command_args" -gt 0 ]; then
    # pharos loads (and validates) the config itself before running the command
    exec java "$@"
fi

case "$role" in
    writer|reader|router)
        # Same validation a node runs at startup; fail before serving anything.
        java "$@" config validate "$config" || exit $? ;;
esac

case "$role" in
    writer|reader|all)
        exec java "$@" web --no-open --port "${PHAROS_PORT:-7070}" ;;
    router)
        exec java "$@" router --port "${PHAROS_PORT:-7080}" ;;
    *)
        echo "pharos-entrypoint: PHAROS_ROLE must be writer, reader, router or all (got '$role')" >&2
        exit 64 ;;
esac
