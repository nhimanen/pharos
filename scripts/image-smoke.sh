#!/usr/bin/env bash
# Smoke test of the pharos container image: build it, index a tiny project, run a single node
# (role all), wait for /health 200 and search once. Exit 0 = pass.
# Needs a reachable Docker daemon; without one it fails at once (exit 2).
#   scripts/image-smoke.sh [image-tag]     (default: pharos:smoke)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${1:-pharos:smoke}"
suffix="$$"
cname="pharos-smoke-$suffix"
vol="pharos-smoke-data-$suffix"
tmp="$(mktemp -d)"

if ! command -v docker >/dev/null 2>&1; then
    echo "image-smoke: docker is not installed" >&2
    exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "image-smoke: no Docker daemon reachable (smoke not run); run this on a host with one" >&2
    exit 2
fi

cleanup() {
    docker rm -f "$cname" >/dev/null 2>&1 || true
    docker volume rm "$vol" >/dev/null 2>&1 || true
    rm -r "$tmp"
}
trap cleanup EXIT

# A tiny project and a config without embedding providers: no model download, keyword search only.
mkdir "$tmp/project" "$tmp/etc"
cat > "$tmp/project/Greeter.java" <<'JAVA'
package demo;

public class Greeter {
    /** Builds a friendly greeting for the named visitor. */
    public String greetVisitor(String name) {
        return "Hello, " + name;
    }
}
JAVA
echo '{"crossEncoderEnabled": false}' > "$tmp/etc/config.json"
chmod -R a+rX "$tmp"

echo "image-smoke: building $image"
docker build -f "$root/docker/pharos/Dockerfile" -t "$image" "$root"

# writers clone ssh sources: git needs an ssh client in the image
if ! docker run --rm --entrypoint sh "$image" -c 'command -v ssh && command -v git' >/dev/null; then
    echo "image-smoke: the image has no ssh client (or git); ssh git sources cannot be cloned" >&2
    exit 1
fi

docker volume create "$vol" >/dev/null
# A fresh named volume takes the ownership of /data from the image (uid 10001).
echo "image-smoke: indexing"
docker run --rm -v "$vol:/data" -v "$tmp/project:/project:ro" -v "$tmp/etc:/etc/pharos:ro" \
    --entrypoint sh "$image" -c \
    'mkdir -p /data/pharos && ln -sf /etc/pharos/config.json /data/pharos/config.json
     exec java --enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED --add-modules jdk.incubator.vector -Dpolyglot.engine.WarnInterpreterOnly=false -Dpharos.home=/data/pharos -jar /opt/pharos/pharos.jar index /project --project demo --no-embed --no-progress'

echo "image-smoke: starting the node"
docker run -d --name "$cname" -p 127.0.0.1::7070 -e PHAROS_ROLE=all \
    -v "$vol:/data" -v "$tmp/etc:/etc/pharos:ro" "$image" >/dev/null
port="$(docker port "$cname" 7070/tcp | head -n1 | sed 's/.*://')"

ok=""
for _ in $(seq 1 90); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/health" || true)" = 200 ]; then
        ok=1
        break
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$cname")" != true ]; then
        break
    fi
    sleep 2
done
if [ -z "$ok" ]; then
    echo "image-smoke: /health never answered 200; container log:" >&2
    docker logs "$cname" >&2 || true
    exit 1
fi

result="$(curl -fsS -G "http://127.0.0.1:$port/api/search" \
    --data-urlencode 'q=greetVisitor' --data-urlencode 'type=keyword' --data-urlencode 'project=demo')"
if ! printf '%s' "$result" | grep -q 'greetVisitor'; then
    echo "image-smoke: search returned no greetVisitor hit: $result" >&2
    docker logs "$cname" >&2 || true
    exit 1
fi

# SIGTERM must reach the JVM (exec): docker stop should exit quickly with 143, not be killed.
docker stop -t 20 "$cname" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$cname")"
if [ "$code" = 137 ]; then
    echo "image-smoke: container had to be SIGKILLed; SIGTERM does not reach the JVM" >&2
    exit 1
fi
echo "image-smoke: PASS"
