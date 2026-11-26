# Container image

`ghcr.io/nhimanen/pharos:<version>` (published for every release from 1.11.1 on) is the image the
[Kubernetes operator](operator.md) deploys. It holds a Temurin 25 JRE, the pharos jar, `python3` with
`python-hcl2` (Python and Terraform indexing), `node` (JavaScript/TypeScript indexing), and `git` with
`openssh-client` (writers clone sources). It runs as uid/gid 10001.

To build it yourself, from the repository root:

```bash
docker build -f docker/pharos/Dockerfile -t pharos:dev .
```

## Entrypoint

| Setting | Default | Meaning |
|---|---|---|
| `PHAROS_ROLE` | `all` | `writer`, `reader` or `all` run `pharos web --no-open`; `router` runs `pharos router`. The node's cluster role itself comes from `cluster.role` in the config; `PHAROS_ROLE` picks the process and the arithmetic default |
| `PHAROS_CONFIG` | `/etc/pharos/config.json` | the config file (a ConfigMap mount on Kubernetes). It is linked to `$PHAROS_HOME/config.json`, so an updated mount is read on the next start |
| `PHAROS_HOME` | `/data/pharos` | pharos state: indexes, registry, caches. Also the JVM's `user.home`, see below |
| `PHAROS_PORT` | 7070 (`web`), 7080 (`router`) | the HTTP port |
| `PHAROS_ARITHMETIC` | `deterministic` for `PHAROS_ROLE=reader`, `native` otherwise | Lucene's vector arithmetic, see [CPU-deterministic readers](cluster.md#cpu-deterministic-readers) |
| `JAVA_OPTS` | — | extra JVM options, placed after the arithmetic flags so they win |
| arguments | — | one pharos command to run instead of the server, e.g. `index --project lucene --source-git <url> --ref main`; the container exits with the command's code |

The JVM gets `-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`, so size the heap with the
container's memory limit.

- With `PHAROS_ROLE` `writer`, `reader` or `router`, a missing config makes the container exit 78, and an
  invalid one fails `pharos config validate` before anything is served. With `all` and no config, pharos
  starts as a single node with defaults.
- Secrets reach the process only as environment variables named by the config (`cluster.tokenEnv`,
  `cluster.objectStore.accessKeyEnv` / `secretKeyEnv`, `cluster.tls.passwordEnv`); the entrypoint never
  writes them anywhere.
- The entrypoint ends in `exec java`, so SIGTERM reaches the JVM and the
  [graceful stop](cluster.md#stopping-and-draining) runs. Give the container a stop timeout above
  `drainSeconds` + 25 s (e.g. `docker stop -t 50`).
- There is no `HEALTHCHECK`: probe `/health` instead.

## Volumes and the model cache

`/data` is a volume. It holds `PHAROS_HOME` (`/data/pharos`) and, in the operator's configs, a writer's
work dir (`/data/work`). Mount a persistent volume on `/data`; on Kubernetes set `securityContext`
`runAsUser: 10001` and `fsGroup: 10001` so the volume is writable (the operator does).

uid 10001 has no home directory, so the entrypoint sets the JVM's `user.home` to `PHAROS_HOME`: the
embedding and cross-encoder models DJL downloads land in `/data/pharos/.djl.ai` on the volume and are
downloaded once, not on every start.

## Examples

A single node, serving on port 7070 with its state in a named volume:

```bash
docker run -d --name pharos -p 7070:7070 -v pharos-data:/data ghcr.io/nhimanen/pharos:1.12.0
docker exec pharos pharos-entrypoint index /data/src/myproject --project myproject
```

(Put or mount the source under the container first, e.g. `-v $PWD:/data/src/myproject:ro`.)

A cluster reader, with its config mounted and its secrets from the environment:

```bash
docker run -d --name pharos-reader -p 7070:7070 \
  -e PHAROS_ROLE=reader \
  -e PHAROS_CLUSTER_TOKEN -e PHAROS_S3_ACCESS_KEY -e PHAROS_S3_SECRET_KEY \
  -v $PWD/reader.json:/etc/pharos/config.json:ro \
  -v pharos-reader:/data \
  ghcr.io/nhimanen/pharos:1.12.0
```

Here `reader.json` is a reader config from [docs/cluster.md](cluster.md#object-store-reader) with
`"advertiseUrl"` set to an address the other readers can reach. Leave `cacheDir` out (its default,
`$PHAROS_HOME/cluster-cache`, is on the volume) or point it at another writable directory.

One command, then exit (as the operator's registration Jobs do):

```bash
docker run --rm -e PHAROS_ROLE=writer -e PHAROS_CLUSTER_TOKEN -e PHAROS_S3_ACCESS_KEY -e PHAROS_S3_SECRET_KEY \
  -v $PWD/writer.json:/etc/pharos/config.json:ro \
  ghcr.io/nhimanen/pharos:1.12.0 index --project lucene --source-git https://github.com/apache/lucene.git --ref main
```

`scripts/image-smoke.sh` builds the image, runs one node and searches once; it needs a Docker daemon.
