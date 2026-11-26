# Multi-node pharos

Without a `cluster` section in `~/.pharos/config.json`, pharos is a single node: it indexes and
serves everything itself (`role: all`, `backend: local`), and nothing on this page applies.

A multi-node cluster splits that work:

- **Writers** index projects and publish each index run as an immutable *generation*.
- **Readers** download generations and answer searches and call-graph queries.
- **`pharos router`** (optional) spreads requests over the healthy readers.

Writers and readers exchange indexes through a **backend**:

| `cluster.backend` | What carries the indexes | Notes |
|---|---|---|
| `local` (default) | nothing: single node | only with `role: all` |
| `object-store` | an S3-compatible bucket (AWS S3, MinIO, …) | the full feature set: work-queue writers with failover, sharding, resharding |
| `peer-pull` (experimental) | the writers themselves, over HTTP | no bucket; no writer failover, no sharding. See [Peer pull](#peer-pull-without-a-bucket-experimental) |

To run a cluster on Kubernetes, use the [operator](operator.md); it writes these configs for you. For the
container image, see [docker.md](docker.md).

Contents:

- [Roles](#roles)
- [Minimal configs](#minimal-configs)
- [Configuration reference](#configuration-reference)
- [Publishing and syncing](#publishing-and-syncing)
- [Readers: lazy or full](#readers-lazy-or-full)
- [Writers: the work queue](#writers-the-work-queue)
- [Fan-out and the router](#fan-out-and-the-router)
- [Health and cluster status](#health-and-cluster-status)
- [Scaling proposals](#scaling-proposals)
- [Sharding and live resharding](#sharding-and-live-resharding)
- [Index formats and upgrades](#index-formats-and-upgrades)
- [Stopping and draining](#stopping-and-draining)
- [CPU-deterministic readers](#cpu-deterministic-readers)
- [Peer pull (without a bucket, experimental)](#peer-pull-without-a-bucket-experimental)
- [Security](#security)
- [Limitations](#limitations)

## Roles

| `cluster.role` | Runs | Serves search |
|---|---|---|
| `all` (default) | the single-node tool | yes |
| `writer` | `pharos web` takes shards from the catalog, indexes and publishes them (the [work queue](#writers-the-work-queue), the default), or you run `pharos index` / `pharos publish` by hand | no: a writer's `pharos web` does not load the search models |
| `reader` | `pharos web` syncs generations, serves `/api/*`, and answers other readers on `/internal/*` | yes |

The router is not a role: `pharos router` runs with the config of any writer or reader of the cluster
(it ignores `cluster.role`) and needs no index.

Readers refuse the commands that change indexes: `index`, `link`, `embed`, `mine`, `remove-index`,
`backfill-embedding-cache` and `upgrade-indexes` (also `./setup.sh --upgrade-indexes`). They receive
upgraded indexes by syncing. `index-status --json` reports the node's `role`.

## Minimal configs

Each example is a complete `~/.pharos/config.json` that passes `pharos config validate`. Secrets
never go into the file: `tokenEnv`, `accessKeyEnv` and `secretKeyEnv` name environment variables.

### Object store: work-queue writer

```json
{"cluster": {
  "role": "writer",
  "backend": "object-store",
  "nodeId": "writer-1",
  "tokenEnv": "PHAROS_CLUSTER_TOKEN",
  "objectStore": {
    "endpoint": "http://minio.internal:9000",
    "bucket": "pharos",
    "accessKeyEnv": "PHAROS_S3_ACCESS_KEY",
    "secretKeyEnv": "PHAROS_S3_SECRET_KEY"
  }}}
```

Start it with `pharos web` (long-running). It indexes the projects registered in the catalog (see
[Sources](#sources)). Add more writers with the same config and another `nodeId`; they share the shards.

### Object store: hand-run writer

Add `"writer": {"workQueue": false}` to index and publish by hand with `pharos index` and
`pharos publish`:

```json
{"cluster": {
  "role": "writer",
  "backend": "object-store",
  "nodeId": "writer-1",
  "writer": {"workQueue": false},
  "objectStore": {
    "endpoint": "http://minio.internal:9000",
    "bucket": "pharos",
    "accessKeyEnv": "PHAROS_S3_ACCESS_KEY",
    "secretKeyEnv": "PHAROS_S3_SECRET_KEY"
  }}}
```

### Object store: reader

A reader needs `advertiseUrl` (the base URL other readers reach it at) and the cluster token:

```json
{"cluster": {
  "role": "reader",
  "backend": "object-store",
  "nodeId": "reader-1",
  "advertiseUrl": "http://reader-1.internal:7171",
  "tokenEnv": "PHAROS_CLUSTER_TOKEN",
  "cacheDir": "/var/cache/pharos",
  "objectStore": {
    "endpoint": "http://minio.internal:9000",
    "bucket": "pharos",
    "accessKeyEnv": "PHAROS_S3_ACCESS_KEY",
    "secretKeyEnv": "PHAROS_S3_SECRET_KEY"
  }}}
```

Start it with `pharos web`. For AWS S3, set the regional endpoint, `"region"` and `"pathStyle": false`
(virtual-hosted URLs); MinIO needs path-style URLs, the default. Without `accessKeyEnv` and
`secretKeyEnv`, the AWS default credential chain is used.

### Router

```bash
pharos router --port 7080          # with a reader's or writer's config
```

### Peer pull

See [Peer pull](#peer-pull-without-a-bucket-experimental) for the writer and reader configs.

### Checking a config

```bash
pharos config validate /etc/pharos/reader.json
```

It runs the validation a node runs at startup on that one file (`tokenEnv` is read from the current
environment), prints `ok` and exits 0, or prints the problem and exits 2. It works even when the
node's own `config.json` is broken, and its messages never echo a value from the file. Run it before
deploying any change.

## Configuration reference

### `cluster`

| Key | Default | Meaning |
|---|---|---|
| `role` | `all` | `all`, `writer` or `reader`. `writer` and `reader` need `backend` `object-store` or `peer-pull`; `all` needs `local` |
| `backend` | `local` | `local`, `object-store` or `peer-pull` |
| `nodeId` | host name | must not be blank |
| `token` / `tokenEnv` | — | the shared cluster secret, or the name of the environment variable holding it (set one, not both). Required on readers, on `pharos router`, and on every node in peer mode. With `tokenEnv`, surrounding whitespace (such as a Secret file's trailing newline) is stripped; an unset or empty variable, or a value with whitespace inside, fails at startup |
| `advertiseUrl` | — | required on readers: an `http://` or `https://` URL with a host |
| `pollIntervalSeconds` | 10 | how often a reader checks for new generations |
| `cacheDir` | `~/.pharos/cluster-cache` | where a reader keeps generations and its block cache |
| `retentionGenerations` | 3 | generations the writer keeps per shard (at least 2) |
| `retentionMinutes` | 60 | the writer also keeps every generation younger than this (not negative) |
| `heartbeatSeconds` | 10 | heartbeat interval of readers and writers; on a work-queue writer it must be below `writer.leaseTtlSeconds` |
| `readerTtlSeconds` | 30 | a reader whose heartbeat is older is dropped (must exceed `heartbeatSeconds`) |
| `retireGraceSeconds` | 120 | how long a reader keeps a shard directory after it stops serving it; at least `2 × pollIntervalSeconds` and at least `fanoutTimeoutMillis` |
| `fanoutTimeoutMillis` | 5000 | timeout of one shard call |
| `connectTimeoutMillis` | 1000 | connect timeout of one shard call |
| `queryTimeoutMillis` | 0 | budget of one fanned-out query, all rounds; 0 means 20000 (or `fanoutTimeoutMillis` if larger). At least `fanoutTimeoutMillis`, below 60000 |
| `maxCatalogAgeSeconds` | 600 | see [Fan-out](#fan-out-and-the-router) |
| `drainSeconds` | 20 | upper bound of a graceful stop, 0–3600; see [Stopping and draining](#stopping-and-draining) |

The sections `objectStore`, `reader`, `writer`, `status`, `sharding`, `peers` and `tls` are described
below.

### `cluster.objectStore`

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | — | the S3 endpoint URL |
| `region` | `us-east-1` | |
| `bucket` | — | required with `backend: object-store` |
| `prefix` | `pharos` | key prefix inside the bucket; must not start with `/` or contain `.` / `..` segments |
| `pathStyle` | `true` | path-style URLs (MinIO); `false` for AWS virtual-hosted URLs |
| `accessKeyEnv`, `secretKeyEnv` | — | names of the environment variables holding the keys (set both or neither; a named variable that is unset or empty is an error). Neither: the AWS default credential chain |
| `readTimeoutSeconds` | 10 | 1–3600; see [S3 timeouts](#s3-timeouts) |
| `callAttemptTimeoutSeconds` | 20 | 1–3600 |
| `callTimeoutSeconds` | 60 | from `callAttemptTimeoutSeconds` to 3600 |
| `uploadMinBytesPerSecond` | 1048576 | at least 1024 |

#### S3 timeouts

Every call to the bucket is bounded, so a slow or hung bucket cannot stall a node for minutes:

- **Ranged reads** (a lazy reader's blocks): `readTimeoutSeconds` per attempt, three times that for the
  whole read with retries (30 s by default). It also bounds the connect. A query whose block cannot be
  fetched in time fails with an error naming the shard and object key. The wait includes time queued
  behind other fetches (`reader.maxConcurrentFetches` at once), so under a burst of cold misses a block
  can time out although the bucket answers each fetch quickly: raise `maxConcurrentFetches` or
  `readTimeoutSeconds` if timeouts appear only under load.
- **Other calls** (heartbeats, leases, catalog, manifest and `LATEST` reads, listings, deletes, a
  download until its headers arrive): `callAttemptTimeoutSeconds` per attempt and `callTimeoutSeconds`
  in total. A healthy bucket answers them in well under a second; raise them only if `timed out` WARNs
  appear while the bucket is merely slow. Between reads of a response body the client waits up to
  `readTimeoutSeconds` or 30 s, whichever is longer.
- **Uploads** (one PutObject per file; segments can be gigabytes) get the call bounds plus the file's
  size at `uploadMinBytesPerSecond` per attempt, and three times that in total: a 5 GiB file may take
  20 s + 85 min per attempt. Lower the rate if uploads to a slow bucket time out.

## Publishing and syncing

**Writers publish.** After each index run the writer uploads the project as a new generation (Lucene
commit files, a compact call-graph file, the unresolved references and a manifest carrying the index
format), reusing files that did not change, and moves the shard's `LATEST` pointer with a conditional
write. After a publish it garbage-collects generations beyond `retentionGenerations` that are also
older than `retentionMinutes`.

On a hand-run writer, if publishing fails at the end of an index run (bucket unreachable, bad
credentials), the local index is kept and `pharos index` exits with code **3**. Retry the upload
without re-indexing:

```bash
pharos publish myproject      # prints "published myproject generation N"
```

`pharos publish` exit codes: 0 published, 1 project not indexed, 2 not a writer node, 3 publish failed,
4 the local index is not publishable (a broken `callgraph.current` pointer, or an index format other
than this release's, including `upgrade-available`: re-index with `pharos index --full` first). A
writer also refuses to publish a [damaged call graph](../README.md#damaged-call-graphs) until a full
run has rebuilt it, so readers never receive one.

**Readers sync.** A reader starts syncing when its daemon (`pharos web` or `pharos mcp-server`) starts.
It polls each shard's `LATEST` every `pollIntervalSeconds`, materialises new generations in `cacheDir`
(checksummed end to end) and swaps them in. It serves class bodies and snippets from the index, never
from source files on the reader machine.

- Only one process per `cacheDir` syncs (it holds `<cacheDir>/.sync.lock`). Another daemon on the same
  `cacheDir` (for example `mcp-server` next to `web`) serves what the lock holder downloads and takes
  over syncing when that process exits. One-shot commands serve the generations already on disk.
- Only `pharos web` joins reader membership (heartbeat, `/internal/*`, the shutdown drain).
  `pharos mcp-server` on a reader answers from the shards this reader holds only, and does not
  coordinate fan-out.
- `pharos web` on a reader binds its port at once and loads its search models while the first sync
  runs. Until both are done `/health` answers 503 `warming` (the log line `Reader … serving N shard(s)`
  appears only then), and searches answered meanwhile carry a notice in the `X-Pharos-Warnings` header
  (`./pharos` prints it on stderr). A model that fails to load, or is still loading after 5 minutes, is
  logged at ERROR and does not hold readiness; vector and hybrid searches then carry a
  `vector search unavailable` warning.

**Pinning shards.** `cluster.reader.shards` limits a reader to some shards: shard ids (`lucene`,
`lucene~ring=s01`) or a project wildcard (`lucene~*`, every shard of the project). Empty (the default)
means every shard.

**Previous generations.** After a reader activates a shard's new generation, it keeps the previous one
open and answerable for `cluster.reader.previousGenerationGraceSeconds` (default
`max(30, 15 + 2 × heartbeatSeconds)` seconds; 0 turns it off), so a query planned against the previous
generation (a query already running, a coordinator whose catalog predates a reshard) is still answered.
The writer's GC keeps a superseded generation that long too. Meanwhile a reader holds up to two
generations of a changed shard (disk, open files, memory).

## Readers: lazy or full

`cluster.reader.mode`:

- **`lazy`** (default): only the manifest, the graph and the small Lucene files (`segments_N`, `.si`,
  `.fnm`, `.cfe`, any file up to `eagerMaxBytes`, and a `.cfs` of at most one block) are downloaded.
  Every other file is read in blocks of `blockSizeBytes` through a local block cache in
  `<cacheDir>/.blocks`, capped at `cacheMaxBytes` (least recently used blocks are evicted). Before a new
  generation is swapped in, the blocks of the `prefetchExtensions` files (term dictionaries, vector
  graphs) are prefetched, for at most `prefetchTimeoutSeconds`.
- **`full`**: every file is copied. Use it for readers that may lag behind the writer's retention (see
  below).

| `cluster.reader` key | Default | Meaning |
|---|---|---|
| `mode` | `lazy` | `lazy` or `full` |
| `cacheMaxBytes` | 8589934592 (8 GiB) | block cache cap; at least 4 blocks |
| `blockSizeBytes` | 8388608 (8 MiB) | a power of two from 64 KiB to 64 MiB |
| `eagerMaxBytes` | 1048576 (1 MiB) | files up to this size are downloaded whole |
| `prefetchExtensions` | `["tip", "tmd", "tim", "vex", "vem"]` | prefetched before a swap |
| `prefetchTimeoutSeconds` | 30 | |
| `maxConcurrentFetches` | 16 | at least 1 |
| `privateCacheMaxBytes` | 0 | cap of a private cache; 0 means `min(cacheMaxBytes, max(4 blocks, cacheMaxBytes / 8))` (1 GiB with the defaults) |
| `shards` | `[]` | shard pinning, see above |
| `previousGenerationGraceSeconds` | derived | see above |

**One block cache per process.** The syncing process takes `<cacheDir>/.blocks`. Any other pharos
process on the same `cacheDir` gets a private cache in `<cacheDir>/.blocks-private/` (up to
`privateCacheMaxBytes`), deleted when it exits. Private caches are cleaned up by file lock, not by PID,
so this also works for containers sharing a `cacheDir` volume.

**Disk for a lazy reader's `cacheDir`:** the eager files of two generations per shard, plus each
generation's compact graph, plus `cacheMaxBytes`, plus (`maxConcurrentFetches` + prefetch threads) ×
`blockSizeBytes` for fetches in flight (prefetch threads = `min(maxConcurrentFetches, max(2,
maxConcurrentFetches / 4))`, 4 by default), plus `privateCacheMaxBytes` per extra process. With the
defaults and no extra process: eager files + graphs + 8 GiB + 20 × 8 MiB. Plan for one whole generation
more if a reader may be held back by a format refusal.

**Retention and lagging lazy readers.** A lazy reader reads its served generation from the bucket on
every cache miss, so `retentionMinutes` must exceed the longest time a reader may lag behind the newest
generation (a rolling upgrade, failing syncs, downtime). When the format check refuses a newer
generation, the reader downloads the rest of its served generation in the background, so it becomes
self-sufficient before GC deletes it. If a miss still finds an object gone, the reader logs one ERROR
per generation, and `/health` answers 200 `degraded` with `degradedShards`: the node stays routable for
its other shards, and coordinators route that shard to other readers. It answers `ok` again once a
newer generation is served.

Switching a reader from `lazy` to `full` leaves `<cacheDir>/.blocks` behind; the full reader deletes it
when it starts syncing, unless another process still uses it.

**Cache statistics.** `pharos index-status` prints `reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%,
41 evictions`, taken from the running daemon when there is one. `index-status --json` and `/health`
carry a `readerCache` object (`mode`, `cacheMaxBytes`, `bytesOnDisk`, `hits`, `misses`, `hitRate`,
`bytesFetched`, `evictions`, `leasesOpen`, `shared`, `source`). A hit rate that stays low while
`evictions` climb means `cacheMaxBytes` is smaller than the working set.

## Writers: the work queue

A `role: writer` node runs the **work queue** by default (`cluster.writer.workQueue: true`):
`pharos web` takes shards from the catalog under leases, polls each shard's source, indexes changes and
publishes them. Any number of writers share the shards, and a dead writer's shards are taken over.

A work-queue writer refuses hand runs: `index`, `publish`, `remove-index` and `embed` exit 2, and
`upgrade-indexes` exits 5. Set `"writer": {"workQueue": false}` for a hand-run writer instead. In peer
mode the work queue is always off.

| `cluster.writer` key | Default | Meaning |
|---|---|---|
| `workQueue` | `true` | `false`: hand-run writer |
| `workDir` | `~/.pharos/work` | source checkouts, `<workDir>/<shard>`. Give every writer the same path, or each takeover re-parses the shard in full |
| `pollSeconds` | 60 | how often sources are checked for changes |
| `leaseTtlSeconds` | 30 | at least 5; renewed every TTL / 3 |
| `minHoldSeconds` | 120 | a taken shard is not released for balance within this time |
| `maxConsecutiveFailures` | 3 | a surplus shard is released after this many failed runs in a row |
| `lsRemoteTimeoutSeconds` | 30 | `git ls-remote` is killed after this |
| `reshardSyncTimeoutSeconds` | 600 | see [Resharding](#sharding-and-live-resharding) |
| `gitCredentials` | `{}` | project name → absolute directory with git credentials |
| `disk` | | the [disk guard](#disk-guard) |

### Sources

A shard is workable once its catalog entry names a source. Register one without indexing anything:

```bash
pharos index --project lucene --source-git https://github.com/apache/lucene.git --ref main
pharos index --project docs --source-path /shared/docs        # a directory every writer can read
```

This writes the catalog only, so it runs with any writer config (a work-queue writer's too), with or
without writers running. Running it again with another URL or ref replaces the source. `--no-embed` is
recorded with the source (`"embed": false`), and the writers then index that project without
embeddings. On a hand-run writer you can also index and register in one step:
`pharos index <dir> --single --project <name> --source-git <url> --ref <branch>` (`pharos publish`
takes the same flags). A shard without a source stays with the writer that has its path.

Remove a project from the catalog in one write (every shard of a split project, its staged and retired
versions and any open reshard with it):

```bash
pharos remove-index --from-catalog lucene
```

Writers release it and readers stop serving it. Its generations and writer state stay in the bucket,
and registering it again resumes from them.

### Git credentials

Writers run git with their own environment (credential helper, ssh agent, `GIT_ASKPASS`). For
credentials per project, `cluster.writer.gitCredentials` maps a project to a directory:

```json
"writer": {"gitCredentials": {"mypal": "/etc/pharos/git/mypal"}}
```

The directory holds the keys of a Kubernetes `ssh-auth` or `basic-auth` Secret: `ssh-privatekey` (and
optionally `known_hosts`; without it a host key is accepted on first contact and pinned in
`<workDir>/.ssh-known-hosts`), or `username` and `password` (a token works as the password). Only that
project's clone, fetch and `ls-remote` use them; the config holds paths only. A source URL must not
carry credentials.

### Change detection and indexing

Every `pollSeconds` (at a fixed rate, also while another shard is being indexed) a writer checks the
sources of its shards, read-only: `git ls-remote` of the URL, or a digest of a path source's tree. An
unreachable remote stalls only its own shard. Pending shards are indexed one at a time, oldest change
first; embeddings come from the shard's embedding cache, so unchanged code is not embedded again.

### Leases and failover

One writer holds a shard's lease (`shards/<shard>/lease`, expiry judged by the bucket's clock). When a
writer dies, its leases expire after `leaseTtlSeconds` and another writer takes them on its next poll,
restores the shard's writer state (embedding cache, file state) from the bucket, and resumes from the
last published generation. Writers share the shards (`ceil(shards / writers)` each) and release a
surplus shard only after its publish or while it has nothing unpublished, never within
`minHoldSeconds` of taking it.

A writer whose lease was taken by another writer aborts that shard's run at once. A renewal that merely
failed (an I/O error or a timeout) lets the run continue: the writer keeps re-reading the lease until
the bucket answers, reclaims it if untouched, and aborts if another writer took it.

Only one process per writer node runs the work loop (`<workDir>/.work-loop.lock`). A second daemon with
the same config (for example one `./pharos` starts on demand) logs `another process is the writer for
node <id> …` and serves everything else.

### Disk guard

`cluster.writer.disk`: a writer whose free space is below `max(minFreeBytes, minFreePercent % of the
disk)` takes no new shard, refuses or aborts (at a safe point, never half committed) a run that would
cross it, hands its pending shards to other writers, and reports `low` in its heartbeat
(`/cluster/status` health `writer-low-disk`, a `writer-disk` proposal) and in a WARN once per low
episode. It is back once free space exceeds the threshold plus `hysteresisBytes`.

| Key | Default |
|---|---|
| `minFreeBytes` | 5368709120 (5 GiB) |
| `minFreePercent` | 0 |
| `hysteresisBytes` | 1073741824 (1 GiB) |
| `checkSeconds` | 15 |

### Synonyms

Each publish merges the writer's mined rules for that shard into the cluster's `catalog/synonyms.txt`,
replacing only that shard's section. Hand-written rules come from the publishing writer's
`synonyms.txt`: keep them the same on every writer. Readers fetch a new `synonyms.txt` on their next
poll.

### Upgrading work-queue writers

Restart them on the new release. A shard gets a full run (and a re-publish) when its local index needs
one (another index format, a broken or damaged graph), its files were embedded under another chunking
version or model, or its published generation has an older index format. Do not run
`upgrade-indexes` there (it is refused with exit 5).

## Fan-out and the router

Any reader takes any query. A reader holding every shard a query needs searches locally, exactly as a
single node. Otherwise it **coordinates**: it classifies and embeds the query once, sums BM25 term
statistics over all the query's shards, and merges candidates from the readers serving the other
shards, so rankings equal a single node's over the same shards. Graph boost, fusion and reranking run
once over the merged lists. Snippets and graph tools (callers, callees, usages, call chains, paths,
method and class lookup) also reach shards held elsewhere.

- Each query pins one reader and generation per shard (a dead reader is retried once on another replica)
  and has one budget, `queryTimeoutMillis`.
- A shard no reader answers for is left out with the warning
  `shard <name> unavailable: <reason>; results exclude it`. When no shard answers and the reader holds
  none of them, the web API answers 503 (`no shard answered`) and MCP a retryable tool error, so the
  router retries on another reader.
- A shard without vectors of the coordinator's search model is left out of the vector part, as on a
  single node.
- A query that loses a shard because a reader just switched generations (a publish, a reshard flip) is
  planned again once, within its original timeout (the trace notes `re-planned once`).
- Coordinator and shards must serve the same `synonyms.txt`; right after a change, a reader still on the
  previous synonyms is answered with them for the grace period.
- The shard catalog is re-read every 5 s. Only while reads fail does its age matter: past 60 s an
  unscoped query carries `routing with a catalog N s old`, and past `maxCatalogAgeSeconds`, or before a
  first good read, unscoped queries answer from the reader's own shards with a warning naming the shards
  not searched.
- Known approximations: ties between shards break by shard order, and vector and unified retrieval over
  several shards approximate a single index (each shard's HNSW search is its own).

### `pharos router`

```bash
pharos router --port 7080
```

A stateless HTTP front for the readers. It lists the live readers (from the bucket, or from the writers
in peer mode), polls each one's `/health` every 2 s, and round-robins over those that answered 200:

- It forwards `/api/*`, `/health`, `GET /cluster/status` and `GET /cluster/events` (streamed). Every
  other path, `/internal/*` included, answers 404.
- An idempotent request (any `GET`, and `POST /api/search`) is retried once on another reader after a
  connect error or a 5xx. With no healthy reader it answers 503.
- It answers 502 for a reader answer over 64 MiB and 504 when an exchange takes over 120 s.
- It adds `X-Forwarded-For` and never forwards the cluster token.

It needs the cluster config (`object-store` with `objectStore`, or `peer-pull` with
`peers.discovery`) and the cluster token, but no index. Any load balancer that routes on `/health` works
too.

## Health and cluster status

### `/health`

| Answer | Meaning |
|---|---|
| 200 `{"status": "ok"}` | serving |
| 200 `{"status": "degraded", "degradedShards": [...]}` | a served lazy generation lost objects on the bucket; other shards still served |
| 503 `{"status": "warming", "ready": false}` | first sync or model load not done yet |
| 503 `{"status": "draining"}` | stopping; see [Stopping and draining](#stopping-and-draining) |

A readiness probe that checks only the HTTP status works as is.

### `/cluster/status`, `/cluster/events` and the webhook

`GET /cluster/status` on any node returns the cluster's layout (writers with their leases, backlog and
current job; readers with their shards, state, cache and load), shard and project state (including
reshards), health findings and the [scaling proposals](#scaling-proposals). Without the
`X-Pharos-Cluster-Token` header you get the public view, without node addresses; with it, the full one.

`GET /cluster/events` (send `Accept: text/event-stream`) streams changes as server-sent events
(proposal `appeared`, `changed` and `cleared`, `layout` changes and `reshard` progress), with a keep-alive
every `cluster.status.eventsKeepAliveSeconds` (15). At most 64 streams per node.

`cluster.status.webhookUrl` (optional, `http://` or `https://`) receives every event as a JSON POST.

`GET /cluster/readers` lists the live readers with their `advertiseUrl`; it requires the cluster token
(401 without it).

### The Cluster page

The web UI (`pharos web`, `./pharos web`) has a **Cluster** button next to *Module Graph*;
`http://<node>:<port>/#cluster` opens it directly. It is drawn from the public `/cluster/status`, so
every node shows the same cluster:

- **Layout:** writers, the bucket and readers, with the shards each writer leases and each reader
  serves, coloured by state (current, lagging, lease-expired, unpublished, staged, degraded, retired).
- **Indexing backlog:** shards with unpublished changes, oldest first, and each writer's job with a
  progress bar.
- **Scale:** live writer and reader counts and the proposals in force.
- **Resharding:** each project's reshard, its phase and progress.
- **Health**, **Nodes** (role, shards, docs, size, free disk, reader cache, hit rate, QPS, warm p95,
  heartbeat age) and **Sharding** (spec versions and one shard table per version).

The page follows `/cluster/events`; when the stream is unavailable it polls every 10 s and says so. On a
single node it shows one node holding every project.

## Scaling proposals

`/cluster/status`, `/cluster/events` and the webhook list the scaling proposals in force. Pharos applies
none of them; a person or an operator (such as the [Kubernetes operator](operator.md)) does. Each
proposal carries a signed `delta`: `+N` to start N readers or writers, `-N` to stop N (`reshard` and
`writer-disk` carry none).

| Proposal | Raised when | Keys under `cluster.status` (defaults) |
|---|---|---|
| `add-readers` | the highest warm p95 or the QPS per reader is above target for the window | `targets.warmP95Millis` (500), `targets.qpsPerReader` (50), `windows.addReadersSeconds` (300) |
| `remove-readers` | more than one reader, QPS per reader below `removeReadersLoad` of the target, warm p95 within target | `removeReadersLoad` (0.2, above 0 and below 1), `windows.removeReadersSeconds` (1800) |
| `add-writers` | at least `writers.backlogShards` shards have an unpublished change older than `writers.oldestPendingSeconds`, and there are fewer live writers than workable shards | `writers.backlogShards` (1), `writers.oldestPendingSeconds` (300), `windows.addWritersSeconds` (0) |
| `remove-writers` | more than one live writer, none indexing and nothing pending, for the window | `writers.idleSeconds` (unset: `windows.removeWritersSeconds`, 1800; setting both to different values is an error) |
| `reshard` | a shard is above half of the smallest reader cache cap | `windows.reshardSeconds` (0) |
| `writer-disk` | a writer reports low disk | `windows.writerDiskSeconds` (0) |

`targets.backlog` (600 s) sets when a pending shard is reported as `shard-lagging` in the health
findings. A proposal clears once its condition has been false for `windows.clearGraceSeconds` (60).

```json
"status": {"removeReadersLoad": 0.2,
           "writers": {"backlogShards": 1, "oldestPendingSeconds": 300, "idleSeconds": 1800},
           "webhookUrl": "http://hooks.internal/pharos"}
```

## Sharding and live resharding

Object-store clusters only. A project is split into shards by its **shard spec**. Every project starts
as `[project]`, one shard named after the project.

| Spec field | Splits by |
|---|---|
| `project` | the project (always the first key, even when not written) |
| `module` | the nearest directory with a build file, relative to the project root (`_` outside any module) |
| `language` | the file extension |
| `kind` | `code` or `doc` |
| `prefix(path, depth=D)` | the first D directories of the path (default 1) |
| `ring(path, shards=N[, vnodes=V])` | a consistent-hash ring over file paths (shards `s01`…`sNN`, at most 999; `vnodes` default 512). Growing it from N to N + 1 moves about 1/(N + 1) of the files. At most one ring, last |

Examples: `[project, module]`, `[project, ring(path, shards=8)]`, `[language]`. Shard ids look like
`lucene~module=core~ring=s03`.

`cluster.sharding.default` and `cluster.sharding.projects.<name>` in `config.json` are validated at
startup (a non-default spec needs `backend: object-store`) but **not applied yet**: only
`pharos reshard` changes a project's spec.

### `pharos reshard`

```bash
pharos reshard lucene --spec "[project, ring(path, shards=4)]"   # request a new spec version
pharos reshard lucene --status                                   # versions and progress
pharos reshard lucene --cancel                                   # abandon an open reshard
```

Exit codes: 0 done, 1 refused (another reshard of the project is open, unknown project, nothing to
cancel, or a reader of an older release is live), 2 bad usage or config, 3 the bucket failed.

How it runs:

1. The writer holding the project takes its reshard lease, writes the new version as *staged* and builds
   it from its local index. A ring change re-indexes only the files whose shard changes; embeddings come
   from the cache. A shard that neither gains nor loses files keeps its Lucene files (a metadata-only
   publish); a shard that does is rewritten whole and downloaded in full by its readers. The writer
   needs about twice the project's Lucene size free while it builds (the disk guard checks first).
2. Readers keep serving the current version and prepare the new one.
3. Once every live reader has it (or after `writer.reshardSyncTimeoutSeconds`, default 600), one
   catalog write flips routing. Each query reads one version, so a file that moved is never seen twice
   or missed.
4. The retired version is deleted after `retentionMinutes`. An interrupted reshard resumes on the next
   writer that holds the project.

Limits: an index run never adds shards, so a new module under `[project, module]` fails the project's
runs with "reshard the project to add it" until `pharos reshard <project> --spec "<the same spec>"`
adds it. Resharding back to `[project]` is not supported; `[project, ring(path, shards=1)]` serves a
project from one shard. While a reader of an older release is live, `pharos reshard` is refused and an
open reshard waits.

Progress shows in `/cluster/status` (`projects[].reshard`: phase `requested` → `building` → `pinned` →
`flipped`, or `abandoned`; shards and files done of total; the lease holder; `specs[]`), as `reshard`
events, and on the Cluster page. When a shard grows past half of a reader's cache, the `reshard`
proposal names the exact command to run; applying it stays manual.

### Search key filters

Documents carry the keys `module`, `pathPrefix`, `language` and `kind`, on single nodes too. Filter on
them with `pharos search "query" --module core --path-prefix src/main --language java --kind code` (web API
`/api/search` parameters `module`, `pathPrefix`, `language`, `kind`; MCP `search_code` `module`,
`path_prefix`, `language`, `kind`). Filters only select documents: scores are those of the unfiltered
query. On a cluster they also prune the shards of split projects (`--trace` shows the routing). A
filtered search over a format-1 project is refused with a message naming the fix.

## Index formats and upgrades

The current index format is **2** (since 1.11.0). Under the **N-1 rule** a release serves its own format
and the previous one, so a cluster keeps answering while it is upgraded:

- Every published manifest carries the shard's format. A reader only materialises generations it can
  serve (format 2 or 1 on 1.12.0); otherwise it keeps serving what it has and logs a WARN naming the
  shard, both formats and the fix.
- Check before upgrading: `pharos upgrade-indexes --check` with the new release against the bucket
  (any role). It changes nothing and exits 3 when the new release would refuse a published shard.
- Upgrade **readers first**, then writers, then the router. Work-queue writers re-index and re-publish
  every shard by themselves once restarted; on a hand-run writer run `pharos upgrade-indexes` (or
  `pharos index --full`).
- Readers refuse `upgrade-indexes` (exit 5): they receive upgraded indexes by syncing.

[UPGRADING.md](../UPGRADING.md) has the steps for each release.

## Stopping and draining

Every long-running pharos process stops within a bound on SIGTERM, also when the bucket hangs.

**Readers** drain: the reader rewrites its heartbeat as `draining` (coordinators stop sending it shard
calls), answers `/health` with 503 `draining` (the router drops it within one 2 s poll) and keeps
serving what still arrives. It stops once it has drained for at least 4 s and one `heartbeatSeconds`
plus 2 s, with nothing in flight for the last second, and at most after `drainSeconds` (default 20).
Then it removes its heartbeat. On a slow bucket (or, in peer mode, a slow writer) each of those calls
gives up after 5 s with a `Stopping: gave up …` WARN, and the heartbeat expires on its own.

**Writers** start no new run, let the running one finish for up to three quarters of `drainSeconds`,
then abort it at its next safe point, release their leases (another writer takes them at once) and
remove their heartbeat, all within `drainSeconds` + 10 s.

After the drain, running model calls get up to 15 s, then the process exits (143/130). The drain runs
before any JVM shutdown hook. A second signal exits at once.

**Stop timeouts** for your process manager (Kubernetes `terminationGracePeriodSeconds`): above
`drainSeconds` + 15 s for readers (e.g. 45 s for the default) and above `drainSeconds` + 25 s for
writers (e.g. 50 s), plus a margin for the exit. A shorter timeout sends SIGKILL mid-drain. The
operator sets `drainSeconds` + 30.

## CPU-deterministic readers

Readers on different CPU types score the same vector query with the same bits, so a fan-out orders
exact score ties (identical methods, copied files) the same way whichever replica answers.

Lucene chooses its vector arithmetic per CPU when it loads (fused multiply-add or not, and the SIMD width
of its Vector API code), so two readers can score the same document one ulp apart. A reader therefore
runs Lucene's **scalar** vector code with FMA off. Writers, routers and single nodes keep Lucene's faster
per-CPU code.

| `PHAROS_ARITHMETIC` | JVM flags | Default for |
|---|---|---|
| `deterministic` | `-Dlucene.useScalarFMA=false -Dlucene.useVectorFMA=false -Dpharos.arithmetic=deterministic`, no `--add-modules jdk.incubator.vector` | readers |
| `native` | `--add-modules jdk.incubator.vector -Dpharos.arithmetic=native` | writers, routers, `role: all` |

The flags must be on the `java` command line (Lucene reads them at class load), so every pharos launcher
sets them: `./pharos` and `setup.sh`'s daemon start (from `cluster.role` in `~/.pharos/config.json`),
the image entrypoint (from `PHAROS_ROLE`) and the Kubernetes operator. Set `PHAROS_ARITHMETIC` to
override the default. With your own launcher, pass the flags yourself.

A reader logs `Vector arithmetic: fma=off simd=scalar canary=… (deterministic)` at startup, or a WARN
naming the flags when it runs anything else, unless `PHAROS_ARITHMETIC=native` was set explicitly.
Readers advertise their profile in their heartbeat, and readers and routers WARN once when the live
readers' profiles differ. A deterministic JVM also logs Lucene's `Java vector incubator module is not
readable` warning; that is expected.

Cost, measured on an AMD Ryzen 9700X: about 0.1–0.2 ms more per KNN query over a 50k-vector shard
(768 dims: 291 µs mean against 102 µs), against a hybrid query's tens of milliseconds. Indexing would be
three to four times slower, which is why writers stay native.

Not covered: the query embedding and the cross-encoder reranker run in ONNX Runtime on the coordinator,
whose kernels also differ per CPU, so two coordinators on different CPUs can embed or rerank the same
query differently.

## Peer pull (without a bucket, experimental)

With `"backend": "peer-pull"` there is no bucket: each writer keeps its published generations in a local
**peer store**, and its `pharos web` serves them on `/replication/*` (every route requires the cluster
token). Readers find the writers through `cluster.peers.discovery`, long-poll each shard for new
generations, and fetch whole files from other readers that already hold them. Reader membership and the
cluster status live on the writers, so fan-out, `pharos router`, `/cluster/status` and the Cluster page
work as with a bucket.

**When to use it.** Small or on-premises clusters where running S3 or MinIO is not worth it. Prefer the
bucket when you have one:

- **No writer failover.** No leases; the work queue is forced off (`writer.workQueue: true` is a config
  error). Each writer owns the shards in its own registry and indexes and publishes them by hand. While a
  writer is down its shards keep serving what readers hold, but nothing publishes them, and a lazy
  reader's cold reads of them fail.
- **No sharding**: only the default spec `[project]`.
- The writer's disk is the only copy of what it published, and the cluster token is the only
  credential.

### Peer-pull configs

Writer (one per group of projects; repeat with another `nodeId` and other projects for more writers):

```json
{"indexDir": "/data/pharos/indexes",
 "cluster": {
  "role": "writer",
  "backend": "peer-pull",
  "nodeId": "writer-1",
  "tokenEnv": "PHAROS_CLUSTER_TOKEN",
  "peers": {
    "storeDir": "/data/pharos/peer-store",
    "discovery": "seeds:http://writer-1.internal:7171,http://writer-2.internal:7171",
    "allowHosts": ["writer-1.internal", "writer-2.internal", "reader-1.internal", "reader-2.internal"]
  }}}
```

Reader:

```json
{"cluster": {
  "role": "reader",
  "backend": "peer-pull",
  "nodeId": "reader-1",
  "tokenEnv": "PHAROS_CLUSTER_TOKEN",
  "advertiseUrl": "http://reader-1.internal:7171",
  "cacheDir": "/var/cache/pharos",
  "peers": {
    "discovery": "seeds:http://writer-1.internal:7171,http://writer-2.internal:7171",
    "allowHosts": ["writer-1.internal", "writer-2.internal", "reader-1.internal", "reader-2.internal"]
  }}}
```

- **Writers must run `pharos web`** (long-lived): it serves `/replication/*`, reader membership and the
  status. Index and publish next to it with `pharos index <dir>` and `pharos publish <project>`; waiting
  readers see a new generation within about a second. The peer store starts empty: publish every project
  once.
- **Readers** need `discovery`, the token and an `advertiseUrl` other nodes can reach. Run
  `pharos router` with a reader's config.

### `cluster.peers`

| Key | Default | Meaning |
|---|---|---|
| `discovery` | — | required on readers: `dns:<name>[:port]` or `seeds:<url>,<url>` |
| `storeDir` | `~/.pharos/peer-store` | a writer's published generations. Lucene files are hard-linked from the index dir, so **put it on the same file system as `indexDir`**; otherwise every file is copied and the writer WARNs at startup. The disk guard watches it too |
| `allowHosts` | the seeds' hosts | see below |
| `refreshSeconds` | 15 | how often readers re-resolve discovery and re-read catalogs |
| `longPollSeconds` | 30 | 1–300 |
| `maxLongPolls` | 4096 | long-polls a writer holds at once; size it to at least readers × shards per writer, and the writer's open-files limit above it |
| `maxTransfersPerWriter` | 8 | file transfers a writer serves at once (more get 503 with `Retry-After`) |
| `maxTransfersPerReader` | 4 | file transfers a reader serves to peer readers at once |

**Discovery.**

- `dns:<name>[:port]`: every A/AAAA record of the name is a writer, re-resolved on every refresh. A
  Kubernetes headless service is the usual example; give the fully qualified name
  (`pharos-writers.<namespace>.svc.cluster.local`). Readers connect to the IP addresses.
- `seeds:<url>,<url>`: the seeds, plus the writers they list (gossip). Every writer a reader polls
  receives the token, so without TLS a gossiped URL is polled only if its host is in `allowHosts`; with a
  truststore, every seed must be `https://` and only `https://` gossip is followed.

**`allowHosts`** (names or IPs, matched as written, ignoring case) is used three ways: the gossip rule
above; the peer reader hosts a reader may fetch from without TLS; and, on writers, the reader hosts whose
heartbeats may come from another address (NAT, proxies). So **peer fetch between readers is off unless
the reader hosts are listed**; every file then comes from the writer, which is correct, only slower.

**Peer readers.** A reader with an `advertiseUrl` serves the whole files of the generations it holds to
other readers. Peers serve whole files only; ranges (a lazy reader's cold blocks) always come from the
writer. Every file from a peer is checked against the manifest, and a peer that fails is skipped for
2 minutes.

**Conflicts.** A writer missing for 3 refreshes is dropped; the generations readers hold of its shards
keep serving. When two writers claim one shard, readers sync from the one with the higher `LATEST` (on a
tie, the lowest node id), and `/health` reports it under `peers.conflicts`.

**`/health` on peer nodes** adds a `peers` section (writers, conflicts, `peerFetch` counters; on a
writer, `transfers`). Without the token it names nodes only; with it, it also shows URLs.

### TLS

`cluster.tls` applies **only to peer-pull replication**, from readers to writers:

- On a writer, `keystore` (with `keystoreType`, `PKCS12` (default) or `JKS`, and `passwordEnv`, the
  name of the environment variable holding the password) makes `pharos web` listen with TLS only. It
  also listens with plain HTTP on `127.0.0.1:<localPort>` (`cluster.tls.localPort`, default web port + 1),
  serving everything except `/replication/*`; `./pharos` on the writer uses that port by itself.
- Readers may set only `truststore` (with `passwordEnv`); every seed must then be `https://`. A reader
  keystore is a config error: coordinators and `pharos router` speak plain HTTP to readers. A writer that
  sets `discovery`, and `pharos router`'s config, need the truststore and `https://` seeds too.
- With `dns:` discovery readers connect to IP addresses, so writer certificates need IP subject
  alternative names; otherwise use `seeds:` with host names the certificates carry.
- With a truststore, readers fetch from peers only over `https://`, and readers serve plain HTTP, so
  **peer fetch is off in a TLS cluster**.

`cluster.tls` on any other role or backend is a config error.

```json
"tls": {"keystore": "/etc/pharos/tls/writer-1.p12", "truststore": "/etc/pharos/tls/ca.p12",
        "passwordEnv": "PHAROS_TLS_PASSWORD"}
```

## Security

- **The cluster token** guards `/internal/*` (shard calls between readers, header
  `X-Pharos-Cluster-Token`, 401 without it), `/cluster/readers`, the full view of `/cluster/status`, and
  in peer mode every `/replication/*` route. Keep it out of `config.json` with `tokenEnv`.
- **The token travels in clear text** between readers, and in peer mode also to writers unless TLS is
  set. Run the cluster on a network only its nodes can reach (a Kubernetes `NetworkPolicy`, a private
  network or VPC). The operator installs such a `NetworkPolicy`.
- **Bucket write access is cluster control.** Whoever can write `catalog/readers/*.json` can point a
  reader's `advertiseUrl` elsewhere and receive token-bearing shard calls. Restrict bucket write access
  to cluster nodes.
- **In peer mode the token is the membership credential**: any holder can read every published file and
  join as a reader. A writer admits a reader's heartbeat only when its `advertiseUrl` host resolves to
  the address the heartbeat came from, or is in `allowHosts` (403 otherwise).
- The internal endpoints cap request sizes (4 MiB bodies on every route of a reader, 10 000 ids or
  terms, 4096-dim vectors).

## Limitations

- Module tools (MCP `list_modules`, `get_module_deps`, `find_module_path`, `get_module_boundary` and the
  web UI's module views) answer "not available on reader nodes yet".
- The cross-project graph (`pharos link`) is not replicated: call paths that span projects are found only
  on the writer.
- Sharding and resharding need the object-store backend.
