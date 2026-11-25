# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

## [1.10.0] - 2026-11-25

**Index format:** 1 (unchanged)

Multi-node Phase 5: peer pull, a cluster **without a bucket** (experimental). With
`cluster.backend: peer-pull` each writer keeps its published generations in a local peer store and serves
them from its `pharos web` on the token-guarded `/replication/*` endpoint; readers find the writers by DNS
(`dns:<name>`, e.g. a Kubernetes headless service) or a seed list with gossip (`seeds:<url>,…`), long-poll
each shard for new generations, and fetch whole files from other readers that already hold them. Reader
membership and the cluster status state live on the writers, so coordinators, `pharos router` and
`/cluster/status` work as with a bucket. Optional TLS protects reader-to-writer pulls. Phase 5 gate: top-10,
callers and usages identical to a single node on 99/99 queries, a new publish visible to readers within
0.6 s, 0 failed queries over 2 minutes with the writer killed, and the Phase 2/3 cold, warm, fan-out and
router-elasticity criteria passed as in bucket mode. **Single-node installs and bucket clusters are unaffected**
by peer pull (`object-store` stays the default backend). The index format is unchanged, but run
`pharos index --full` once for every project and re-run `pharos link`: older releases could write a call
graph that silently decodes wrong (see Fixed; UPGRADING.md "Upgrading to 1.10.0").

**When to use it.** Peer mode is for small or on-premises clusters where running S3 or MinIO is not worth
it. Prefer the bucket when you have one: peer mode has **no writer takeover** (no leases; the Phase 4 work
queue is forced off, so writers index and publish by hand), a writer that is down publishes nothing until
it is back (its shards keep serving what readers hold; a lazy reader's cold reads of them fail), the writer's
disk is the only copy of its published generations, and `cluster.token` is the only credential, visible on
the network except reader-to-writer with TLS. Read README "Without a bucket: peer-pull" and UPGRADING.md
("Upgrading to 1.10.0").

### Added (multi-node Phase 5: peer pull without a bucket)
- Peer-pull backend without a bucket (experimental, README "Without a bucket: peer-pull"):
  `cluster.backend: peer-pull`. A writer keeps its published generations in a local peer store
  (`cluster.peers.storeDir`; Lucene files hard-linked) and its `pharos web` serves them on the
  token-guarded `/replication/*` endpoint (catalog, `latest` long-poll, manifests, whole files
  and `Range`s, synonyms; at most `maxTransfersPerWriter` transfers, more get 503 with
  `Retry-After`; optional TLS on writers via `cluster.tls`). Readers find writers through
  `cluster.peers.discovery` (`dns:<name>[:port]` or `seeds:<url>,…` with gossip limited to
  `allowHosts`), long-poll each shard's `latest` (`longPollSeconds`), merge the writers' catalogs
  (a writer missing for 3 refreshes is dropped; its shards keep serving their cached generation)
  and fetch whole files from peer readers that hold the generation (checked against the
  manifest; `maxTransfersPerReader`). Reader membership and the cluster status state live on the
  writers (`POST /replication/readers`), so coordinators and `pharos router` work without a
  bucket. No writer takeover in this mode (`cluster.writer.workQueue` is forced off). `/health`
  reports `peers` (writers, conflicts, `peerFetch` files and bytes; on a writer `transfers`).
  - In peer mode `cluster.token` is the membership credential. A writer admits a reader heartbeat
    only when its `advertiseUrl` host resolves to the request's address or is in
    `cluster.peers.allowHosts` (403 otherwise). The token travels in clear text on the reader tier
    even with `cluster.tls` (which protects reader-to-writer pulls only): restrict the network.
  - `/health` shows the discovery spec, writer URLs and peer URLs only with
    `X-Pharos-Cluster-Token` (as `/cluster/status`); the public view names nodes only.
  - With `cluster.tls.keystore` a writer also listens with plain HTTP on
    `127.0.0.1:<cluster.tls.localPort>` (default web port + 1, no `/replication/*`), which
    `./pharos` uses.
  - `cluster.peers.maxLongPolls` defaults to 4096; size it to readers × shards per writer.
- A peer reader's shutdown is bounded as a bucket reader's is (1.9.0): on a slow or frozen writer
  the draining heartbeat and the withdraw each give up after 5 s with a `Stopping: gave up …` WARN,
  so the drain stays within `cluster.drainSeconds`.

### Fixed
- `./pharos` in a repo checkout runs the newest `target/pharos-*.jar` by version number. It sorted
  the names as strings, so a leftover `pharos-1.9.0.jar` (a `mvn package` without `clean`) would have
  won over `pharos-1.10.0.jar`. The gate scripts pick their default jar the same way (`sort -V`).
- A newly created call graph could be written with a stale property dictionary (rare; every release
  since the ArcadeDB graph backend, single-node and clusters alike). ArcadeDB queues a new database's
  first dictionary page for asynchronous flushing, and pharos then rewrote that page synchronously;
  when the queued copy reached the disk last, every later open decoded the records' property names
  wrongly: methods and edges vanished from the graph and other vertices came back with shuffled
  properties, with no error. Any graph creation could hit it: a full index, a writer takeover's
  restore, the cross-project link graph, the module graph. A new graph now waits until that page is
  on disk before anything else is written.
- Graphs damaged that way by an older release are now detected when opened (pharos always added the
  dictionary names in a fixed order) and logged as a WARN with the repair:
  - a project's call graph: the project's next index run is a full one, which rebuilds it; work-queue
    writers (which refuse `index --full`) repair their shards this way on their own. A standalone
    `pharos index --full <project>` is still recommended, to find the damage before a run does. As a
    backstop a writer refuses to publish a damaged graph (and to export it as `graph.cgf.gz`), so
    readers and takeovers never receive it; `index-status` reports it (`graphDamage` in `--json`),
    and `upgrade-indexes` re-indexes it.
  - the cross-project graph: the next `pharos link` rebuilds it.
  - the module graph (`~/.pharos/module-graph.arcadedb`, never re-created by a re-index): rebuilt
    empty automatically; each project's modules return when it is next indexed.
  A reader serving a published `graph.cgf.gz` cannot check it (the file has no dictionary): re-index
  on the writer, which then publishes a clean generation.

### Maintainers
- Phase 5 gate (`scripts/cluster-gate-phase5.sh`): every port is a setting (`GATE_W1_PORT`,
  `GATE_W2_PORT`, `GATE_P1_PORT`, `GATE_REF_PORT`, `GATE_ROUTER_PORT`, `GATE_D1_PORT`/`GATE_D2_PORT`,
  `GATE_LOAD_BASE_PORT`, the Phase 2/3 drivers' `GATE_LAZY_PORT`/`GATE_FULL_PORT` and
  `GATE_R1_PORT`..`GATE_R4_PORT`; probe bound `GATE_PORT_PROBE_TIMEOUT`), `GATE_PHAROS_SRC` snapshots
  pharos from a git tree, and `GATE_SEED_FROM` seeds the pharos (and mypal) embedding caches; defaults
  unchanged. `GATE_KEYWORD_ONLY=1` loads no embedding model anywhere: pharos and mypal are indexed
  `--no-embed`, ranking compares the keyword pipeline only and the router load is keyword only; a
  `GATE` set up in the other mode is refused. The header shows how to run it under
  `scripts/gate-watchdog.sh`.

## [1.9.0] - 2026-11-24

**Index format:** 1 (unchanged)

Multi-node Phase 4: work-queue writers. Writers take shards from the catalog under leases, poll each
shard's source (a git remote or a shared directory), index and publish its changes on their own, share the
shards among however many writers run, and take over a dead writer's shards with its writer state restored
from the bucket (Phase 4 gate: a writer killed mid-commit or mid-embed was replaced within 25–31 s, the resume
re-embedded only the changed files, 51 changes over scaling 1→4→1 writers were each published exactly once,
0 of 23 881 queries failed). New: a writer disk guard, `/cluster/status` with an SSE stream and a webhook for
scaling proposals, and a **Cluster** page in the web UI. **Single-node installs are unaffected** (they gain the
Cluster page, showing one node). No re-index.

**Behaviour change for cluster writers: `role: writer` now defaults to the work queue**
(`cluster.writer.workQueue: true`). A writer config without the key no longer accepts hand-run
`pharos index` / `pharos publish`; add `"writer": {"workQueue": false}` to keep indexing by hand. Its
`cluster.heartbeatSeconds` must be below `cluster.writer.leaseTtlSeconds` (30), or it does not start. Read
UPGRADING.md ("Upgrading to 1.9.0") before restarting writers.

### Added (multi-node Phase 4: work-queue writers)
- Work-queue writers (README "Work-queue writers"): `role: writer` nodes take shards from the
  catalog under leases (`cluster.writer.leaseTtlSeconds`, 30), detect source changes every
  `pollSeconds` at a fixed rate, also during runs (`pharos index|publish --source-git <url> --ref
  <ref>` or `--source-path` record the source; `--no-embed` is recorded as `"embed": false` and
  work-queue runs of that shard skip embeddings), publish changes, share shards
  (`minHoldSeconds`, 120) and take over a dead writer's shards with its writer state restored from
  the bucket. Writer disk guard (`cluster.writer.disk`), `/cluster/status`, `/cluster/events`, a
  webhook for scaling proposals (`cluster.status`) and the Cluster page. A writer's `pharos web`
  no longer loads the search models.

### Changed (upgrade note)
- `cluster.writer.workQueue` defaults to `true` on `role: writer` (after the Phase 4 gate). A
  writer config without the key now runs the work queue and refuses hand-run `pharos index` /
  `pharos publish` (exit 2); add `"writer": {"workQueue": false}` to keep indexing and publishing
  by hand. On a work-queue writer `remove-index` and `embed` are refused too (exit 2), and
  `upgrade-indexes` / `./setup.sh --upgrade-indexes` (exit 5): restarting the writer on a new
  release upgrades it, because its work loop gives a full run (and a re-publish) to every held shard
  whose local or published index format, chunking version or model is out of date.
- `cluster.heartbeatSeconds` must be below `cluster.writer.leaseTtlSeconds` (default 30) on a
  work-queue writer, validated at startup: a Phase 1–3 writer config with `heartbeatSeconds` of 30
  or more stops starting after the upgrade until it is lowered.
- `cluster.writer.disk.minFreePercent` defaults to 0 (was 10): the threshold is `minFreeBytes`
  (5 GiB), so a writer on a large disk no longer stays "low" and publishes nothing. A low writer
  logs a WARN once per low episode.
- Only one process per writer node runs the work loop (`<workDir>/.work-loop.lock`); another daemon
  with the same config (e.g. one `./pharos` auto-starts) logs a WARN and serves without a loop.
- Synonyms: a publish replaces only its own shard's section of the cluster's `synonyms.txt`
  (conditional read-modify-write), so writers holding different shards no longer drop each other's
  rules, and every reader serves the same file.
- `git ls-remote` of a work-queue source is killed (with its helper processes) after
  `cluster.writer.lsRemoteTimeoutSeconds` (default 30); an unreachable remote stalls only its own
  shard, and its failed polls count toward `maxConsecutiveFailures`.
- A takeover that restores a generation with an older writer-state backup (the newest one was lost)
  runs a full index, so documents of files deleted since never stay behind.
- A writer's graceful stop is bounded when the bucket is slow or hangs (gate run 2: two writers
  needed SIGKILL 120 s after SIGTERM): the stopping heartbeat, the lease releases and the heartbeat
  delete each have a deadline, so a stop ends within `cluster.drainSeconds` + 10 s and logs each call
  it gave up on. A lease taken by another writer aborts that shard's run (a renewal that merely
  timed out does not), and a full run's parse checks the abort between files (it could only stop after the parser's last file).
- Object-store calls are bounded (new `cluster.objectStore.callAttemptTimeoutSeconds`, default 20,
  `callTimeoutSeconds`, default 60, and `uploadMinBytesPerSecond`, default 1 MiB/s): a heartbeat put
  on the gate's saturated MinIO took about 4 minutes (three 30 s socket timeouts). Uploads get the
  call bounds plus their size at the minimum rate, so large generation uploads are not cut; a
  download's body stays bounded by the 30 s idle-read timeout.
- A reader's SIGTERM drain is bounded on a slow bucket too (the same pattern, also in 1.8.1): the
  draining heartbeat waits at most 5 s (and never past `drainSeconds`) for a tick stuck on the
  bucket and for its put, the heartbeat delete on close at most 5 s, and each call given up on is
  logged (`Stopping: gave up …`); the drain's `drainSeconds` now includes the announce.

### Maintainers
- Phase 4 gate (`scripts/cluster-gate-phase4.sh`): ports (`GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`,
  `GATE_ROUTER_PORT`, `GATE_WEBHOOK_PORT`, `GATE_R1_PORT`/`GATE_R2_PORT`, `GATE_W1_PORT`..`GATE_W6_PORT`,
  `GATE_WLOW_PORT`), sources (`GATE_LUCENE_SRC`, `GATE_PHAROS_SRC`), `GATE_MODELS_SRC` and an optional
  embedding-cache seed (`GATE_SEED_FROM`) are configurable, with a bounded port probe and curl connects
  (`GATE_PORT_PROBE_TIMEOUT`). `GATE_KEYWORD_ONLY=1` runs failover, scaling, status and the disk guard with no
  embedding model on any node (K2 then kills the `GATE_K2_SHARD` holder in `GATE_K2_PHASE`). Run it under
  `scripts/gate-watchdog.sh`. Defaults are unchanged.

## [1.8.1] - 2026-11-24

**Index format:** 1 (unchanged)

The v1.8.0 tag produced no release: its release build failed in a test, so no 1.8.0 jar was published.
1.8.1 is 1.8.0 plus the test fix below; there are no product changes. Upgrade from 1.7.x straight to 1.8.1
and read the 1.8.0 notes below (and UPGRADING.md) for what changed.

### Fixed
- Build: `PipelineSplitParityTest` failed on GitHub runners with an AMD CPU without AVX-512. Lucene picks fused
  or unfused multiply-add per CPU, so every vector score moved by one ulp against the fixture recorded on an
  Intel machine. Surefire now pins `-Dlucene.useScalarFMA=true -Dlucene.useVectorFMA=true`, and the test
  skips on hardware that cannot reproduce the recorded bits (no FMA, vectors narrower than 256 bits).

## [1.8.0] - 2026-11-23

**Index format:** 1 (unchanged)

Multi-node Phase 3: multi-shard fan-out and the router. A reader no longer has to hold every project: a query
that needs shards held elsewhere is coordinated by the reader that received it, and `pharos router` spreads
requests over the healthy readers. Fan-out rankings equal a single node's (Phase 3 gate: keyword 99/99
bitwise, hybrid τ 0.9996, vector 99/99; 3 600/3 600 requests answered while a reader joined and another
drained). **Single-node installs are unaffected** apart from the multi-project vector change under Changed.
Requires no re-index; cluster readers should read UPGRADING.md (config validation, health probes, stop timeouts).

### Added (multi-node Phase 3: fan-out and router)
- `pharos router --port <p>` (`./pharos router`): a stateless router over the healthy readers. It polls each
  reader's `/health` every 2 s, round-robins, retries an idempotent request once on another reader, never
  forwards `/internal/*`, and answers 504 when a forwarded exchange (body included) exceeds 120 s. It needs
  the object-store config and `cluster.token`, but no index.
- Fan-out on readers: a reader that does not hold every shard a query needs coordinates it. It classifies
  and embeds the query once, gathers BM25 term statistics from every shard (so keyword ranking equals a single
  node's), fetches raw candidates from the readers serving the other shards, and applies graph boost, fusion
  and reranking once over the merged lists. Vector first passes and unified recall pools are merged in two
  rounds (global top candidates, then rescored on their shards). Snippets and neighbourhood expansion come
  from the owning shard; each query pins one reader and generation per shard. A reader holding every
  requested shard searches locally, unchanged.
- Graph and lookup tools (callers, callees, usages, call chains, call paths, method and class lookup) reach
  shards held elsewhere.
- Partial answers: a shard no reader can answer for is named in the response warnings
  (`shard <name> unavailable: <reason>; results exclude it`) and the other shards still answer. When none
  answers and the reader holds none of them, the call answers 503 (`no shard answered`; MCP: a retryable tool
  error), so the router retries it on another reader.
- An internal shard API on readers (`/internal/term-stats`, `/internal/shard-search`, `/internal/snippets`,
  `/internal/callees`, `/internal/graph`), guarded by `cluster.token` (`X-Pharos-Cluster-Token`,
  constant-time compare, 401 without it). Coordinator and shard must use the same `synonyms.txt`; a shard
  refuses otherwise (409). Right after a `synonyms.txt` change that refusal lasts up to one
  `pollIntervalSeconds` and those shards are left out with a warning; it heals by itself.
- Draining on SIGTERM: a serving reader announces `draining` (heartbeat, and `/health` 503
  `{"status":"draining"}`), keeps serving for a quiet period (at least 4 s and one heartbeat plus 2 s, and
  until nothing has been in flight for 1 s) and stops after at most `cluster.drainSeconds` (default 20).
  Give the process manager a stop timeout above `drainSeconds` + 15 s (Kubernetes
  `terminationGracePeriodSeconds`, e.g. 45).
- Reader heartbeats carry `state` (`warming|ready|degraded|draining`) and `degradedShards`; coordinators skip
  a reader only for its degraded shards. `GET /cluster/readers` lists the live readers; it needs the cluster
  token (401 without it), as it names every reader's `advertiseUrl`.
- New config: `cluster.fanoutTimeoutMillis` (5000), `cluster.connectTimeoutMillis` (1000),
  `cluster.queryTimeoutMillis` (20000: one budget for all rounds of a query), `cluster.maxCatalogAgeSeconds`
  (600: past it while catalog reads are failing, or before the catalog was first read, unscoped queries
  answer from the reader's own shards with a warning naming the shards not searched; the catalog is re-read
  every 5 s) and `cluster.drainSeconds` (20).

### Fixed
- `pharos mcp-server` on a reader no longer joins reader membership. It wrote the heartbeat of the `web`
  daemon's node id (same config), and on exit marked it draining and deleted it; it now serves the reader's
  own shards only, and exits without a drain wait.
- Shutdown order on SIGTERM/SIGINT/SIGHUP: the reader drain, then the running model calls, then exit; JVM
  shutdown hooks (ONNX Runtime's environment closer among them) start only after that, so a signal no longer
  frees the model under a running query (SIGSEGV in `OrtSession.run`). Applies to `web`, `mcp-server`,
  `index`, `embed` and `upgrade-indexes`.
- Lazy readers missed a corrupt checksum when a file's footer straddles two blocks (or fills the last block
  alone); present since 1.7.0. The real footer is now compared wherever it falls in the blocks.

### Changed
- A vector, hybrid or unified search over several projects no longer fails when one of them has no vectors
  of the search model: that project is left out of the vector part with the warning
  `project <name> has no vectors for model <id>; vector results exclude it`, on a single node and on every
  cluster reader alike. A search scoped to that one project still fails, now with HTTP 409 on the web API
  (was 500) and a tool error on MCP; `./pharos search` exits 6 for it (exit 4 stays "index format
  incompatible").
- `/health` on a reader whose lazy generation has lost objects on the bucket now answers HTTP 200
  `{"status":"degraded","degradedShards":[…],"reason":…}` (1.7.0: 503), so the node stays routable for its
  other shards. Probes that treated "degraded" as down need no change; probes that expected 503 for it do.
- The S3 client waits up to `readTimeoutSeconds` or 30 s, whichever is longer, between reads of a response
  (ranged block reads keep their `readTimeoutSeconds` bounds), so whole-object downloads and uploads on a slow
  bucket are not cut short.
- Config validation: `cluster.retireGraceSeconds` must be at least `2 x cluster.pollIntervalSeconds` and at
  least `cluster.fanoutTimeoutMillis` (in seconds); a reader config that violates either now fails at startup
  with a message naming both fields (see UPGRADING.md).

### Maintainers
- Phase 3 gate driver: `scripts/cluster-gate-phase3.sh` and `scripts/cluster_gate_phase3.py`. Ports are
  configurable (`GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`, `GATE_R1_PORT`..`GATE_R4_PORT`,
  `GATE_REF_PORT`, `GATE_ROUTER_PORT`; defaults 9000, 9001, 7301-7304, 7305, 7080), and so are the
  sources, embed-cache seed and model source for hosts without a `~/.pharos` registry (`GATE_LUCENE_SRC`,
  `GATE_EXTRA_PROJECT` / `GATE_EXTRA_SRC`, `GATE_SEED_FROM`, `GATE_MODELS_SRC`).

## [1.7.1] - 2026-11-23

**Index format:** 1 (unchanged)

The first published build of 1.7: the 1.7.0 tag never produced a release, because the release
workflow failed on a flaky test fixture. 1.7.1 has the same code as 1.7.0; read 1.7.0's notes below
for what changed since 1.6.0.

### Fixed
- `RemoteDirectoryTest`'s flat fixture is always rewritten without compound files. Lucene sometimes
  flushed the sample index as a single compound segment, which left the fixture without a `.tim`
  file and failed `openingAReaderWithEdgesFetchesNothing` (test only; no runtime change).

## [1.7.0] - 2026-11-23

**Index format:** 1 (unchanged)

Multi-node Phase 2: on-demand warming. Readers no longer need a full copy of every generation: by default
they keep only the small Lucene files locally and read the rest in blocks, with ranged GETs, through a
size-capped block cache. **Single-node installs are unaffected** apart from the body fix below: the next index run
re-processes every file once and re-embeds only the documents whose text changed.

### Fixed
- Two files yielding the same qualified name (for example a Python module and a YAML or Markdown file with the
  same path stem, such as `releaseWizard.py` and `releaseWizard.yaml`) got each other's synthesized document
  body: it was cached by qualified name, so both documents carried the text of whichever file was indexed
  first, and which one won depended on file order. The cache is now keyed by file and qualified name.
  `CHUNKING_VERSION` 4 → 5: the next `pharos index` re-processes every tracked file once; embedding texts that
  did not change come from the embedding cache, so only the affected documents are re-embedded.
- On a lazy reader, `pharos embeddings status` reported "no" (and a `pharos index --full` hint) for every
  project, because it probed the generation dir, where most Lucene files are not local. It now reports
  "unknown (lazy reader; …)", and `index-status` no longer logs a WARN for the same probe.

### Added (multi-node Phase 2: on-demand warming)
- Reader modes (`cluster.reader.mode`): `lazy` (default) downloads only the manifest, the graph
  and the small Lucene files, and reads the rest in blocks through a size-capped local block cache
  (`<cacheDir>/.blocks`) with prefetch of term dictionaries and vector graphs before a new
  generation is swapped in; `full` copies every file. Keys under `cluster.reader`: `mode`,
  `cacheMaxBytes` (8 GiB), `blockSizeBytes` (8 MiB, power of two 64 KiB–64 MiB), `eagerMaxBytes`
  (1 MiB), `prefetchExtensions`, `prefetchTimeoutSeconds` (30), `maxConcurrentFetches` (16),
  `privateCacheMaxBytes` (0 = derived). Size the cache dir with the README's disk formula.
- `/health` on a reader: HTTP 503 `{"status":"warming"}` until its first sync and its search
  models are ready (model warm-up runs in parallel with the first sync; a model that fails or
  hangs for 5 minutes does not hold readiness), and HTTP 503 `{"status":"degraded","reason":…}`
  while a served lazy generation has lost objects on the bucket (it lags beyond retention); 200
  otherwise. Searches answered while warming carry a notice in `X-Pharos-Warnings`.
- `readerCache` (`mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate, bytesFetched,
  evictions, leasesOpen, shared, source`) in `index-status --json` and `/health` on readers, and a
  `reader cache: …` line in `index-status`.
- A lazy reader held on an older generation by a refused newer one downloads the rest of it in
  the background, so the bucket's GC can't strand it; switching a reader from lazy to full deletes
  the unused block cache.

### Changed
- **Readers upgraded from 1.6.0 switch to `lazy` mode** unless `cluster.reader.mode` is set; set
  `"mode": "full"` to keep 1.6.0's full download per generation (recommended for readers that may lag beyond
  the writer's retention, see README "Retention and lagging lazy readers").

### Maintainers
- Gate scripts (`scripts/cluster-gate.sh`, `scripts/cluster-gate-phase2.sh`): ports are configurable with
  `GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`, `GATE_WRITER_PORT` / `GATE_READER_PORT` (Phase 1) and
  `GATE_LAZY_PORT` / `GATE_FULL_PORT` (Phase 2); defaults are unchanged (9000, 9001, 7300, 7301, 7302). The
  free-port probe and curl connects are bounded by `GATE_PORT_PROBE_TIMEOUT` (default 2 s) for hosts where
  an unused port hangs instead of refusing.

## [1.6.0] - 2026-11-23

**Index format:** 1 (unchanged)

The first multi-node release (Phases 0 and 1): one writer and any number of readers sharing an
S3-compatible object store. **Without a `cluster` section in `config.json` nothing changes:** pharos stays the
single-node tool (`role: all`, `backend: local`), and its search and graph output is byte-identical to 1.5.0
(`SingleNodeGoldenTest`).

### Added (multi-node: writer + readers over an object store)
- `cluster` config section: `role` (`all` | `writer` | `reader`), `backend` (`local` | `object-store`), `nodeId`,
  `pollIntervalSeconds`, `cacheDir`, retention (`retentionGenerations`, `retentionMinutes`) and `objectStore`
  (`endpoint`, `region`, `bucket`, `prefix`, `pathStyle` for MinIO, `accessKeyEnv` / `secretKeyEnv`). Validated at
  startup; a writer requires the `object-store` backend, and missing S3 credentials fail fast. See README,
  "Multi-node (experimental)".
- **Writer:** after each index run, the project is published as a new immutable generation (Lucene commit files,
  a compact call-graph file, unresolved refs, and a manifest with the index format stamp), and `LATEST` is
  flipped with a conditional put. Unchanged files are reused across generations. Old generations are
  garbage-collected by count and age; GC validates manifests and re-checks data before deleting.
- `pharos publish <project>` (Java CLI and `./pharos`, writer only) retries the upload without re-indexing. A
  failed publish at the end of `pharos index` keeps the local index and exits **3**; `publish` refuses (exit
  **4**) a broken `callgraph.current` pointer or an index format other than this build's.
- **Reader:** polls `LATEST`, downloads whole generations into `cacheDir` (checksummed end to end, hard-linking
  files it already has), and swaps them in; handles `LATEST` going back. Readers serve search and call-graph
  queries from an immutable compact graph file (parity-tested against ArcadeDB) and class bodies/snippets from
  the index, never from source files. Only one process per `cacheDir` syncs (`.sync.lock`).
- A reader only materialises generations whose index format it can serve (its own or N-1); otherwise it keeps
  the generation it has and logs a WARN naming the fix.
- `pharos web` on a reader binds its port at once; until the first sync finishes `/health` reports
  `{"status": "syncing", "ready": false}` and searches carry the notice in `X-Pharos-Warnings`.
- `index-status --json` reports the node's `role`.

### Changed
- Readers refuse `index`, `link`, `embed`, `mine`, `remove-index`, `backfill-embedding-cache` and
  `upgrade-indexes` (also `./setup.sh --upgrade-indexes`, which points readers at the writer).
  `upgrade-indexes` on a reader exits with its own code **5**.
- Internal: graph read paths go through a read-only `GraphView`; index publishing and local caching go through
  the new `com.pharos.cluster` SPI (`LocalCache`, `IndexPublisher`, `ReplicationBackend`). No user-visible change
  on single-node installs.

### Not yet on readers
- Module tools (MCP `list_modules`, `get_module_deps`, `find_module_path`, `get_module_boundary`, web UI module
  views) answer "not available on reader nodes yet"; the cross-project graph is not replicated, so call paths
  spanning projects are only found on the writer.

## [1.5.0] - 2026-11-23

**Index format:** 1 (unchanged)

### Fixed (cross-encoder reranking lowered quality)
- `hybrid-reranked` scored below plain `hybrid`. Three causes, all fixed: the reranker passage never contained the
  class or camelCase method name (it now starts with `ClassName.methodName`); `CrossEncoderReranker` and
  `CrossEncoderMerger` replaced the first-stage score with a saturated cross-encoder probability (they now fuse
  first-stage rank and cross-encoder rank with equal-weight RRF, k=60); and `hybrid-reranked` reranked a smaller
  pool than `hybrid` (it now gets the same adaptive oversample). Measured on the real daemons (99 queries,
  MRR@10 / P@1): jina hybrid 0.588 / 0.495, hybrid-reranked 0.579 -> **0.647** / 0.444 -> **0.566**;
  granite hybrid 0.664 / 0.566, hybrid-reranked 0.569 -> **0.661** / 0.414 -> **0.556**.
  `auto` no longer dispatches to the reranked child (`_auto-hybrid-rrf` instead): reranked did not reach hybrid on
  both models (granite -0.003, within noise).

### Added (embeddings opt-in)
- `pharos embeddings enable|disable|status` (Java CLI and `./pharos`). **No default changes for anyone:**
  semantic search stays off until you enable it, and `enable` never downloads or embeds by itself (it prints
  the cost, about 600 MB download and a CPU-heavy first embed). **granite-embedding-english-r2 is now the
  recommended model** (`enable` default; CLS pooling, 768 dims, pinned commit) based on the embedding A/B
  (`docs/superpowers/gates/2026-11-16-embedding-ab.md`): hybrid-rrf MRR@10 +0.056 [+0.002, +0.112], hybrid
  +0.068 [+0.008, +0.129], tie with the cross-encoder reranker (-0.009); peak RSS about 2.8 GB vs 2.3 GB and
  slower CPU embedding. Existing jina installs keep jina; switch with
  `pharos embeddings enable --model granite-r2 --force` plus a re-embed (embedding caches do not carry over).
  `enable` refuses to replace a different provider without `--force`; asking for the model that is already
  configured (same URL ignoring an `@<sha>` pin, including the old `embeddingModelUrl` form) reports "already
  configured" and exits 0. `setup.sh` prints a hint that semantic search is off only when no embedding provider is
  configured.
- **Opt-in flow: `pharos index --full <root path>` embeds a project.** `enable` and `embeddings status` print it
  (status prints the exact command per project without vectors: `pharos index --full --single --project <name>
  <root path>`; `index` takes the project's directory, not its name). `pharos embed` is not the opt-in path: it
  only adds a model next to vectors an index already has. On an index with no vector fields it now fails with
  "this index has no vectors yet; run `pharos index --full …` to embed it" (nonzero exit), and it records a model
  in the project's `embeddedModels` only when the index holds that model's vectors afterwards (before, a run that
  embedded 0 documents registered the model anyway).
- The jina preset id is **`jina-v2-code`** (`pharos embeddings enable --model jina-v2-code`), the id README
  documented for the jina provider block before 1.5.0; the `jina-code-v2` spelling used during 1.5.0 development
  is gone. Existing configs keep whatever id they have (the vector field is `vec.<id>`).

### Changed
- DJL 0.28 → 0.40 (its tokenizers parse list-form BPE merges, which granite-r2's `tokenizer.json` uses). jina
  vectors are byte-identical, so existing caches and indexes stay valid.
- Embedding failures are loud: a broken model logs a WARN and aborts the embedding pass (a batch whose every text
  fails is retried with a canary text first; only a failing canary aborts), while a single failing text is
  tolerated and logged. The embedding cache is saved on abort. Search warns when the query cannot be embedded and
  results fall back to keyword-only.
- `hybrid-reranked` scores are now on the RRF scale (first-stage rank fused with cross-encoder rank), not
  cross-encoder probabilities; tools that thresholded the old scores need new thresholds.
- `embeddings status` reads vectors from each project's Lucene index (live documents in the model's vector field)
  instead of trusting the registry; it no longer reports "yes (legacy)" for keyword-only projects.
- `index-status` sets `needsReembed` only when an embedding provider is configured; its hint and
  `upgrade-indexes`' hints print the registered root path (`pharos index --full --single --project <name> <root
  path>`) instead of passing the project name where `index` needs a path.
- `upgrade-indexes` lists projects with legacy-model vectors and the exact `pharos index …` command for each; it
  does not run them (exit codes unchanged).
- A `config.json` that fails validation no longer locks you out: `pharos embeddings disable|status` still run,
  and every other command prints the error plus how to fix the file.
- Legacy (`url:dims`) embedding-cache and file-state fingerprints are accepted only when the provider uses mean
  pooling and no prefixes, the only settings older pharos versions embedded with.
- The CLI, the daemon and `setup.sh` run the JVM with `-DOPT_OUT_TRACKING=true` (and `Main` sets it when unset):
  DJL's telemetry otherwise calls the EC2 instance-metadata endpoint.

### Added
- Embedding pooling (`pooling`: `mean` or `cls`) and `queryPrefix` / `documentPrefix` per provider.
- `hf://owner/name@<sha>` pins a HuggingFace model to a commit (7-40 hex chars).
- External ONNX weights (`model.onnx_data`) are downloaded when a repo has them. Model caches downloaded by
  earlier versions are probed once for a missing `model.onnx_data` and completed; offline, the probe is skipped
  and retried on the next start.

### Fixed
- Vector search returned nothing (hybrid silently fell back to keyword) for configs using the old
  single-model form (`embeddingModelUrl`), or any provider with model id `legacy`: indexing wrote
  `vec.legacy` while search reads `vectorEmbedding`. Writers and readers now share one mapping
  (`DocumentMapper.vectorFieldFor` / `chunkVectorFieldFor`); named models keep `vec.<id>`.
  The first `pharos index` after upgrading re-indexes affected projects (indexes built by
  1.4.0-1.4.1 with the legacy model) in full, reusing cached embeddings (`index-status` flags such projects as
  `needsReembed` and prints the command). `pharos embed` migrates only single-chunk `.java` documents; the next
  `pharos index` finishes the rest. `pharos upgrade-indexes` lists these projects but does not migrate them.
  Index format unchanged.

## [1.4.1] - 2026-11-16

**Index format:** 1 (unchanged)

### Fixed
- Project-name safety: `pharos index .` registered a project named `.`, whose index directory was
  the indexes root, so removing it could delete every project's index. Default names now come from
  the directory name, invalid names (empty, `.`, `..`, separators, anything outside the indexes
  directory) are refused, and deletes and writes never leave the indexes directory. Existing bad
  registrations are reported as `invalid` and ignored; `pharos remove .` unregisters them without
  deleting anything (see UPGRADING.md, "Invalid project names").
- `setup.sh` printed "pharos pharos X" on its final line.

### Added
- `pharos --version` prints the installed jar's version and the CLI location.

## [1.4.0] - 2026-11-16

**Index format:** 0 → 1 (format 0 still served; re-index recommended, see UPGRADING.md)

First tagged release. Existing indexes keep working after upgrading (the previous format is
still served); re-index when convenient to move them to format 1.

### Added
- Index format stamp (`index-format.json`) per project, with N-1 compatibility: format 0 indexes
  are still searchable (status `upgrade-available`), older or newer formats are refused with a
  clear message.
- `pharos index-status` and `pharos upgrade-indexes` to inspect and re-index projects whose format
  is not current.
- Searchable-while-reindexing: the call graph is rebuilt into a new generation directory and
  switched in atomically, so readers are never disturbed by a full re-index.
- `setup.sh --release latest|vX.Y.Z` installs a published release with checksum verification,
  warns about projects that would become unsearchable, and `--upgrade-indexes` re-indexes them.
- GitHub release workflow publishing the jar, its checksum, the skill bundle, `pharos` and `setup.sh`.
- `UPGRADING.md` and this changelog.

### Changed
- Tests are isolated from the real `~/.pharos` via `-Dpharos.home` (set by surefire).

### Fixed
- Java nested and anonymous members are no longer indexed twice; `pharos embed --model` works (#18).
- Embedding caches survive fingerprint changes and are kept per model (#19).
- The search model is loaded once instead of per request (#20).
- Model loading is lazy, with background warm-up for the web UI and MCP server (#21).
- File size limits: `maxDocumentFileSizeKb` (10240) and `maxSourceFileSizeKb` (102400) (#22).
- JS extractor no longer indexes control flow as methods; script and doc ids are unique (#23).
- Call-graph index repair; callers are kept across incremental patches (#24).
- `daemon.log` is rotated (#25).
- No ONNX SIGSEGV on shutdown (#26).
- Unresolved call references are kept on disk instead of in memory (#27).
- Lucene directories are memory-mapped (#28).
- `index --no-embed` never loads the embedding model (#29).
