# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

## [Unreleased]

**Index format:** 1 → 2 (documents carry key fields for the new search filters; format-1 indexes
stay searchable unfiltered, re-index to filter — see UPGRADING.md)

### Added
- Multi-node (experimental, README "Multi-node"): a `cluster` section in `config.json` with
  `role` (`all` — the single-node default —, `writer`, `reader`) and `backend` (`local`,
  `object-store`). Without it pharos behaves exactly as before.
- Object-store backend for any S3-compatible store (`cluster.objectStore`: endpoint, region,
  bucket, prefix, pathStyle, credentials from the variables named by `accessKeyEnv`/`secretKeyEnv`
  or the AWS default chain, `readTimeoutSeconds`). A writer publishes each index run as an
  immutable generation (content-named objects plus a manifest carrying the index format stamp)
  and garbage-collects generations beyond `retentionGenerations` and older than `retentionMinutes`.
  `pharos publish <project>` retries a failed upload; `index` exits 3 when publishing failed, and
  `publish` exits 4 on an index it would not serve.
- Readers poll the store (`pollIntervalSeconds`), sync into `cacheDir` and serve search and
  call-graph queries; they refuse write commands (`index`, `link`, `embed`, `mine`,
  `remove-index`, `backfill-embedding-cache`, `upgrade-indexes`) and never read source files. A
  reader only swaps in generations whose index format it serves (N or N-1) and keeps serving its
  current one otherwise. One process per `cacheDir` syncs; others follow it and take over.
- Reader modes (`cluster.reader.mode`): `lazy` (default) downloads only the manifest, the graph
  and the small Lucene files, and reads the rest in blocks through a size-capped local block cache
  (`<cacheDir>/.blocks`) with prefetch of term dictionaries and vector graphs before a new
  generation is swapped in; `full` copies every file. Keys under `cluster.reader`: `mode`,
  `cacheMaxBytes` (8 GiB), `blockSizeBytes` (8 MiB, power of two 64 KiB–64 MiB), `eagerMaxBytes`
  (1 MiB), `prefetchExtensions`, `prefetchTimeoutSeconds` (30), `maxConcurrentFetches` (16),
  `privateCacheMaxBytes` (0 = derived). Size the cache dir with the README's disk formula.
- `/health` on a reader: HTTP 503 `{"status":"warming"}` until its first sync and its search
  models are ready (model warm-up runs in parallel with the first sync; a model that fails or
  hangs for 5 minutes does not hold readiness), HTTP 503 `{"status":"draining"}` while it shuts
  down, and HTTP 200 `{"status":"degraded","degradedShards":[…],"reason":…}` while a served lazy
  generation has lost objects on the bucket (it lags beyond retention): the node stays routable for
  its other shards. 200 `ok` otherwise. Searches answered while warming carry a notice in
  `X-Pharos-Warnings`.
- Reader heartbeats carry `state` (`warming|ready|degraded|draining`) and `degradedShards`;
  coordinators skip a reader only for its degraded shards.
  `GET /cluster/readers` on a reader lists the live readers; it needs the cluster token
  (`X-Pharos-Cluster-Token`, 401 without it), as it names every reader's `advertiseUrl`.
- Draining: on SIGTERM a serving reader announces `draining` (heartbeat and `/health` 503), keeps
  serving for a quiet period (at least 4 s and one heartbeat plus 2 s, and until nothing has been in flight
  for 1 s) and stops after at most `cluster.drainSeconds` (default 20). Give the process manager
  a stop timeout above `drainSeconds` + 15 s (Kubernetes `terminationGracePeriodSeconds`, e.g. 45).

- `pharos router --port <p>` (`./pharos router`): a stateless router over the healthy readers
  (`/health` polled every 2 s, round-robin, one retry of idempotent requests on another reader,
  `/internal/*` never forwarded; 504 when a forwarded exchange, body included, exceeds 120 s).
  Needs the object-store config and `cluster.token`, no index.
- `readerCache` (`mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate, bytesFetched,
  evictions, leasesOpen, shared, source`) in `index-status --json` and `/health` on readers, and a
  `reader cache: …` line in `index-status`.
- A lazy reader held on an older generation by a refused newer one downloads the rest of it in
  the background, so the bucket's GC can't strand it; switching a reader from lazy to full deletes
  the unused block cache.
- Cluster readers serve an internal shard API (`/internal/term-stats`, `/shard-search`, `/snippets`,
  `/callees`) guarded by `cluster.token` (`X-Pharos-Cluster-Token`, constant-time compare, 401 without it).
  New config: `cluster.fanoutTimeoutMillis` (5000), `cluster.connectTimeoutMillis` (1000).
- Fan-out (readers): a reader that does not hold every shard a query needs coordinates it. It
  classifies and embeds the query once, gathers BM25 term statistics from every shard (so keyword
  ranking equals a single node's), fetches raw candidates from the readers serving the other shards
  (`/internal/shard-search`), and applies graph boost, fusion and reranking once over the merged
  lists; snippets and neighbourhood expansion come from the owning shard. Each query pins one reader
  and generation per shard. A reader holding every requested shard searches locally, unchanged.
  Graph and lookup tools (callers, callees, usages, call chains, call paths, method/class lookup)
  reach shards held elsewhere through `/internal/graph`. A shard no reader can answer for is named
  in the response warnings (`shard <name> unavailable: <reason>; results exclude it`); the other
  shards still answer; when none answers and the reader holds none of them, the call answers 503
  (`no shard answered`, with the warnings; MCP: a retryable tool error) rather than an empty result,
  so `pharos router` retries it on another reader. Coordinator and shard must use the same `synonyms.txt` (a shard refuses
  otherwise, 409). An invalid project name answers 400 (an unusable shard catalog maps to 503 where no answer from local shards is possible).
  Vector first passes and unified recall pools are merged in two rounds (global top candidates,
  then rescored on their shards), so fan-out rankings equal a single node's. New config:
  `cluster.queryTimeoutMillis` (default 20000: one budget for all rounds of a query) and
  `cluster.maxCatalogAgeSeconds` (600: past it while catalog reads are failing, or before the
  catalog was first read, unscoped queries answer from the reader's own shards with a warning naming
  the shards not searched; the catalog is re-read every 5 s on a timer, so an idle reader is not
  "stale"). After a `synonyms.txt` change, shard calls to readers that have not polled it yet are
  refused (409) for up to one `pollIntervalSeconds`, and those shards are left out with a warning:
  transient, it heals by itself. A
  shard without vectors of the coordinator's model is named in a warning and left out of the vector
  part (see Changed).
- Work-queue writers (README "Work-queue writers"): `role: writer` nodes take shards from the
  catalog under leases (`cluster.writer.leaseTtlSeconds`, 30), detect source changes every
  `pollSeconds` at a fixed rate, also during runs (`pharos index|publish --source-git <url> --ref
  <ref>` or `--source-path` record the source; `--no-embed` is recorded as `"embed": false` and
  work-queue runs of that shard skip embeddings), publish changes, share shards
  (`minHoldSeconds`, 120) and take over a dead writer's shards with its writer state restored from
  the bucket. Writer disk guard (`cluster.writer.disk`), `/cluster/status`, `/cluster/events`, a
  webhook for scaling proposals (`cluster.status`) and the Cluster page. A writer's `pharos web`
  no longer loads the search models.
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

- Search key filters: `module` (the module's directory relative to the project root, the
  nearest directory holding a build file — not the Maven artifactId; `_` for files outside any
  module), `pathPrefix` (a directory relative to the project root, whole path segments),
  `language` (the file extension, case-insensitive) and `kind` (`code` or `doc`), on `/api/search`
  (`module`, `pathPrefix`, `language`, `kind`), `pharos search` (`--module`, `--path-prefix`,
  `--language`, `--kind`) and MCP `search_code` (`module`, `path_prefix`, `language`, `kind`).
  They only select documents: scores are those of the unfiltered query. On a cluster they also
  prune the shards of split projects; `--trace` shows the routing notes, and a filter that matches
  no shard of a project says so in the warnings. Readers advertise filter support in their
  heartbeat; a coordinator never sends a filtered query to a reader without it.
- `cluster.reader.shards` accepts shard ids of split projects and project wildcards (`lucene~*`:
  every shard of the project).
- Live resharding (object-store clusters): `pharos reshard <project> --spec "[project, ring(path,
  shards=4)]"` requests a new spec version; `--status` shows the project's versions and the job's
  progress, `--cancel` abandons it. The writer holding the project builds the new shards from its
  index (a ring change re-indexes only the files that change shard; shards that neither gain nor
  lose files keep their generations; embeddings come from the cache), readers keep serving the
  current version and prepare the new one, and one catalog CAS flips routing once they have it
  (or after `cluster.writer.reshardSyncTimeoutSeconds`, default 600). A query reads one version:
  right after the flip it reads the previous one until its readers serve the new one. The retired
  version is removed after `retentionMinutes`. An index run never adds shards: a new module under
  `[project, module]` needs `pharos reshard` with the same spec.
- Resharding is visible: `/cluster/status` gains `projects[]` (a split project is one entry over
  its shards, with its staged and retired spec versions and the reshard's phase — requested,
  building, pinned, flipped, abandoned — progress in shards and in files, and the reshard lease
  holder, and `published`, `generation`, `docCount` and `pending` over its active shards only) and
  `specs[]` (every spec version: active, staged, requested, retired, abandoned or
  superseded); shards gain `project`, `role`, `staged` and `stagedGeneration` (fields are only
  added). `/cluster/events` sends a `reshard` event at each phase and at each quarter of the
  shards or files. The Cluster page shows a Resharding panel, one sharding table per spec version,
  and marks staged shards in the layout. The `reshard` proposal targets a project and names the
  exact command, e.g. `pharos reshard lucene --spec "[project, ring(path, shards=4)]"` (a ringed
  project gets one more shard, or more for its size). MCP `list_projects` on a reader lists a split
  project once, with its shards.

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

### Fixed
- Multi-node: `pharos mcp-server` on a reader no longer joins reader membership. It wrote the
  heartbeat of the `web` daemon's node id (same config), and on exit marked it draining and deleted
  it; it now serves the reader's own shards only, and exits without a drain wait.
- Shutdown order on SIGTERM/SIGINT/SIGHUP: the reader drain, then the running model calls, then
  exit; JVM shutdown hooks (ONNX Runtime's environment closer among them) start only after that, so
  a signal no longer frees the model under a running query (SIGSEGV in `OrtSession.run`). Applies
  to `web`, `mcp-server`, `index`, `embed` and `upgrade-indexes`.

### Changed
- A reshard rewrites each affected shard whole: the moved files' documents are expunged (their
  deletes merged away) before the commit, so BM25 statistics count every document once. Those
  shards, HNSW vector graphs included, get new segment files, are re-uploaded in full and
  re-downloaded in full by readers. The expunge can be aborted (a graceful stop, a lost lease) and
  rolls back; the disk guard admits a reshard build only when about twice the project's Lucene
  files fit (old and new segments coexist until the commit).
- Multi-node fan-out fixes from the Phase 3 gate: a query naming a project the coordinator does
  not hold boosts it as a single node does (the project list comes from the shard catalog), and
  synonym expansions are scored with every expanded term's global statistics, terms absent on a
  shard included (`SynonymQuery`, `MultiPhraseQuery`), so fan-out keyword results equal a single
  node's.
- A vector, hybrid or unified search over several projects no longer fails when one of them has
  no vectors of the search model: that project is left out of the vector part with the warning
  `project <name> has no vectors for model <id>; vector results exclude it`, on a single node and
  on every cluster reader alike. A search scoped to that one project still fails, now with HTTP
  409 on the web API (was 500) and a tool error on MCP; `./pharos search` exits 6 for it (exit 4
  stays "index format incompatible").
- The S3 client waits up to `readTimeoutSeconds` or 30 s, whichever is longer, between reads of a
  response (ranged block reads keep their `readTimeoutSeconds` bounds), so whole-object downloads
  and uploads on a slow bucket are not cut short.
- Config validation: `cluster.retireGraceSeconds` must be at least `2 x cluster.pollIntervalSeconds`
  and at least `cluster.fanoutTimeoutMillis` (in seconds); a reader config that violated either now
  fails at startup with a message naming both fields (see UPGRADING.md).

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
