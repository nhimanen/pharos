# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

## [Unreleased]

**Index format:** 1 (unchanged)

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
  hangs for 5 minutes does not hold readiness), and HTTP 503 `{"status":"degraded","reason":…}`
  while a served lazy generation has lost objects on the bucket (it lags beyond retention); 200
  otherwise. Searches answered while warming carry a notice in `X-Pharos-Warnings`.
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
  shards still answer. Coordinator and shard must use the same `synonyms.txt` (a shard refuses
  otherwise, 409). An invalid project name answers 400 and an unreadable shard catalog 503.
  Vector first passes and unified recall pools are merged in two rounds (global top candidates,
  then rescored on their shards), so fan-out rankings equal a single node's. New config:
  `cluster.queryTimeoutMillis` (default 20000: one budget for all rounds of a query) and
  `cluster.maxCatalogAgeSeconds` (600). A shard without vectors of the coordinator's model is named
  in a warning and left out of vector retrieval.

### Changed
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
