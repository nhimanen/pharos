# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

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
