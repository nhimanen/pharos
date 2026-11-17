# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

## [Unreleased]

**Index format:** 1 (unchanged)

### Added (embeddings opt-in)
- `pharos embeddings enable|disable|status` (Java CLI and `./pharos`). **No default changes for anyone:**
  semantic search stays off until you enable it, and `enable` never downloads or embeds by itself (it prints
  the cost, about 600 MB download and a CPU-heavy first embed). **granite-embedding-english-r2 is now the
  recommended model** (`enable` default; CLS pooling, 768 dims, pinned commit) based on the embedding A/B
  (`docs/superpowers/gates/2026-11-16-embedding-ab.md`): hybrid-rrf MRR@10 +0.056 [+0.002, +0.112], hybrid
  +0.068 [+0.008, +0.129], tie with the cross-encoder reranker (-0.009); peak RSS about 2.8 GB vs 2.3 GB and
  slower CPU embedding. Existing jina installs keep jina; switch with
  `pharos embeddings enable --model granite-r2 --force` plus a re-embed (embedding caches do not carry over).
  `enable` refuses to replace a different provider without `--force`. `setup.sh` prints a hint that semantic
  search is off.

### Added
- Embedding pooling (`pooling`: `mean` or `cls`) and `queryPrefix` / `documentPrefix` per provider.
- `hf://owner/name@<sha>` pins a HuggingFace model to a commit (7-40 hex chars).
- External ONNX weights (`model.onnx_data`) are downloaded when a repo has them.

### Fixed
- Vector search returned nothing (hybrid silently fell back to keyword) for configs using the old
  single-model form (`embeddingModelUrl`), or any provider with model id `legacy`: indexing wrote
  `vec.legacy` while search reads `vectorEmbedding`. Writers and readers now share one mapping
  (`DocumentMapper.vectorFieldFor` / `chunkVectorFieldFor`); named models keep `vec.<id>`.
  The first `pharos index` after upgrading re-indexes affected projects (indexes built by
  1.4.0-1.4.1 with the legacy model) in full, reusing cached embeddings. `pharos embed`
  migrates them too, and `index-status` flags such projects (`needsReembed`). Index format unchanged.

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
