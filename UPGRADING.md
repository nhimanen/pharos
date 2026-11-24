# Upgrading

## Upgrading to 1.8.0

Index format: 1 (unchanged); no re-index. Single-node installs need nothing. Cluster readers:

### `retireGraceSeconds` rules

`cluster.retireGraceSeconds` (default 120) must be at least `2 x cluster.pollIntervalSeconds` and at
least `cluster.fanoutTimeoutMillis / 1000` (default 5 s). A reader whose config breaks either rule
refuses to start and names both fields; raise `retireGraceSeconds` (or lower the other value).
Readers also need `cluster.token` (already required) for the new `/internal/*` shard API.

### Health probes and stop timeouts

- `/health` now answers **200** `{"status":"degraded",…}` for a reader with a degraded shard (1.7.0: 503),
  so the reader stays routable for its other shards, and **503** `{"status":"draining"}` while it drains.
  A readiness probe that only checks the HTTP status keeps working.
- On SIGTERM a reader drains for up to `cluster.drainSeconds` (default 20) before exiting. Set the
  process manager's stop timeout above `drainSeconds` + 15 s (Kubernetes
  `terminationGracePeriodSeconds`, e.g. 45), or it will be killed mid-drain.

## Upgrading to 1.5.0

Index format: 1 (unchanged). No default changes: semantic search stays off unless you configured it, and
existing jina installs keep jina.

### Legacy-model vectors

With the old single-model config (`embeddingModelUrl`) or a provider with model id `legacy`, 1.4.0-1.4.1
stored vectors in a field vector search never read, so vector search returned nothing. `pharos index-status`
flags such projects (`needsReembed`) and prints the command for each; it looks like this (`index` takes the
project's root directory, and `--project` keeps the registered name):

```bash
pharos index --full --single --project <name> <root path>
```

It re-indexes the project and re-embeds it from the embedding cache, so it is fast. `pharos upgrade-indexes`
lists these projects with the same command but does not migrate legacy vectors: use `pharos index`.

### Switching to granite-r2

Nothing changes on upgrade: no default model changed, no re-embed happens, and the index format is unchanged.
Semantic search is off unless you configured it; existing jina installs keep jina. granite-embedding-english-r2
is the *recommended* model when you do enable embeddings (see the A/B in the CHANGELOG).

To enable it (or to switch a jina install):

```bash
pharos embeddings enable                 # fresh: writes the granite-r2 provider
pharos embeddings enable --force         # replaces an existing jina provider
pharos daemon restart
pharos embeddings status                 # prints the index command for every project without vectors
pharos index --full <root path>          # per project; embeds it (`index` takes the directory, not the name)
```

The block written to `~/.pharos/config.json` (you can also paste it by hand):

```json
{
  "embeddingProviders": [
    {
      "modelId": "granite-r2",
      "type": "djl",
      "url": "hf://onnx-community/granite-embedding-english-r2-ONNX@2a49b9c076aa627b14bc528b36b67462808ccc23",
      "dimensions": 768,
      "maxTokens": 512,
      "pooling": "cls"
    }
  ],
  "searchEmbeddingModel": "granite-r2"
}
```

`enable` only edits the config and never embeds. `pharos embed` is not the way to embed a keyword-only
project: it adds a model next to existing vectors only, and fails on an index that has none. Embedding caches are per model and do not carry over, so the
first embed covers everything: expect hours for a large project on CPU (10.9 h for the lucene source tree in the
A/B, against about 30 min for jina from cache). Peak memory is about 2.8 GB (jina about 2.3 GB). To stay on jina
change nothing; `pharos embeddings status` shows what is configured and which projects have vectors.

## Invalid project names (e.g. ".")

### What happened

Before 1.4.1, `pharos index .` registered the project under the name `.`. A project's index
directory is `<indexes>/<name>`, so for `.` it was the indexes directory itself, and removing or
re-indexing that "project" could delete every project's index. Names such as `..` or ones
containing `/` had the same problem.

### What 1.4.1 does

- `pharos index .` now uses the directory's real name; names that are empty, `.`, `..`, contain
  `/` or `\\`, or do not resolve to a direct child of the indexes directory are refused (use
  `--project <name>`).
- Deletes and writes never leave the indexes directory.
- An existing `.` registration is ignored with a warning (`invalid` in `pharos index-status`,
  "invalid name" in `pharos projects`); `upgrade-indexes` reports it without failing.

### Removing the entry

```bash
pharos remove .
```

This only unregisters the entry (registry, link references, synonyms) and deletes nothing on disk.
Then re-index the directory with a proper name: `pharos index --project <name> <path>`.

### Stray files

A `.` project wrote its files directly into the indexes directory. These may be left over there and
can be deleted by hand: `callgraph-*.arcadedb*`, `callgraph.current`, `file-state.json`,
`graph.stamp`, `index-format.json`. Delete only these names, and only directly in the indexes
directory. If you also have a project named `lucene`, the `.` project may have placed
`segments_N` and `write.lock` directly in `indexes/lucene/`; those two may go too. The real
`lucene` index lives one level down, in `indexes/lucene/lucene/`: never touch that folder.
**Never delete project subdirectories** of the indexes directory.

## Upgrading to 1.4.0 (index format 1)

### What changed

1.4.0 stamps every project index with a format number (`index-format.json`). Indexes built by
earlier versions have no stamp and count as format 0. Document ids, the call-graph layout and the
embedding caches changed, so format 1 is what new indexing produces.

**Who is affected:** everyone with existing indexes.

**Compatibility (N-1):** a release serves its own format and the one before it. After upgrading,
your format 0 indexes keep working: search, callers and callees all still answer, and
`pharos projects` / `pharos index-status` show them as "upgrade available (still searchable)".
Nothing breaks, but you should re-index when convenient to get the fixes that depend on format 1.
Indexes more than one format behind (or newer than the installed release) are refused until they
are re-indexed.

### Steps

1. Pull and install the release:
   ```bash
   git pull && ./setup.sh --release v1.4.0
   ```
   (From a checkout older than 1.4.0 the `git pull` is what brings in the `--release` option.)
2. Review the warning `setup.sh` prints. Projects listed as "searchable now" keep working; projects
   listed as unsearchable need a re-index and the script asks before continuing.
3. Re-index:
   ```bash
   ./setup.sh --upgrade-indexes      # or: pharos upgrade-indexes
   ```
   You can check progress any time with `pharos index-status`.

### Time and cost

Re-indexing is a full run per project. Parsing dominates for large projects; embeddings are cheap
because the persistent embedding caches are reused, so unchanged text is not sent to the model
again. Projects stay searchable during the re-index (the old data is served until the new one is
published).

### Rolling back

1.4.0 is the first release, so there is no earlier release to install with `--release`: roll back
by building the older commit from source.

```bash
git checkout <older-commit> && ./setup.sh
```

A build older than 1.4.0 only knows the legacy graph dir `callgraph.arcadedb`. A 1.4.0 full re-index
removes that dir, and an older build then silently creates an **empty** graph in its place. It also
knows nothing about `index-format.json` or `callgraph.current`, so if you later upgrade again those
leftovers would make 1.4.0 serve the older build's data as current. So, for **every** project you
re-indexed with 1.4.0, after installing the older build:

1. Delete these entries in the project's index dir (`~/.pharos/indexes/<name>/`):
   `index-format.json`, `callgraph.current` and every `callgraph-*.arcadedb` (plus their sidecar
   files `callgraph-*.arcadedb.*`).
   ```bash
   cd ~/.pharos/indexes/<name> && rm -rf index-format.json callgraph.current callgraph-*.arcadedb*
   ```
2. Re-index it with the older build:
   ```bash
   pharos index --full <name> <path>
   ```

Do **not** use `--force` for this: it deletes the whole project index dir, including the
embedding caches, so every text is sent to the embedding model again.

Projects you never re-indexed with 1.4.0 (still format 0) are untouched and work as before.
