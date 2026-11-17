# Upgrading

## Legacy-model vectors (1.4.0-1.4.1)

With the old single-model config (`embeddingModelUrl`) or a provider with model id `legacy`, 1.4.0-1.4.1
stored vectors in a field vector search never read, so vector search returned nothing. After upgrading,
run `pharos index <project>`: it detects such projects (`pharos index-status` shows them) and re-indexes them
in full, reusing cached embeddings, so it is fast. Named models are unaffected.

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

## Switching to granite-r2 (optional)

New installs use granite-embedding-english-r2 by default. An existing install keeps its current model
(jina, or keyword-only); nothing changes on upgrade and no re-embed happens. The index format is unchanged.

To switch, replace the embedding settings in `~/.pharos/config.json`:

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

Then re-embed: `pharos index --full <project>` per project, or keep the old vectors and add the new ones
alongside with `pharos embed --model=granite-r2 --all` (list both providers in `embeddingProviders` and set
`searchEmbeddingModel` to the one to search). Embedding caches are per model and do not carry over, so the first
run embeds everything: expect hours for a large project on CPU (10.9 h for the lucene source tree in the A/B,
against about 30 min for jina from cache). Peak memory is about 2.8 GB (jina about 2.3 GB).

To stay on jina, change nothing; to pin it explicitly, keep your `embeddingProviders` entry for
`hf://jinaai/jina-embeddings-v2-base-code`.

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
