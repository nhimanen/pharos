# Upgrading

## Index format 2 (search key filters)

### What changed

Documents now carry key fields (module directory, directory prefixes, file extension, file kind)
for the `module`, `pathPrefix`, `language` and `kind` search filters. That is index format 2.

### What you need to do

Nothing, unless you want the filters:

- A format-1 index keeps working unchanged for every unfiltered search, call-graph and lookup
  query (status `upgrade-available` in `pharos index-status`).
- A *filtered* search over a format-1 project is refused with a message naming the project and the
  fix (`pharos index --full <project>`; the web API answers 409, `pharos search` exits 4). It is
  never answered silently empty.
- The next index run of a project (incremental or not) is a full one, which writes format 2;
  `pharos upgrade-indexes` re-indexes every project.
- Format-0 indexes (before 1.x stamps) are now outside the read window (N-1 = 1) and must be
  re-indexed.

**Clusters:** readers serve format-1 generations unfiltered until the writers republish at
format 2. Upgrade readers before relying on filters: a coordinator skips readers that do not
advertise filter support (with a warning) for filtered queries.

**Resharding:** finish the rolling upgrade before running `pharos reshard`. A node of an older
release can't read split shard ids (`p~ring=…`) and never prepares a reshard's new version, so while
a reader of an older release heartbeats, `pharos reshard` is refused (exit 1, naming the nodes) and
an already open reshard holds before staging and before its flip (past
`reshardSyncTimeoutSeconds` too); `pharos reshard <p> --status` shows why it waits. It goes on
once every live reader runs this release.

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

## Work-queue writers (multi-node)

`role: writer` nodes now run the work queue by default (`cluster.writer.workQueue: true`). On such a
writer:

- **Upgrading is restarting it on the new release.** Its work loop gives every shard it holds a full
  run when this release needs one (an older local or published index format, a broken graph pointer,
  files embedded under another chunking version or model) and re-publishes it, without a source
  change. `pharos upgrade-indexes` and `./setup.sh --upgrade-indexes` refuse there (exit 5), as do
  `remove-index` and `embed` (exit 2): they would change the indexes under the running loop. The
  "Steps" of a format upgrade below apply to single nodes and hand-run writers
  (`"writer": {"workQueue": false}`).
- **`cluster.heartbeatSeconds` must be below `cluster.writer.leaseTtlSeconds`** (default 30), checked
  at startup: a writer config from an earlier phase with `heartbeatSeconds` of 30 or more no longer
  starts until you lower it (or raise the TTL).
- **Disk guard default:** `cluster.writer.disk.minFreePercent` defaults to 0, so the threshold is
  `minFreeBytes` (5 GiB). A percentage made writers on large disks permanently "low" (no lease
  taken, nothing published); set one explicitly if you want it. A low writer logs a WARN once per
  low episode.
- **One work loop per node:** a second pharos daemon with the same config on the writer host (one
  that `./pharos` starts on demand, say) runs no loop; it logs a WARN and serves everything else.

## Cluster readers: `retireGraceSeconds` rules

`cluster.retireGraceSeconds` (default 120) must be at least `2 x cluster.pollIntervalSeconds` and at
least `cluster.fanoutTimeoutMillis / 1000` (default 5 s). A reader whose config breaks either rule
refuses to start and names both fields; raise `retireGraceSeconds` (or lower the other value).
Readers also need `cluster.token` (already required) for the new `/internal/*` shard API.

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
   On a multi-node cluster: readers sync the upgraded generations; a work-queue writer refuses this
   step and upgrades by itself once restarted on the new release (see "Work-queue writers" above).
   Run it only on a hand-run writer (`"writer": {"workQueue": false}`).
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
