# Upgrading

## Upgrading to 1.12.0

Index format: unchanged. Readers switch to deterministic vector arithmetic (README, "Multi-node CPU
determinism"); nothing to re-index.

- **Graph store: ArcadeDB 25.1.1 → 26.11.1, no step.** 1.12.0 opens 1.11.x graphs as they are, and it turns
  off ArcadeDB 26's super-node promotion and bloom filters, so 1.11.x can still read the graphs 1.12.0 writes.
  A downgrade or a mixed 1.11.x/1.12.0 cluster during the rollout needs no re-index.
- **Cluster fan-out, mixed versions.** A 1.11.x reader ignores the new optional `statsCheck` field of
  `/internal/shard-search`; the coordinator runs the separate statistics round with it, as before.
- **Kubernetes operator: one rolling restart of every pharos pod.** The operator renders a new env var,
  `PHAROS_ARITHMETIC`, on every writer, reader and router pod (and the project Jobs). That changes every pod
  template, so the first reconcile after the operator upgrade rolls all pharos pods, even when `spec.version`
  pins an older pharos image. On an older image the variable is ignored, so the restart is harmless, but it
  is a full rollout: schedule the operator upgrade as you would a release rollout (readers drain one at a
  time under their PodDisruptionBudget).
- **Mixed readers during the rollout.** Until every reader runs this release, old readers keep Lucene's
  per-CPU arithmetic: exact ties can still order differently between old and new replicas, as before. The
  new readers' WARN about differing profiles ignores old readers' heartbeats (they carry no profile).
- **Custom launchers.** A reader started without the deterministic flags now logs a WARN naming them; to accept native
  arithmetic, set `PHAROS_ARITHMETIC=native` with a pharos launcher, or pass `-Dpharos.arithmetic=native`
  (what that sets) in your own.

## Upgrading to 1.11.0

Index format: 1 → 2. This release brings multi-node sharding and live resharding (Phase 6) and the
Kubernetes operator. Documents now carry key fields (module directory, directory prefixes, file
extension, file kind) for the `module`, `pathPrefix`, `language` and `kind` search filters and the
shard keys; that is index format 2.

### What keeps working without a re-index

Under the N-1 rule a release serves its own format and the one before it:

- A format-1 index keeps working unchanged for every unfiltered search, call-graph and lookup
  query (status `upgrade-available` in `pharos index-status`).
- A *filtered* search over a format-1 project is refused with a message naming the project and the
  fix (`pharos index --full <project>`; the web API answers 409, `pharos search` exits 4). It is
  never answered silently empty.
- **Format-0 indexes** (written before 1.4.0's stamps) are now outside the read window (N-1 = 1):
  re-index them before upgrading, or right after (`pharos upgrade-indexes` lists and re-indexes them).
  `pharos upgrade-indexes --check` on the new jar reports, without changing anything, what it would
  refuse (exit 3).

### Single node

Restart on the new jar. The next index run of each project (incremental or not) is a full one, which
writes format 2; `pharos upgrade-indexes` re-indexes every project now. Embeddings come from the
embedding cache, so only the parse and the Lucene write are repeated. The call-graph advice of 1.10.0
below still applies if you skipped that release.

### Clusters (object-store backend)

1. **Check first:** `pharos upgrade-indexes --check` with the new jar against the bucket (any role;
   the operator runs it as its pre-flight Job). Exit 3 names the shards 1.11.0 would refuse (format 0
   generations); republish those on 1.10.0 first.
2. **Readers first, then writers, then the router.** A 1.11.0 reader serves the writers' format-1
   generations (unfiltered) until they are republished. Upgrade readers before relying on filters: a
   coordinator skips readers that do not advertise filter support (with a warning) for filtered
   queries.
3. **Writers:** restarted on 1.11.0, a work-queue writer re-indexes and republishes every shard it holds
   at format 2 (a full run each; the published format is older than the one it writes), so the cluster
   upgrades itself shard by shard. Size the writers' disk and time for one full run of every project.
   Writers without the work queue: run `pharos upgrade-indexes` (or `pharos index --full`) on the writer.
4. **Then reshard, if you want to.** Finish the rolling upgrade before running `pharos reshard`. A node of
   an older release can't read split shard ids (`p~ring=…`) and never prepares a reshard's new version,
   so while a reader of an older release heartbeats, `pharos reshard` is refused (exit 1, naming the
   nodes) and an already open reshard holds before staging and before its flip (past
   `reshardSyncTimeoutSeconds` too); `pharos reshard <p> --status` shows why it waits. It goes on once
   every live reader runs this release. After a reshard, do not roll a reader back to 1.10.0: it cannot
   read the split project's shards.
5. Projects are split only by `pharos reshard`. `cluster.sharding` in `config.json` is validated at
   startup but not applied in 1.11.0, so setting it changes nothing.

Peer-pull clusters (`cluster.backend: peer-pull`): the format change applies the same way; sharding
and resharding need the object-store backend.

### Kubernetes operator (new)

The operator is new in 1.11.0; there is nothing to migrate. To move an existing bucket cluster under
it, point a `PharosCluster` with `storage.s3` at the same bucket, with `spec.version: "1.11.0"` (images
`ghcr.io/<owner>/pharos:1.11.0` and `ghcr.io/<owner>/pharos-operator:1.11.0`). The operator owns
`cluster.objectStore` and always uses the default prefix `pharos`, so this works only for a cluster
that kept that prefix. Stop the hand-run writers first, so only the operator's writers take leases.
The CHANGELOG has the install steps.

## Upgrading to 1.10.0

Index format: 1 (unchanged). Peer pull changes nothing for single-node installs (`role: all`) or for
clusters with a bucket (`cluster.backend: object-store`, still the default): restart on the new jar.
One recommended step applies to every install:

### Recommended: rebuild the call graphs once

Every release since the ArcadeDB graph backend could, rarely, write a new graph whose property
dictionary was stale. Such a graph opens without an error but decodes wrongly: methods and edges are
missing, other vertices have shuffled properties, so callers, callees, usages and call paths are wrong.
1.10.0 creates graphs safely and recognises the damaged ones, but cannot repair a graph in place.

- **Every project:** run `pharos index --full <project>` once (or `pharos upgrade-indexes`, which
  re-indexes the damaged ones it finds). A full run builds a new call graph. Any index run of a
  damaged project now turns full by itself, so the graph is repaired at the latest on the project's
  next change; the explicit run repairs it now. Work-queue writers refuse `index --full` and need
  nothing: a shard whose local graph is damaged gets a full run and publishes a clean generation,
  which replaces the one readers serve. Writers without the work queue: run it on the writer.
- **Cross-project graph:** re-run `pharos link` afterwards; it rebuilds the graph when it is damaged.
- **Module graph** (`~/.pharos/module-graph.arcadedb`): a re-index never re-creates it. 1.10.0 rebuilds
  a damaged one automatically (empty; each project's modules return when it is next indexed, so the
  full re-indexes above refill it).

What 1.10.0 does when it finds a damaged graph: it logs a WARN naming the graph and the repair, and the
project's next index run is a full one. As a backstop a writer refuses to publish a damaged project graph
(the publish fails with that message) until a full run has rebuilt it.
`pharos index-status` shows a `warning:` line (`graphDamage` in `--json`). A reader can only check an
ArcadeDB graph, not a published `graph.cgf.gz`: repair on the writer.

### Optional: moving a cluster to peer pull (no bucket)

Peer pull (`cluster.backend: peer-pull`) is new and experimental; see README "Without a bucket: peer-pull"
for the configs. It trades the bucket for these limits: no writer takeover, the writer's disk is the only
copy of what it published, and the cluster token is the only credential. Keep the bucket if you rely on
Phase 4 failover or the work queue.

- **Writers.** Set `backend: peer-pull`, `cluster.token` (required) and `cluster.peers` (`storeDir` on the
  same file system as `indexDir`, `discovery`, `allowHosts`). Remove `"writer": {"workQueue": true}` if set
  (a config error in peer mode; the default is forced off) and publish by hand next to a long-running
  `pharos web`. The peer store starts empty: run `pharos publish <project>` (or `pharos index <dir>`) for
  each project of the writer so readers have a generation to pull. Each writer owns the shards in its own
  registry; give every shard exactly one writer.
- **Readers, coordinators and `pharos router`.** Set `backend: peer-pull`, the same `token`, and
  `cluster.peers.discovery` (`dns:<fully qualified name>` or `seeds:<url>,…`); drop `objectStore`. Readers
  need an `advertiseUrl` whose host resolves to the address they connect from, or is in the writers'
  `allowHosts`. Run `pharos router` with a reader's config.
- **TLS (optional).** A writer `cluster.tls.keystore` (password from `passwordEnv`) and a reader
  `truststore`; seeds must then be `https://`, `dns:` discovery needs IP SANs, and peer fetch between readers
  is off. On a TLS writer `./pharos` uses the plain loopback port `cluster.tls.localPort` (web port + 1).
- **Size `cluster.peers.maxLongPolls`** (default 4096) to at least readers × shards per writer, and the
  writer's open-files limit above it.
- **Network.** The token travels in clear text between readers and to coordinators: run the cluster on a
  network only its nodes reach (a Kubernetes `NetworkPolicy`, a private network).

## Upgrading to 1.9.0

Index format: 1 (unchanged); no re-index. Single-node installs (`role: all`) and readers need nothing.
**Cluster writers change behaviour** — read this before restarting them.

### Work-queue writers are the default

`role: writer` nodes now run the work queue by default (`cluster.writer.workQueue: true`): `pharos web`
on the writer takes shards from the catalog under leases, polls their sources and indexes and publishes
changes by itself. Hand-run `pharos index` / `pharos publish` are refused there (exit 2).

- **To keep your current hand-run writer**, add `"writer": {"workQueue": false}` to its `cluster`
  section before restarting it on 1.9.0. Nothing else changes for it.
- **To adopt the work queue**, every shard needs a source in the catalog, or it stays pinned to the writer
  that has its path: index it once on a hand-run writer with `--source-git <url> --ref <branch>` (or
  `--source-path <dir>` readable by every writer), see README "Work-queue writers". Give every writer the
  same `workDir` path, or each takeover re-parses the shard in full.

On a work-queue writer:

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
