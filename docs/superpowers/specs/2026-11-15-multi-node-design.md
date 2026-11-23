<!-- Snapshot of https://claude.ai/artifact/F1XtggNDsoToZMc64tSeKh at rev 58 (2026-11-15). The live doc is the source of truth. -->
# Multi-node pharos: sharding and replication plan (draft 1)
 · 
Pharos becomes writers that index into immutable, versioned shard snapshots in object storage, and readers that serve search by warming a local cache from those snapshots on demand; replication sits behind a backend-neutral interface so writer-to-reader pull ships as a second backend without touching indexing or search. This is a first draft for your comments — open questions are at the end.
## Goals and non-goals
Goals
Separate writers from readers. Indexing (parsing + ONNX embedding, the expensive part) runs on writer nodes; search, MCP and graph lookups run on reader nodes that never index.
Shard and replicate behind one interface. Indexing and search code talk to an SPI (shard map, publisher, replica source, local cache); the replication backend is pluggable.
Backend 1: object storage. Writers publish immutable, versioned shard snapshots to an S3-compatible store; readers fetch what a query touches into a bounded local cache, warming on demand (OpenSearch remote-backed index / searchable snapshots, turbopuffer).
Backend 2: writer → reader direct pull. Readers fetch the same manifests and files straight from the writer's HTTP endpoint, so a cluster can run with no bucket. A new `ReplicaSource` plus a writer endpoint, same SPI.
Readers start cold and cheap. A new reader serves its first query within seconds of start, without copying every index first, and keeps only the hot part on local disk.
No regression for one machine. A single-node install keeps working unchanged: "local filesystem" is just another backend.
Environment-agnostic, elastic scale. Pharos runs anywhere (laptop, VMs, Kubernetes) and scales by adding nodes: from one writer for all shards up to one writer per shard, and any number of readers per shard, added or removed on demand.
Non-goals for this first step
Multi-writer per shard, or real-time (sub-second) freshness: a shard has one writer at a time and readers see a new snapshot within a poll interval.
Automatic resharding: with the default key a project stays one shard until a project outgrows a node, and then an operator changes its shard key (see Sharding).
Auth, tenancy and quotas beyond what the object store provides.
## What has to move
A pharos index today is a directory per project plus a few global files; each needs a replication rule. Sizes are from the local lucene project (77.7k docs).
 | 
State | 
Where today | 
Size (lucene) | 
Written by | 
Read by | 
Rule in multi-node
 | 
Lucene index | 
`<project>/lucene/` (26 files) | 
548 MB | 
indexer | 
search | 
Snapshot. Immutable segments + commit point = a natural versioned snapshot; segments are shared between snapshots
 | 
Call / knowledge graph | 
`<project>/callgraph.arcadedb` | 
180 MB | 
indexer (full rebuild or patch) | 
graph tools, search boost | 
Snapshot, whole. Writers keep ArcadeDB and export an immutable compact graph file per generation for readers
 | 
Unresolved refs | 
`<project>/unresolved-refs.json` | 
28 MB | 
full parse | 
cross-project linking | 
Snapshot (consumed by the linker)
 | 
Embedding cache | 
`<project>/embed-cache*.bin` | 
263 MB | 
indexer | 
indexer only | 
Writer state, not served: backed up to the store so a new writer doesn't re-embed for hours
 | 
File state | 
`<project>/file-state.json` | 
2.6 MB | 
indexer | 
indexer only | 
Writer state, backed up with the cache
 | 
Search params, synonyms | 
`search-params.json`, `synonyms.txt` | 
KB | 
user / miner | 
search | 
Config: published with the snapshot / cluster config
 | 
Project registry | 
`registry.json` | 
72 KB | 
indexer | 
everything | 
Control plane: becomes the shard catalog
 | 
Cross-project graph | 
`cross-project.arcadedb` | 
103 MB | 
linker | 
graph tools | 
Built by a linker writer job from per-project refs, published as its own cross-project shard
 | 
Module graph | 
`module-graph.arcadedb` | 
2.6 MB | 
indexer | 
module tools | 
Control plane / small global snapshot
The first two rows are 95% of what readers need, and only Lucene has the immutable-file shape that makes on-demand warming cheap.
## Architecture
Writers and readers never talk to each other: a writer publishes a generation, and readers discover it by polling `LATEST`. That is what keeps the replication backend swappable. Peer pull (Phase 5) replaces the store in the middle with the writer's own HTTP endpoint, using the same manifests. A node with `role: all` is today's single daemon using the `local` backend.
## Generic SPI
The core rule: indexing and search code never knows where bytes live. They see a shard, a generation and a local directory; backends move the bytes. A new `cluster/` package holds the interfaces, and today's single-node behaviour becomes the `local` backend.
 | 
Interface | 
Responsibility | 
Called by
 | 
`ShardCatalog` | 
Lists shards (`project` → `ShardId`), each shard's latest `Generation`, and its writer lease. Replaces `ProjectRegistry` as the source of truth | 
everything
 | 
`ShardRouter` | 
Maps a query's project filter to the shards to fan out to and the live readers that serve each one, spreading queries across them | 
coordinator
 | 
`IndexPublisher` | 
Writer side: after a commit, `publish(shard, localSnapshot) → Generation`. Uploads only new files, then flips the generation pointer atomically | 
`ProjectIndexManager` after `commit()`
 | 
`ReplicaSource` | 
Reader side: `latest(shard)`, `manifest(shard, gen)`, `open(shard, gen, file) → RemoteFile` with ranged reads | 
`LocalCache`
 | 
`LocalCache` | 
A bounded on-disk cache of remote files/blocks with LRU eviction and pinning per open generation; exposes a Lucene `Directory` and a local graph path | 
`LuceneIndexer`, `CallGraph` on readers
 | 
`WriterStateStore` | 
Backs up and restores writer-only state (embed cache, file state) | 
writer start-up / after index
 | 
`ReplicationBackend` | 
Factory that bundles a `IndexPublisher` + `ReplicaSource` + `ShardCatalog` implementation, chosen by config | 
`Main.DependencyFactory`
Backends:
`local`: today's behaviour. Publisher is a no-op, the source is the index directory itself, the catalog is `registry.json`. Also used by tests.
`object-store` (first real backend): S3-compatible API (MinIO for dev/tests, S3/GCS in production).
`peer-pull` (second backend, Phase 5): readers fetch the same manifests and files from a writer's HTTP endpoint instead of the bucket. Same manifest format, so it's a new `ReplicaSource` + a writer endpoint, nothing else.
Node roles come from config (`role: writer | reader | all`); `all` is the single-node default.
## Sharding
Sharding decides which shards a query touches, so a reader warms only the shards its queries hit, and inside those only the blocks they read (see On-demand warming). Shards can be added and re-keyed live, and users choose the keys.
Shard spec. Each index has a versioned shard spec in the catalog: an ordered list of key fields plus an optional hash bucket count. Examples: `[project]` (the default), `[project, module]`, `[project, hash(path) % 8]`, `[language]`. Keys are fields pharos already indexes: project, module, path prefix, language, file kind.
User-defined keys. Users set the spec in config, globally or per project. Changing it creates a new spec version; a spec is never edited in place.
Live growth. Under the default spec a new project is a new shard: a writer takes its lease, publishes, and the catalog adds it. Nothing else moves.
Live resharding. A spec change runs as a background job. Writers build the new shards (re-indexing from source through the embed cache, so nothing is re-embedded), publish them under spec version N+1, and one catalog CAS flips routing. Readers serve version N until the flip, and its shards are garbage-collected after the retention window. Hash splits double the bucket count (`% 8` → `% 16`), so each old shard maps to exactly two new ones and the job can run shard by shard.
Pruning. `ShardRouter` matches query filters against the spec: a project, module or path filter selects only the matching shards. A query with no key filter fans out to every shard of the selected projects.
Placement. Readers can be pinned to a subset of shards (see Reader membership), so with many shards each reader caches only its share.
When a shard outgrows a node. Each manifest records shard size and doc count, and readers report their working set against the cache cap plus warm p95. A shard is flagged when it passes about 50% of a reader's cache cap, or when warm p95 misses its target while the cache thrashes.
Nothing happens automatically. If the load is query volume, add readers; if it is size, use a bigger reader or change the project's shard key (for example `[project]` → `[project, hash(path) % 4]`), which runs as a live reshard. Queries on the split project then fan out across its shards like cross-project ones, using Phase 3's distributed term statistics. Both cases show up as proposals (see Cluster status and change proposals).
## Cluster status and change proposals
Every node serves `GET /cluster/status`: the current layout plus the changes pharos proposes. Pharos applies none of them for now; an operator, a script or an autoscaler acts on them, and automation can come later.
Layout. Shards (spec version, keys, size, doc count, latest generation and its age), writers (leases held, heartbeat), readers (shards served, cache use, QPS, warm p95) and routers.
Health. Shards whose latest generation lags source changes, expired leases, draining readers.
Proposals. Each has a stable id, a kind, a target, the reason, the metrics behind it and the suggested action.
Notifications. `GET /cluster/events` streams proposal changes (server-sent events), and an optional webhook in config receives the same events. An event fires when a proposal appears, changes or clears; the stable id lets hooks dedupe.
No extra state. Status is built from the catalog and the heartbeats, which carry each node's metrics, so any node gives the same answer.
 | 
Proposal | 
Raised when | 
Suggested action
 | 
Add readers | 
Warm p95 or QPS per reader above target for 5 min | 
Start N readers
 | 
Remove readers | 
Readers below 20% load for 30 min | 
Drain and stop N readers
 | 
Add writers | 
Indexing backlog above target and fewer writers than shards | 
Start writers; leases rebalance
 | 
Remove writers | 
Writers mostly idle | 
Stop writers; leases move
 | 
Reshard | 
Shard above ~50% of a reader's cache cap | 
New shard spec, e.g. `[project, hash(path) % 4]`
Later, `POST /cluster/proposals/<id>/apply` can run the actions pharos performs itself (reshard), behind auth.
## Object-storage backend
### Layout
<bucket>/<prefix>/
  catalog/shards.json                     # shard list + config (small, CAS-updated)
  shards/<shard>/
    LATEST                                # {"generation": 42, "manifest": "gen-42.json"}  (conditional put)
    lease                                 # {"writer": "node-a", "expires": ...}       (conditional put)
    manifests/gen-42.json                 # files, sizes, checksums, doc count, model fingerprint, parent gen
    lucene/<segment-file>                 # immutable, content-named, shared across generations
    graph/gen-42.graph.zst         # whole graph snapshot per generation
    refs/gen-42.json.zst
    writer-state/embed-cache-<model>.bin  # backup, not read by readers
### Commit protocol (writer)
Index locally exactly as today, then Lucene `commit()`.
Diff the commit's file list against the previous manifest; upload only new segment files (existing names are immutable, so they're reused).
Snapshot the graph and refs for this generation and upload them.
Write `manifests/gen-N.json`.
Conditional-put `LATEST` from N-1 to N (`If-Match` on the ETag). If it fails, another writer took the shard: stop and re-read the lease.
Garbage-collect: delete files referenced by no manifest within the retention window (default: last 3 generations + 1 hour), so a reader mid-query on an old generation doesn't lose files.
The writer holds the shard `lease` (renewed via conditional put); a writer that loses it stops publishing.
### Writer assignment
Shards form a lease-based work queue, so the number of writers can change live, from one writer for every shard up to one per shard. Each writer heartbeats into `catalog/writers/` and runs a balancer loop that aims to hold its fair share of leases: shard count ÷ live writers, rounded up.
Minimum setup. A single writer takes every free lease and indexes all shards.
Adding a writer. Existing writers see the new heartbeat, find themselves over their share, and release surplus leases at a clean point (after a publish, never mid-commit); the new writer takes them by conditional put.
Removing a writer. A graceful stop releases its leases after the current publish; a crashed writer's leases expire (TTL, e.g. 30 s) and others take them.
Handoff. The new owner restores writer state from `WriterStateStore` and resumes from the shard's `LATEST`, so there is no full re-embed. Lease CAS guards ownership and `LATEST` CAS guards publishing, so a stale writer can never publish.
### On-demand warming (reader)
A reader polls `LATEST` per shard (default 10 s) and opens a new `DirectoryReader` on the new generation, the same reopen path `LuceneIndexer` uses today.
Phase 1, file-level: opening a generation downloads the manifest and the small files (`segments_N`, `.si`, `.fnm`) eagerly; big files (postings `.doc/.tim`, vectors `.vec/.vex`, stored fields `.fdt`) download when first touched.
Phase 2, block-level: a `CachingRemoteIndexInput` fetches fixed blocks (e.g. 8 MB) with ranged GETs and keeps them in the local cache, the OpenSearch searchable-snapshot model. HNSW graphs and term dictionaries are prefetched, because random access to cold blocks costs one GET each.
The graph is not lazily loadable: a reader downloads the compact graph file for a generation when a graph tool or graph boost first needs it, then keeps it.
Eviction is LRU over blocks, never evicting files of a generation that has open searchers.
## Peer-pull backend
Readers pull directly from the writer that owns a shard, with no bucket in between. Manifests and content-named files are the same as in object storage, so only the transport changes.
Writer endpoint. `GET /replication/<shard>/latest`, `/manifests/gen-N.json` and `/files/<name>` with `Range` support, served from the writer's local index directory. Files of retained generations stay on disk under the same retention rule as GC (last 3 generations + 1 hour).
Reader. `PeerReplicaSource` implements `latest`, `manifest` and `open`; `LocalCache`, warming and the reopen path are unchanged, ranged reads just hit the writer instead of S3. Readers long-poll `latest` instead of polling every 10 s.
Catalog and leases. Without a bucket there is no shared CAS store, so the catalog is a static writer → shard map in config, and each writer serves `/replication/catalog`. Readers register with the writers they pull from, so they still come and go freely; there is no automatic writer takeover in this mode.
Writer load. Every cold reader hits the writer: cap concurrent transfers per writer, and let readers fetch files from a peer reader that already holds the generation (manifest checksums verify them).
Writer down. Readers keep serving the generation they hold; files not yet cached fail, and so do queries touching that shard. There is no copy outside the writer, so `WriterStateStore` falls back to local disk.
Security. TLS plus a shared cluster token on the replication endpoint.
## Query path
Any reader can coordinate; the CLI, MCP server and web UI connect to one reader (or a load balancer in front).
Reader membership. Any number of readers can serve a shard, and they join or leave at any time. Each reader heartbeats into the catalog (`catalog/readers/<node>.json`, TTL 30 s) with the shards it serves; `ShardRouter` spreads each shard's queries across live readers and drops a reader whose heartbeat lapses.
By default every reader can serve every shard, since its cache holds only what it touches; config can pin a reader to a subset. Adding a reader means starting one, and it warms on demand. Removing one drains in-flight queries, then stops the heartbeat. Autoscaling stays with the environment (Kubernetes HPA, a VM group, a script); pharos exposes per-reader load metrics for it.
### Request distribution
Clients stay simple: they get one URL or a short seed list, and any live reader can take a query and coordinate it. No separate query-service catalog is needed: the reader heartbeats in the bucket are the registry, so it is as redundant and scalable as the object store, and readers only cache it.
 | 
Mode | 
How a query reaches a reader | 
Use when
 | 
External routing | 
The client calls one URL; DNS, a Kubernetes Service or a cloud load balancer picks a reader by its `/health` | 
Production with routing infrastructure
 | 
`pharos router` | 
The client calls the router, a stateless proxy that reads the live reader list and round-robins, skipping readers that fail `/health`; run several for redundancy | 
No DNS or load balancer; tests (Testcontainers) and laptops
 | 
Client-side list | 
The client takes seed URLs, fetches `GET /cluster/readers` from any of them, round-robins, and refreshes every 30 s or on error | 
Later, if the extra hop matters
The reader that takes a query fans out to shard readers itself through `ShardRouter`, so clients never need to know shard placement. A draining reader answers `/health` with 503, so routers and load balancers stop sending to it. External routing and `pharos router` ship in Phase 3.
Route. `ShardRouter` turns the project filter into shards; no filter means all shards.
Embed once. The coordinator embeds the query (vector/hybrid), locally or through the configured remote service, and sends the vector, so shard readers don't each run ONNX.
Fan out. Each shard returns its top k with BM25 scores, vector scores and its `inDegree`/`maxInDegree` stats.
Merge. BM25 across shards needs consistent IDF: the first phase collects term statistics from shards (Lucene `CollectionStatistics`/`TermStatistics`), like Elasticsearch's DFS query-then-fetch. Vector scores are already comparable. Hybrid RRF fusion runs at the coordinator over the merged lists.
Post-process at the coordinator: graph boost (with a global `maxInDegree`), neighbourhood expansion (asks the owning shard for callees), cross-encoder rerank on the merged top N.
Fetch. Snippets and bodies come from the owning shard.
With one project per shard and project-filtered queries (the common case for MCP), steps 3–4 collapse to a single shard and there's no merge cost; cross-project queries are where distributed IDF matters.
Graph tools (`get_callers`, `find_call_path` …) go to the project's shard; cross-project paths go to the cross-project shard.
## Phases and gates
Each phase ships on its own and has a gate to pass before the next one starts.
 | 
# | 
Phase | 
Delivers | 
Gate
 | 
0 | 
SPI refactor | 
`cluster/` interfaces + the `local` backend; `ProjectIndexManager`, `LuceneIndexer`, `CallGraph` and `SearchEngine` go through them | 
All tests pass, single-node search results byte-identical, no measurable latency change
 | 
1 | 
Publish + full-copy reader | 
`object-store` publisher (manifest, LATEST CAS, GC); a reader that downloads a whole generation, then serves it read-only | 
Index pharos + lucene on writer, search on a separate reader against MinIO; same top-10 as single node; a new generation is visible in ≤ poll interval + download
 | 
2 | 
On-demand warming | 
Lazy file fetch, then block-level `CachingRemoteIndexInput`, LRU cache with a size cap, prefetch of term dicts / HNSW | 
Cold reader's first query p95 within a target (e.g. < 3 s for lucene); warm p95 within 10% of local; cache cap respected
 | 
3 | 
Multi-shard fan-out | 
`ShardRouter`, reader membership and load spreading, coordinator, distributed term statistics, global graph boost, pharos router and /cluster/readers | 
Cross-project query ranking matches single node (rank correlation on the eval set); adding or removing a reader under load fails no queries
 | 
4 | 
Leases and failover | 
Lease-based work queue, live rebalancing as writers join or leave, takeover, writer-state restore from the bucket, /cluster/status with proposals, events and webhook | 
Kill a writer mid-index: no corrupt generation, a second writer resumes without a full re-embed; scale writers from 1 to shard count and back under load with no lost or duplicate generations
 | 
5 | 
Peer pull | 
`peer-pull` `ReplicaSource` + writer HTTP endpoint for manifests/files | 
Same Phase 1–2 gates with no bucket; kill the writer and readers keep serving their cached generation
 | 
6 | 
Custom shard keys + live resharding | 
Versioned shard specs, user-defined keys, filter-based shard pruning, live resharding by spec version | 
Change a shard key under load with no failed queries; a key-filtered query touches only the matching shards
 | 
backlog | 
Near-real-time freshness | 
NRT segment publishing: readers see new docs without waiting for a full commit + generation | 
Only if ~10 s freshness proves too slow; sub-second visibility on readers
Phases 0–2 are the "object-storage replication first" milestone; 3 and 4 make it multi-tenant and robust; 5 adds peer pull for clusters without a bucket.
## Open questions
These are the places I'd most like your comments:
Target scale and deployment: decided. Pharos is environment-agnostic and scales by adding writers and readers. At most one writer per shard at a time, and one writer can own many shards; many readers per shard, added or removed on demand (see Reader membership). So Phase 3 is needed.
Object store: decided. Build against the S3 API, so S3, GCS and on-prem MinIO all work, and test with MinIO in Testcontainers.
Freshness: deferred. A new generation within ~10 s of the writer finishing is enough for now. Near-real-time (NRT segment publishing) is a backlog item in Phases and gates.
The graph: decided. ArcadeDB is a single mutable database, so it ships as a whole archive per generation (180 MB for lucene). Options: accept it; move readers to a read-only, immutable graph format (e.g. our GraphML or a compact adjacency file); or keep the graph only on writers and answer graph tools via RPC. Decision: an immutable compact graph file for readers, one per generation, matching the Lucene snapshot model; ArcadeDB stays on writers.
Cross-project graph and linking: decided. Rebuild on each reader from per-shard refs, or make the linker a writer job that publishes its own "cross-project" shard? Decision: the latter.
Who indexes what: decided. Decision: a lease-based work queue from the start. One writer can own every shard, and writers can be added or removed live up to one per shard (see Writer assignment).
How clients find the cluster: decided, auth open. Decision: `./pharos` and the MCP server talk to a remote reader URL (with the local daemon as fallback), reached through Request distribution. Still open: do we need auth on that API?
Embedding placement: decided. It follows the existing embedding config. With local hosting, writers and readers run ONNX, and readers load the model only to embed queries; with remote hosting, both call the configured embedding service and carry no model.
