# Multi-node Phase 6: Custom Shard Keys and Live Resharding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A shard no longer has to be a whole project. Each index has a versioned **shard spec**, an ordered list of key fields plus an optional hash bucket count. Users set it globally or per project. Changing it runs a live background reshard: the new shards are built from source through the embedding cache, then one catalog CAS flips routing. Queries that filter on a key touch only the matching shards.

**Architecture:**
- **Assignment:** a `ShardSpec` assigns each document to a `ShardId` derived from its key values.
- **Writers:** they partition a project's documents by the spec when indexing. One parse pass feeds N shard writers, and the call graph is still built per project.
- **Catalog:** it records spec versions. `ShardRouter` (Phase 3) prunes shards by matching query filters against the spec.
- **Resharding job:** builds spec version N+1 shard by shard, publishes it under a new prefix, and flips `catalog/shards.json` with one CAS. Version N is garbage-collected after the retention window.

**Tech Stack:** Java 25, Lucene 10.4, AWS SDK v2 (conditional writes), JUnit 5, MinIO (gate).

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md`, section "Sharding" and Phase 6 in "Phases and gates". The live doc is rev 61.

**Base:** branch `feat/cluster-phase-6` from Phase 4. Phase 6 needs Phase 3's fan-out and distributed statistics, and Phase 4's leases and writer state. **Refresh this plan before execution** against what Phases 3 and 4 built (`ShardRouter`, `DistributedSearch`, `LeaseManager`, `WriterStateStore`, `/cluster/status` reshard proposals).

## Decisions made here (confirm before execution)

- **The call graph stays per project.** A project split into N shards still has one call graph. It is built once per index run, published as a `graph` object at the project level (`projects/<project>/graph/gen-N@crc`), and referenced by every shard manifest of that project.
  - Graph tools route to any reader serving any shard of the project.
  - Graph boost needs only per-doc `inDegree`, which is a stored field, so it is unaffected.
  - The alternative, per-shard partial graphs, would break callers and callees across shards. It was rejected.
- **Hash keys use a consistent-hash ring (user decision, 2026-11-17), not `mod buckets`.**
  - Each file's position is `murmur3_64(normalizedRelativePath)`, stable across platforms. Every document of a file lands in the same shard, so class and method documents of one file are never split.
  - Each shard owns `vnodes` points on the ring (default 128), at `murmur3_64(shardId + "#" + i)`. A file belongs to the shard owning the first point clockwise from its position.
  - **Changing the shard count moves only the files whose owner changes.** Adding one shard to N moves about 1/(N+1) of the files; removing one hands its ranges to its ring neighbours. Any count change is allowed: no double-or-halve rule.
  - Unaffected shards keep their current generations, and the new spec version references them unchanged.
  - Changes other than the ring's shard count, such as adding a key field, still run as a full reshard of that project.
- **Shard id format:** `<project>` for spec `[project]`, which is unchanged and backward compatible. Otherwise `<project>~<key>=<value>[~…]`, for example `lucene~ring=s03` or `lucene~module=core`. Ring shard names are stable (`s01`, `s02`, …, never renumbered), because their names place their points on the ring.
  - `ShardId` validation is extended to allow `~` and `=`.
  - Every `ShardId` maps to an object-store prefix safely, by URL-safe encoding.

## Global Constraints

- **Phase 6 gate (spec):** "Change a shard key under load with no failed queries; a key-filtered query touches only the matching shards."
- **Shard spec (spec, amended):** an ordered list of key fields plus an optional ring. Allowed keys: project, module, path prefix, language and file kind, plus `ring(path, shards=N, vnodes=V)`, which replaces the spec's `hash(path) % N`. Examples are `[project]` (the default), `[project, module]`, `[project, ring(path, shards=8)]` and `[language]`.
- **Versioning (spec):** "Changing it creates a new spec version; a spec is never edited in place."
- **Live resharding (spec):**
  - Writers build the new shards by re-indexing from source through the embedding cache, so nothing is re-embedded.
  - They publish under spec version N+1, and "one catalog CAS flips routing".
  - Readers serve version N until the flip.
  - Version N's shards are GC'd after the retention window.
  - The job can run shard by shard. With a ring change, only the shards that gain or lose files get new generations.
- **Pruning (spec):** a project, module or path filter selects only the matching shards. A query with no key filter fans out to every shard of the selected projects.
- **Placement (spec):** readers can be pinned to a subset of shards (Phase 3's `reader.shards` accepts the new ids).
- **Proposals:** Phase 4's "Reshard" proposal suggests a concrete spec, such as `[project, ring(path, shards=4)]`, or `shards=N+1` for an already-ringed project. Applying it stays manual through `pharos reshard`.
- **Backward compatibility:** the default spec `[project]` gives the shard ids, object layout and manifests of Phases 1–5 unchanged. `SingleNodeGoldenTest` stays identical. No `INDEX_FORMAT` change is needed for `[project]`. A project-level graph object is used only when a project has more than one shard.
- **Isolation:** tests and the gate never touch `~/.pharos` or :7171. `./pharos` stays in sync.

## Review Focus

1. **A reshard is interrupted** (writer crash or lease loss) halfway through building version N+1. Version N keeps serving, and the half-built N+1 is invisible: it is not in the catalog. The job resumes from the shards already published under N+1, or GC removes them after retention if the job is abandoned. Test owned by Task 4.
2. **A query arrives during the flip.** Each query reads the catalog once and uses one spec version throughout, so it never mixes shards of N and N+1. Test owned by Task 5.
3. **A document's key value changes between runs,** for example a file moved to another module. The incremental run deletes it from the old shard and adds it to the new one in the same publish cycle. A query never sees it twice or not at all after both publishes. Test owned by Task 2.
4. **A filter that can't be pruned** (a path filter against a hash-bucket spec) fans out to every shard of the project. The trace says why. Test owned by Task 5.
5. **The embedding cache is not reused across the reshard.** Resharding must not call the embedding provider for unchanged text. The provider call count during a reshard of an already-embedded project is 0. Test owned by Task 4.

---

### Task 1: `ShardSpec` and shard ids

**Files:** `cluster/sharding/ShardSpec.java` (parse, validate, version, `assign(DocKeys) → ShardId`), `cluster/sharding/DocKeys.java`, `cluster/ShardId.java` (allow `~` and `=`; encoded object-store prefix), `config` (`cluster.sharding.default`, `cluster.sharding.projects.<name>`), the catalog (`specs[{version, project?, keys, buckets}]`, `activeVersion`)

- [ ] **Tests:**
  - Parse and validation for every spec example.
  - `pathHashStableAcrossPlatforms`: golden values.
  - `sameFileSameShard`
  - `ringBalance`: 50 000 synthetic paths over 8 shards with 128 vnodes; every shard within ±15% of the mean.
  - `addingAShardMovesOnlyItsShare`: 8 → 9 shards moves between 8% and 14% of files, all of them to the new shard; 9 → 8 moves only the removed shard's files.
  - `shardNamesAreStable`: removing `s03` never renames or remaps `s04`.
  - `defaultSpecGivesProjectIds`: backward compatibility.
  - ShardId encoding round-trips.
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: shard specs and shard ids`.

### Task 2: Partitioned indexing and the project-level graph

**Files:**
- `indexer/ProjectIndexManager.java`: one parse pass with N per-shard Lucene writers. Incremental runs route deletes by previous key and adds by new key.
- `indexer/LuceneIndexer.java`: a per-shard index dir under the project.
- `ObjectStorePublisher`: publishes each shard's generation, and the project graph once.
- `ShardManifest`: an optional `projectGraph` reference.

- [ ] **Tests:**
  - `partitionedIndexUnionEqualsUnpartitioned`: the union of N shards' docs equals a `[project]` index's docs, and BM25 under global statistics matches.
  - `movedFileChangesShardInOneCycle`: Review Focus 3.
  - `graphPublishedOncePerProject`
  - `singleShardProjectUnchanged`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `indexer: partitioned indexing by shard spec`.

### Task 3: Readers and routing over spec versions

**Files:**
- `cluster/ReplicaSyncer.java`: syncs the shards of the catalog's active version. Shards of a version that is about to become active are synced ahead when the catalog lists them as "staged".
- `cluster/routing/ShardRouter.java`: filter matching (project, module, path prefix, language, file kind) against the spec.
- `search/distributed/DistributedSearch.java`: reads the catalog version once per query.

- [ ] **Tests:**
  - `moduleFilterTouchesOnlyMatchingShards`: counts shard calls.
  - `unprunableFilterFansOutAndTraces`: Review Focus 4.
  - `queryUsesOneSpecVersion`: Review Focus 2.
  - `stagedShardsSyncedBeforeFlip`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: routing and sync over shard spec versions`.

### Task 4: Resharding job

**Files:** `cluster/sharding/ReshardJob.java` (runs on writers under a per-project reshard lease), `cli/ReshardCommand.java` (`pharos reshard <project> --spec "[project, hash(path) % 4]"` and `--status`), and the catalog flip by CAS

**Behaviour:**
1. Write the new spec version as `staged`.
2. **Ring count change (incremental):** compute the moved files from the old and new rings. Only shards that gain or lose files get new generations: the source shards delete the moved files' docs, and the target shards index them from source with the restored writer state (embedding cache). Unaffected shards' current generations are referenced by N+1 as they are, with no re-index, no upload and no reader re-sync.
   **Other spec changes:** for each new shard, index from source with the restored writer state, then publish under N+1.
3. When all N+1 shards are published and readers report them synced (from heartbeats, or a timeout), CAS the catalog's `activeVersion` to N+1.
4. Mark N as `retired`. GC removes it after retention.

- [ ] **Tests:**
  - `reshardBuildsThenFlipsOnce`
  - `ringAddReindexesOnlyMovedFiles`: 4 → 5 shards re-indexes only the moved files (counted at the parser), unaffected shards' generations are byte-identical and referenced by N+1, and no reader re-downloads them.
  - `interruptedReshardKeepsVersionNServingAndResumes`: Review Focus 1.
  - `reshardReembedsNothing`: Review Focus 5. The provider call count is 0.
  - `concurrentReshardsOfOneProjectRefused`
  - `retiredVersionGcAfterRetention`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `cluster: live resharding`.

### Task 5: Status, proposals and the Cluster page

**Files:**
- Phase 4's `StatusBuilder`: spec versions, staged and retired state, and reshard progress.
- `Proposals`: the reshard proposal suggests a concrete doubled spec.
- The Cluster page: a sharding table per spec version, and reshard progress.

- [ ] **Tests:**
  - `statusShowsReshardProgress`
  - `reshardProposalSuggestsDoubledSpec`
  - A page fixture rendering a staged version.
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: resharding in status, proposals and the Cluster page`.

### Task 6: Phase 6 gate against MinIO

**Gate procedure:**
1. **Setup.**
   - The lucene project (`--no-embed`) plus pharos, with embeddings.
   - Two writers, three readers and a router.
   - About 10 QPS of mixed queries throughout.
2. **Reshard.**
   - `pharos reshard lucene --spec "[project, ring(path, shards=4)]"` under load, then `shards=5`. Then `[project, module]` for pharos.
   - **Pass:** the 4 → 5 step re-indexes at most 25% of lucene's files (expected ≈20%), and the unaffected shards' generations are unchanged.
   - **Pass:** 0 failed queries.
   - **Pass:** cross-project ranking after the reshard matches before, under the Phase 3 gate rules.
   - **Pass:** 0 embedding provider calls during the pharos reshard.
3. **Pruning.**
   - A module-filtered pharos query touches only that module's shard, checked by the shard call count in the trace.
   - A path filter on ring-sharded lucene fans out to all of its shards, as traced.
4. **Interruption.** Kill the writer running the reshard mid-way.
   - **Pass:** version N keeps serving, and the job resumes on the other writer and completes.
5. **Safety.** Marker check; kill processes by PID; leave :7171 alone.

- [ ] Write the scripts.
- [ ] Run the gate.
- [ ] Record the results.
- [ ] **Commit** `cluster: Phase 6 gate scripts and results`.
