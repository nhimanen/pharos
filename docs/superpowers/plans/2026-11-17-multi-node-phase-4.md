# Multi-node Phase 4: Leases, Failover and Cluster Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Writers become a lease-based work queue: one writer holds every shard, or up to one writer per shard, and writers join or leave live. A crashed writer's shards are taken over without a full re-embed. Every node serves `/cluster/status` (layout, health and scaling proposals), with server-sent events and an optional webhook. The web UI gets a Cluster page drawn from that status.

**Architecture:**
- **Shards as a work queue:** each shard has a `lease` object owned through conditional puts. Writers heartbeat into `catalog/writers/`, and a balancer loop keeps each writer at its fair share. A writer releases leases only at clean points, after a publish.
- **Writer-only state** (embedding cache, file state) is backed up to `shards/<shard>/writer-state/` after each publish and restored on takeover. The `LATEST` CAS still guards publishing, so a stale writer can never publish.
- **Status and proposals:** built from the catalog and the heartbeats (writers and readers) by a pure function. Any node gives the same answer.

**Tech Stack:** Java 25, AWS SDK v2 (conditional writes), Javalin (SSE), JDK `HttpClient` (webhook), JUnit 5, MinIO (gate). The web UI uses the existing static frontend in `src/main/resources/web/` (check its stack before Task 6).

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md` (snapshot rev 58), plus the live doc https://claude.ai/artifact/F1XtggNDsoToZMc64tSeKh at **rev 61**. Rev 61 adds the "Cluster page (web UI)" section and the Phase 4 gate wording. Sections: "Writer assignment", "Commit protocol (writer)", "Cluster status and change proposals", "Cluster page (web UI)", "Phases and gates".

**Base:** branch `feat/cluster-phase-4` from `feat/cluster-phase-3` after Phase 3's gate passes. **Refresh this plan before execution:** check the Interfaces blocks against what Phases 2 and 3 actually built (`ReaderRegistry`, `ReaderHeartbeat`, `ShardRouter`, `/internal/*`, `readerCache`).

## Decision to confirm before execution: where writers get source code

The spec makes writers interchangeable, so after a takeover a different machine indexes the shard. That machine needs the project's source, and the spec doesn't say how it gets it. This plan proposes the following (Task 1):

- Each catalog shard entry gains `source: {kind: "path" | "git", location, ref}`.
- `path` is a directory every writer can read, such as a shared volume or a Kubernetes PVC.
- `git` is a clone URL plus a ref. A writer keeps a clone under `<workDir>/<shard>`, created on lease acquisition, and fetches before each run. Credentials come from the environment, never the catalog.
- Change detection polls: `git ls-remote <ref>` for git, and directory mtimes through the existing `FileStateTracker` for path. The interval is `cluster.writer.pollSeconds` (default 60).
- A shard with no `source` (registered by a plain `pharos index` on the writer) is indexed only by the writer that has the path. It keeps its lease, and the others never take it. This is today's behaviour, and Phase 1 works this way.

## Global Constraints

- **Phase 4 gate (spec, rev 61):** "Kill a writer mid-index: no corrupt generation, a second writer resumes without a full re-embed; scale writers from 1 to shard count and back under load with no lost or duplicate generations, and the Cluster page shows each scale change within one heartbeat."
- **Leases (spec):** `shards/<shard>/lease` = `{"writer": node, "expires": …}`, renewed by conditional put, with a **TTL of about 30 s**. "a writer that loses it stops publishing." Takeover happens by conditional put after expiry.
- **Fair share (spec):** "shard count ÷ live writers, rounded up". Surplus leases are released "at a clean point (after a publish, never mid-commit)".
- **Handoff (spec):** "The new owner restores writer state from `WriterStateStore` and resumes from the shard's `LATEST`, so there is no full re-embed. Lease CAS guards ownership and `LATEST` CAS guards publishing."
- **Status (spec):**
  - `GET /cluster/status` returns layout, health and proposals. Proposals have a stable id, kind, target, reason, metrics and suggested action.
  - `GET /cluster/events` streams server-sent events. An optional webhook receives the same events.
  - "Pharos applies none of them for now."
  - **Proposal thresholds (spec table):**
    - Add readers: warm p95 or QPS per reader above target for 5 min.
    - Remove readers: below 20% load for 30 min.
    - Add writers: backlog above target and fewer writers than shards.
    - Remove writers: writers mostly idle.
    - Reshard: a shard above about 50% of a reader's cache cap.
- **Cluster page (spec, rev 61):**
  - A layout diagram: writers, bucket and readers, with the shards per node and the health of each shard.
  - A node listing: role, shards, docs, index size, reader cache use and hit rate, QPS, warm p95, and heartbeat age.
  - Sharding: the spec and one row per shard.
  - Current scale against the proposals in force.
  - The page is drawn only from `/cluster/status` and live-updated from `/cluster/events`.
- **Unchanged:**
  - Single node (`role: all`): no lease code runs, and `SingleNodeGoldenTest` stays identical.
  - A Phase 1–3 writer configured without `cluster.writer.workQueue: true` behaves exactly as before. It publishes what it indexes and holds an implicit lease. Work-queue mode is opt-in until its gate passes, and the default flips in Task 7.
- Field names in status JSON reuse `readerCache` and the heartbeat fields from Phases 2 and 3 unchanged.
- **Isolation:** tests and the gate never touch `~/.pharos` or :7171. `./pharos` stays in sync.

**Out of scope:** applying proposals (`POST /cluster/proposals/<id>/apply` is later, behind auth), peer pull (Phase 5), custom shard keys and resharding (Phase 6). The operator consumes this phase's output later.

## Review Focus

1. **A writer is paused (GC, laptop sleep) past its lease TTL, then wakes and tries to publish.** The `LATEST` CAS fails or the lease check fails, and nothing is published. It stops work on that shard and logs it. Test owned by Task 2.
2. **Two writers both see a lease as expired and race to take it.** Exactly one conditional put wins; the loser backs off. Test owned by Task 2.
3. **The writer-state backup is missing or corrupt on takeover.** The new owner falls back to a full index of the shard, logged as a WARN, and still publishes a valid generation. A missing backup never blocks the shard forever. Test owned by Task 3.
4. **The balancer flaps** as writers repeatedly join and leave. Releases happen only after a publish, and a writer that just acquired a lease keeps it for at least one publish or `minHoldSeconds`, so leases don't bounce between writers. Test owned by Task 4.
5. **Status is consistent across nodes** while heartbeats arrive. Two nodes given the same catalog and heartbeat snapshot produce byte-identical `/cluster/status` JSON (sorted, no node-local state), and proposal ids are stable across refreshes. Test owned by Task 5.

---

### Task 1: Shard sources and the writer work directory

**Files:** `cluster/ShardSource.java` (record), `cluster/objectstore/ObjectStoreCatalog.java` (persist `source`), `cli/IndexCommand.java` and `cli/PublishCommand.java` (`--source-git <url> --ref <ref>`, or `--source-path` recorded in the catalog), `cluster/writer/SourceCheckout.java` (clone, fetch, change detection), `config/ClusterConfig.java` (`writer.workDir`, `writer.pollSeconds`, `writer.workQueue`)

**Interfaces:**
- `record ShardSource(Kind kind, String location, String ref)`.
- `SourceCheckout.prepare(ShardId, ShardSource) → Path`.
- `SourceCheckout.changedSince(ShardId, String lastRevision) → Optional<String newRevision>`.

- [ ] **Tests:**
  - `gitSourceClonesThenFetches`: uses a local bare repo, no network.
  - `pathSourceUsesDirectory`
  - `noSourceShardIsPinnedToItsWriter`
  - `credentialsNeverStoredInCatalog`
  - `changeDetectionByRevision`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: shard sources and writer checkouts`.

### Task 2: Shard leases

**Files:** `cluster/writer/LeaseManager.java`, `cluster/objectstore/ObjectStoreLayout.java` (`leaseKey`), `cluster/objectstore/ObjectStorePublisher.java` (verify the held lease before the `LATEST` CAS)

**Interfaces:**
- `Optional<Lease> tryAcquire(ShardId)` takes the lease when it is absent or expired, by conditional put.
- `boolean renew(Lease)` uses `If-Match`.
- `void release(Lease)`.
- `Lease(ShardId, String writer, Instant expires, String etag)`.
- A renewer thread renews at TTL/3.
- Expiry is judged by the store's `LastModified` plus the TTL, never the local clock alone.

- [ ] **Tests:**
  - `acquireWhenFree`
  - `cannotAcquireHeldLease`
  - `takeoverAfterExpiry`
  - `concurrentTakeoverExactlyOneWins`: Review Focus 2, 16 threads.
  - `pausedWriterCannotPublish`: Review Focus 1. The lease is lost, then publish throws `PublishConflictException` and `LATEST` is unchanged.
  - `renewFailsAfterTakeover`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: shard leases`.

### Task 3: `WriterStateStore`: backup and restore

**Files:** `cluster/WriterStateStore.java` (interface), `cluster/objectstore/ObjectStoreWriterStateStore.java`, `cluster/LocalBackend` (a no-op implementation), and the publish hook in `ProjectIndexManager`/`ObjectStorePublisher` (back up after a successful publish)

**Interfaces:**
- `backup(ShardId, Generation, Path indexDir)` uploads `embed-cache*.{bin,meta}` (content-addressed, only when changed) and `file-state.json` under `shards/<shard>/writer-state/gen-N/`.
- `Optional<Generation> restore(ShardId, Path indexDir)` restores the newest backup at or below `LATEST`.
- GC keeps writer-state for the retained generations only.

- [ ] **Tests:**
  - `backupAfterPublishRestoresIdentically`
  - `restoreThenIncrementalIndexReembedsNothingUnchanged`: the provider call counter stays at 0 for unchanged files.
  - `missingBackupFallsBackToFullIndex`: Review Focus 3.
  - `corruptBackupDetectedByChecksumAndIgnored`
  - `gcRemovesOldWriterState`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: writer state backup and restore`.

### Task 4: Writer heartbeats, balancer and work loop

**Files:** `cluster/writer/WriterHeartbeat.java`, `WriterRegistry.java` (mirrors `ReaderRegistry`), `Balancer.java`, `WorkLoop.java`, and `ClusterRuntime` wiring for `role: writer` with `writer.workQueue: true`

**Interfaces:**
- **Heartbeat:** `catalog/writers/<node>.json` = `{node, leases, backlog (shards with changes not yet published), indexing (shard or null), startedAt}`.
- **Balancer:**
  - `share = ceil(shards / liveWriters)`.
  - Acquire free or expired leases up to the share.
  - Release surplus only right after a publish of that shard, and never within `minHoldSeconds` (default 120) of acquiring it.
- **Work loop** (one shard at a time per writer, in order of oldest unpublished change):
  1. Restore writer state if this writer has none locally.
  2. `SourceCheckout`, then an incremental `index`.
  3. Publish (with the lease check), then back up writer state, then renew.
- **Graceful stop:** finish the current publish, release all leases, delete the heartbeat.

- [ ] **Tests** (in-JVM writers on one `InMemoryObjectStore` with a fake clock):
  - `oneWriterTakesAllShards`
  - `secondWriterGetsFairShareAfterPublishes`
  - `noReleaseMidCommit`
  - `crashedWriterLeasesExpireAndAreTakenOver`
  - `flappingWritersDoNotBounceLeases`: Review Focus 4.
  - `takeoverResumesFromLatestWithoutFullReembed`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `cluster: writer work queue with fair-share balancing`.

### Task 5: `/cluster/status`, proposals, events and webhook

**Files:** `cluster/status/ClusterStatus.java` (records), `StatusBuilder.java` (a pure function of the catalog, heartbeats and config), `Proposals.java` (rules with hysteresis state kept in a small object in the bucket, so every node agrees), `StatusEvents.java` (SSE plus the webhook with retry and backoff), `web/WebServer.java` routes, `config` (`status.targets.{warmP95Millis, qpsPerReader, backlog}`, `status.webhookUrl`)

**Interfaces:**
- **Status JSON:**
  - `layout: {shards[{shard, specVersion, keys, docCount, sizeBytes, generation, ageSeconds, writer, readers[]}], writers[...], readers[...]}`, with reader entries embedding the `readerCache` fields.
  - `health[]`.
  - `proposals[{id, kind, target, reason, metrics, action, since}]`.
  - Lists are sorted. A proposal id is `sha1(kind + target)`.
- **Events:** `{type: appeared|changed|cleared, proposal}` and `{type: layout, summary}` on scale changes.

- [ ] **Tests:**
  - `statusIsIdenticalAcrossNodes`: Review Focus 5.
  - One test per proposal rule, at its threshold and duration from the spec table: `addReadersAfter5MinAboveTarget`, `removeReadersAfter30MinBelow20Percent`, `addWritersWhenBacklogAndFewerWritersThanShards`, `removeWritersWhenIdle`, `reshardAbove50PercentOfCacheCap`.
  - `proposalIdsStable`
  - `sseStreamsAppearChangeClear`
  - `webhookRetriesThenGivesUp`
  - `singleNodeStatusShowsOneNode`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: /cluster/status with proposals, events and webhook`.

### Task 6: Cluster page in the web UI

**Files:** a new view in `src/main/resources/web/` (follow its existing stack and styling; read it first), a nav entry, and README screenshots or text

**Behaviour:**
- **Layout diagram** (SVG): writer boxes, a bucket box and reader boxes. Each writer lists the shards it holds, and each reader lists the shards it serves. Shard chips are coloured current, lagging or lease-expired. Lines show publish and sync.
- **Node table:** role, shards, docs, size, cache bytes of cap, hit rate, QPS, warm p95, heartbeat age.
- **Sharding table:** spec version and keys; one row per shard.
- **Scale panel:** live writer and reader counts and the proposals in force, each with its reason and metrics.
- Data comes from `/cluster/status`. `/cluster/events` triggers a refetch, with a 10 s poll as fallback.
- On a single node, the page shows one node holding every shard.

- [ ] **Tests:**
  - A frontend unit test for the status-to-view mapping, if the stack has a test runner. Otherwise a Java test that the page and its assets are served and that the status fixture renders without script errors, using the existing web test harness.
  - A golden JSON fixture of `/cluster/status` used by both.
- [ ] Implement.
- [ ] Check visually in a browser against a local three-node fixture, and record screenshots in the report.
- [ ] **Commit** `web: Cluster page`.

### Task 7: Phase 4 gate against MinIO

**Files:** `scripts/cluster-gate-phase4.sh`, `scripts/cluster_gate_phase4.py`, `docs/superpowers/gates/<date>-phase-4.md`

**Gate procedure:**
1. **Setup.**
   - Writers W1–W3 in work-queue mode. Shards come from local bare git repos: pharos, lucene (`--no-embed`), and two small fixture projects, so there are 4 shards.
   - Two readers plus a router.
   - About 10 QPS of query load throughout.
2. **Kill mid-index.** Start a lucene index on W1 and `kill -9` it during the embed or commit phase.
   - **Pass:** `LATEST` and the manifests are valid (no corrupt generation). Within the TTL plus one poll, another writer takes the lease, restores writer state and publishes. The provider call count on the resume is ≤ the number of changed files.
3. **Scale.** Go from 1 writer to 4 (one per shard), then back to 1, while sources change every 30 s.
   - **Pass:** every source change is published exactly once. Generations are consecutive per shard, with no gaps or duplicates (checked from the manifests' `parentGeneration`). There are 0 failed queries.
   - **Pass:** the Cluster page, scraped through `/cluster/status`, shows each writer count change within one heartbeat interval.
4. **Proposals.** Drive the reader load above its target for the configured window (shortened for the gate through config).
   - **Pass:** an "add readers" proposal appears on `/cluster/status`, is streamed on `/cluster/events` and is delivered to a local webhook sink.
5. **Safety.** Marker check; kill processes by PID; leave :7171 alone.
6. **If it passes,** flip the `writer.workQueue` default to `true` for `role: writer` and document it.

- [ ] Write the scripts.
- [ ] Run the gate and record the numbers.
- [ ] Investigate any failure before tuning; never lower a bar silently.
- [ ] **Commit** `cluster: Phase 4 gate scripts and results`.
