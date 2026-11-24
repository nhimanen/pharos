# Multi-node Phase 4: Leases, Failover and Cluster Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Writers become a lease-based work queue: one writer holds every shard, or up to one writer per shard, and writers join or leave live. A crashed writer's shards are taken over without a full re-embed. Every node serves `/cluster/status` (layout, health and scaling proposals), with server-sent events and an optional webhook. The web UI gets a Cluster page drawn from that status.

**Architecture:**
- **Shards as a work queue:** each shard has a `lease` object owned through conditional puts. Writers heartbeat into `catalog/writers/`, and a balancer loop keeps each writer at its fair share. A writer releases leases only at clean points, after a publish.
- **Writer-only state** (embedding cache, file state) is backed up to `shards/<shard>/writer-state/` after each publish and restored on takeover. The `LATEST` CAS still guards publishing, so a stale writer can never publish.
- **Status and proposals:** built from the catalog and the heartbeats (writers and readers) by a pure function. Any node gives the same answer.

**Tech Stack:** Java 25, AWS SDK v2 (conditional writes), Javalin (SSE), JDK `HttpClient` (webhook), JUnit 5, MinIO (gate). The web UI uses the existing static frontend in `src/main/resources/web/` (check its stack before Task 6).

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md` (snapshot rev 58), plus the live doc https://claude.ai/artifact/F1XtggNDsoToZMc64tSeKh at **rev 61**. Rev 61 adds the "Cluster page (web UI)" section and the Phase 4 gate wording. Sections: "Writer assignment", "Commit protocol (writer)", "Cluster status and change proposals", "Cluster page (web UI)", "Phases and gates".

**Base:** branch `feat/cluster-phase-4` from `feat/cluster-phase-3`, after Phase 3's final review and its fix wave. Phase 3's gate passed on 2026-11-21 (607154a).

**Refreshed 2026-11-21** against Phase 3 at 607154a. The section "What Phases 1–3 already provide" lists the code this plan builds on. The Interfaces blocks below were corrected to match it.

## What Phases 1–3 already provide (refresh, 2026-11-21)

**Object store**
- `ObjectStore` (`cluster/objectstore/ObjectStore.java`) has these operations:
  - `get` returns a `StoredObject(data, etag)`;
  - `putIfAbsent` and `putIfMatch` each return `Optional<etag>`, and empty means the CAS lost;
  - `list(prefix)` returns `ObjectInfo(key, size, lastModified)`;
  - there is no `head` or `stat`.
- `StoredObject` carries no `lastModified`.
- `InMemoryObjectStore(Clock)` in the tests takes an injectable clock.

**Liveness by the bucket's clock**
- `ReaderRegistry` already judges liveness by the bucket's clock: it takes the `LastModified` of its own heartbeat write, found in the listing, as "now".
- `ReaderHeartbeat` has these fields: `node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, state, degradedShards`. It is annotated `@JsonIgnoreProperties(ignoreUnknown = true)`, so new fields are safe for older nodes.
- It has no latency field. `LoadCounter` only counts `inFlight` and `qps`.

**Catalog**
- `catalog/shards.json` is `{shards: [{id}]}`.
- `ObjectStoreCatalog` reads only `id` and rewrites the file with an `ObjectNode` read-modify-write CAS, so unknown per-shard fields survive a rewrite by an older writer.
- `ShardCatalog` exposes only `shards()`.

**Layout keys** (`ObjectStoreLayout`)
- `catalogKey`, `readerKey`/`readersPrefix`, `synonymsKey`, `shardPrefix`, `latestKey`, `manifestKey`, `luceneKey`, `graphKey`, `refsKey`.

**Publishing**
- Publishing calls `ObjectStorePublisher.publish(LocalSnapshot)`. It uploads the files, `putIfAbsent`s the manifest, then runs the `LATEST` CAS, and throws `PublishConflictException` on a lost race.
- It has two callers: `ProjectIndexManager` (around line 440, `checkPublishable` and then `publish` after an index run) and `PublishCommand`.
- `ShardManifest` already carries `parentGeneration`, `writerNode`, `docCount`, `sizeBytes`, `modelFingerprint`, `indexFormat` and `projectMeta`, so the gate's consecutive-generation check and the status `docCount`/`sizeBytes` come from manifests.
- The call graph is ArcadeDB. It is published per generation under `graphKey` behind a graph pointer.

**GC**
- `GenerationGc(store, layout, keepGenerations, minAge)` takes its numbers from `cluster.retentionGenerations` and `cluster.retentionMinutes`.

**Roles and runtime**
- `NodeRole` is `ALL | WRITER | READER`.
- `ClusterRuntime` builds the Phase 3 classes only for `READER`, so writer wiring is new.
- `ShutdownSequence` (in the `com.pharos` package) runs drain and then stops model calls on SIGTERM, SIGINT and SIGHUP before ORT's shutdown hook. Writer shutdown must use it too.

**Web and router**
- `WebServer` serves `/health`, plus `/cluster/readers` (on readers only, 404 elsewhere), plus `/internal/*` (on readers only, behind the token).
- `drainCounted` already counts `/cluster/*`.
- `ClusterTokenSecrecyTest` guards against the token leaking.
- `pharos router` (`RouterCommand`) forwards only `/health` and `/api/*`, through a path allowlist, a 64 MiB cap and a total deadline returning 504.

**Web UI**
- The UI in `src/main/resources/web/` is vanilla JS (`app.js`, D3 v7) with `index.html` and `style.css`. Views switch with `btn-view` buttons.
- There is no frontend test runner. Web tests are Java (`WebServerHealthTest` style).

**Not built yet** (all Phase 4): `WriterStateStore`, writer heartbeats, leases, any `/cluster/status`.

## Decision (confirmed by the user, 2026-11-17): where writers get source code

The spec makes writers interchangeable, so after a takeover a different machine indexes the shard. That machine needs the project's source, and the spec doesn't say how it gets it. Confirmed: git with polling, or a shared directory, configurable per deployment (Task 1):

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

**Refresh:** `source` is a new optional field on the existing `{id}` catalog entry, written by the same CAS loop as `register`.
- Add `register(ShardId, Optional<ShardSource>)`, which sets or updates `source` idempotently.
- Add `entries()`, returning `List<ShardEntry(id, Optional<ShardSource>)>`.
- `shards()` stays as it is, so readers and `CachedCatalog` are untouched.
- Test that an older writer's `register` keeps an existing `source`.

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

**Refresh:**
- **Expiry needs the bucket's `LastModified`, which `get` doesn't return.** Add `Optional<ObjectInfo> stat(String key)` to `ObjectStore`:
  - S3 `HeadObject` in `S3ObjectStore`;
  - the stored entry in `InMemoryObjectStore`;
  - a default implementation via `list(key)` that filters for an exact key match.
- Judge "now" the way `ReaderRegistry` does: by the `LastModified` of the writer's own latest write (its heartbeat or lease renewal). Factor that helper out of `ReaderRegistry` instead of copying it.
- **The publisher gets the lease check through an injected `LeaseGuard`** (`void assertHeld(ShardId) throws PublishConflictException`):
  - It is called in `ObjectStorePublisher.publish` right before the `LATEST` CAS. It re-reads the lease, compares the ETag with the held one and checks expiry by the bucket's clock.
  - The default `LeaseGuard.IMPLICIT` always passes. That keeps the non-work-queue writer and `PublishCommand` unchanged.

**Files:** `cluster/writer/LeaseManager.java`, `cluster/writer/LeaseGuard.java`, `cluster/objectstore/ObjectStore.java` + `S3ObjectStore` + test `InMemoryObjectStore` (`stat`), `cluster/objectstore/ObjectStoreLayout.java` (`leaseKey`), `cluster/objectstore/ObjectStorePublisher.java` (call the guard before the `LATEST` CAS)

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
- **Refresh: writer-state alone is not a base for an incremental run.**
  - A new owner first materialises the `LATEST` generation into its local index dir: the Lucene files, the ArcadeDB graph generation plus the graph pointer, `refs` and the format stamp.
  - It reuses the reader download path: `ObjectStoreReplicaSource` plus `ManifestValidation`, with manifest CRCs checked.
  - Only then does it overlay `file-state.json` and the embedding cache from the backup.
  - If the backup's generation is older than `LATEST`, the overlay still applies. The file-state then marks the files changed since that generation as dirty, and they re-embed. That costs more than restoring at `LATEST`, but it is correct.
  - Add a test: `restoreMaterialisesLatestThenOverlaysState`.
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
- **Refresh:**
  - **`WriterRegistry`** shares the bucket-clock liveness code with `ReaderRegistry`: one base class or helper, extracted in Task 2. Its layout keys are `writerKey(node)` and `writersPrefix()` under `catalog/writers/`.
  - **Graceful stop** registers with `ShutdownSequence` so it runs inside `cluster.drainSeconds` and before model calls stop. An index run that can't finish in that budget aborts at a safe point, the same abort as Task 4b, and is never half committed.
  - **`ClusterRuntime`** gets a `WRITER` branch with `workQueue: true`. `ALL` and `READER` stay byte-for-byte unchanged.

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

### Task 4b: Writer disk guard (user request, 2026-11-17)

A writer must never run out of disk. Running out mid-index can corrupt a Lucene commit, an ArcadeDB graph or the embedding cache, and on a shared node it also starves other processes.

**Files:** `cluster/writer/DiskGuard.java`, `config/ClusterConfig.java` (`writer.disk.minFreeBytes`, default 5 GiB; `writer.disk.minFreePercent`, default 10; `writer.disk.checkSeconds`, default 15), plus the work loop (Task 4), the writer heartbeat and `/health`

**Interfaces:**
- `DiskGuard(Path dataDir, …)` watches the file store of the writer's index directory (and its source checkout and model cache when those sit on other file stores). Low means free space is below either threshold.
- `boolean admits(EstimatedRun)`: before starting a shard's index run, the guard estimates the run's peak extra space and refuses if the run would cross the threshold. The estimate is about 2× the shard's current index size for a full run (a full re-index writes a new generation next to the old one), the changed files' share for an incremental run, plus the graph generation and the spool.
- While low, the writer:
  - starts no new index runs;
  - finishes or aborts the current run at a safe point (abort deletes the uncommitted segments and the partial graph generation; never a half commit);
  - releases leases of shards it cannot index, so a writer with space can take them (Task 4's balancer skips low writers);
  - keeps renewing leases for shards whose last publish is still served.
- It recovers automatically when space returns above the threshold plus a hysteresis margin (default 1 GiB) to avoid flapping.
- **Heartbeat and status:** the writer heartbeat gains `disk: {freeBytes, totalBytes, low: bool}`. Task 5 adds a health entry `writer X low on disk` and a proposal `add writer capacity / free disk on X`. The Cluster page (Task 6) shows each writer's free disk and marks low writers.
- Before deleting anything, garbage collection of old local generations, retired graph generations and stale spools runs first when the disk is low, within the retention rules.

- [ ] **Tests** (with an injectable `FileStore` / free-space supplier):
  - `refusesRunThatWouldCrossThreshold`
  - `lowDiskAbortsAtSafePointWithoutHalfCommit`: the previous generation stays intact and publishable.
  - `lowWriterReleasesLeasesAndAnotherTakesThem`
  - `recoversWithHysteresis`
  - `gcRunsBeforeRefusing`
  - `heartbeatReportsDisk`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `cluster: writer disk guard`.

### Task 5: `/cluster/status`, proposals, events and webhook

**Files:** `cluster/status/ClusterStatus.java` (records), `StatusBuilder.java` (a pure function of the catalog, heartbeats and config), `Proposals.java` (rules with hysteresis state kept in a small object in the bucket, so every node agrees), `StatusEvents.java` (SSE plus the webhook with retry and backoff), `web/WebServer.java` routes, `config` (`status.targets.{warmP95Millis, qpsPerReader, backlog}`, `status.webhookUrl`)

**Interfaces:**
- **Status JSON:**
  - `layout: {shards[{shard, specVersion, keys, docCount, sizeBytes, generation, ageSeconds, writer, readers[], backlog}], writers[...], readers[...]}`, with reader entries embedding the `readerCache` fields.
  - **Indexing backlog (user request):** each shard's `backlog` is `{sourceRevision, publishedRevision, pending: bool, oldestUnpublishedChangeAgeSeconds}`. Each writer's `indexing` is `{shard, phase: parse|embed|commit|publish, progress: 0..1, startedAt}` or null. A cluster-level `backlogSummary` gives `{shardsPending, oldestPendingAgeSeconds}`. Writers report these fields in their heartbeats (Task 4), and `SourceCheckout.changedSince` supplies the revisions (Task 1).
  - `health[]`.
  - `proposals[{id, kind, target, reason, metrics, action, since}]`.
  - Lists are sorted. A proposal id is `sha1(kind + target)`.
- **Refresh:**
  - **Warm p95 doesn't exist yet.**
    - Extend `LoadCounter` with a request-latency window: a fixed-bucket histogram over the last 60 s, with no per-request allocation.
    - Add `warmP95Millis` to `ReaderHeartbeat`. The field is nullable: a heartbeat from an older reader has none, and that reader's "add readers" rule falls back to QPS only.
  - **Reader entries copy the `ReaderHeartbeat` fields by their names** (`node, advertiseUrl, shards, state, degradedShards, readerCache, inFlight, qps, startedAt`), plus `heartbeatAgeSeconds`. `/cluster/readers` stays unchanged.
  - **Shard `docCount`, `sizeBytes`, `generation` and `writer`** come from the `LATEST` manifest (`docCount`, `sizeBytes`, `writerNode`).
  - **Routes:** `/cluster/status` and `/cluster/events` are served on every role.
    - On `role: all` the status is built from local state: one node holding every shard, as in `singleNodeStatusShowsOneNode`.
    - The status JSON never contains `cluster.token` or object-store credentials. Extend `ClusterTokenSecrecyTest` to check that.
  - **Router:**
    - `RouterCommand`'s allowlist gains `GET /cluster/status` and `GET /cluster/events`.
    - `/cluster/events` is a long-lived SSE stream. It must be exempt from the total deadline and the 64 MiB body cap, and is streamed through without buffering, with an idle timeout of 2 × the SSE keep-alive instead. Test this: `routerStreamsEventsPastTotalDeadline`.
  - **Readers' `CachedCatalog` age** goes into each reader entry as `catalogAgeSeconds`. Phase 3's gate saw an age warning on the first query after an idle spell, and the status makes that visible.
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

**Refresh:** the stack is vanilla JS with D3 v7 in a single `app.js`, plus `index.html` and `style.css`. Views switch through `btn-view` header buttons.
- Add a "Cluster" button and a view container to them.
- Put the mapping from status to view in its own file, `web/cluster.js`, as pure functions, loaded by `index.html`.
- There is no JS test runner, and none should be added.
- The Java test serves the page and its assets in the style of `WebServerHealthTest`. It checks that the golden status fixture has every field `cluster.js` reads, with that field list kept in one place.
- Rendering is checked by hand in a browser, as in the step below.

**Files:** a new view in `src/main/resources/web/` (follow its existing stack and styling; read it first), a nav entry, and README screenshots or text

**Behaviour:**
- **Layout diagram** (SVG): writer boxes, a bucket box and reader boxes. Each writer lists the shards it holds, and each reader lists the shards it serves. Shard chips are coloured current, lagging or lease-expired. Lines show publish and sync.
- **Node table:** role, shards, docs, size, cache bytes of cap, hit rate, QPS, warm p95, heartbeat age.
- **Sharding table:** spec version and keys; one row per shard.
- **Scale panel:** live writer and reader counts and the proposals in force, each with its reason and metrics.
- **Indexing backlog panel (user request):** shards with unpublished source changes, sorted oldest first, showing the source revision against the published revision and how long each change has waited. Each writer's current job is shown with its phase and a progress bar. A shard with a pending change is marked on its chip in the layout diagram.
- Data comes from `/cluster/status`. `/cluster/events` triggers a refetch, with a 10 s poll as fallback.
- On a single node, the page shows one node holding every shard.

- [ ] **Tests:**
  - A frontend unit test for the status-to-view mapping, if the stack has a test runner. Otherwise a Java test that the page and its assets are served and that the status fixture renders without script errors, using the existing web test harness.
  - A golden JSON fixture of `/cluster/status` used by both.
  - The backlog panel renders pending shards oldest first and a writer's job progress from the fixture.
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
   - **Pass:** each source change appears in the backlog (`pending: true`) within one writer poll interval, and clears within one heartbeat of its publish.
4. **Proposals.** Drive the reader load above its target for the configured window (shortened for the gate through config).
   - **Pass:** an "add readers" proposal appears on `/cluster/status`, is streamed on `/cluster/events` and is delivered to a local webhook sink.
5. **Disk guard (Task 4b).** Run one writer on a small filesystem (a loop-mounted image or a tmpfs sized just above the threshold) and index into it.
   - **Pass:** the writer refuses or aborts before crossing the threshold, the shard's last generation stays served, another writer takes the shard's lease, `/cluster/status` shows the low-disk entry, and the low writer recovers once space is freed.
6. **Safety.** Marker check; kill processes by PID; leave :7171 alone.
7. **If it passes,** flip the `writer.workQueue` default to `true` for `role: writer` and document it.

- [ ] Write the scripts.
- [ ] Run the gate and record the numbers.
- [ ] Investigate any failure before tuning; never lower a bar silently.
- [ ] **Commit** `cluster: Phase 4 gate scripts and results`.
