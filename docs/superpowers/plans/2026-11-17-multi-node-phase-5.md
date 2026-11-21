# Multi-node Phase 5: Peer Pull Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A cluster can run with no bucket. Readers fetch the same manifests and content-named files directly from the writer that owns a shard, over HTTP with `Range` support.

**Architecture:** A new `ReplicaSource`, `PeerReplicaSource`, plus a writer replication endpoint. They reuse the Phase 1 manifest format and the Phase 2 block cache unchanged.
- **Writers are discovered dynamically (user decision, 2026-11-17)**, not listed in a static map. A reader resolves `cluster.peers.discovery` periodically (DNS name, or seed URLs) and asks each writer which shards it owns via `/replication/catalog`.
- Readers long-poll `latest`.
- A reader may fetch a file from a peer reader that already holds the generation. The manifest checksums verify those bytes.

**Tech Stack:** Java 25, Javalin (writer endpoint, `Range`), JDK `HttpClient`, TLS through the JDK keystore, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md`, section "Peer-pull backend", and Phase 5 in "Phases and gates". The live doc is rev 61.

**Base:** branch `feat/cluster-phase-5` from `feat/cluster-phase-4` after Phase 4's final review. The refresh below assumes Phases 3 and 4 are present.

**Refreshed 2026-11-21** against Phase 3 (788882d) and the refreshed Phase 4 plan (3c84919). The original plan said Phase 5 depends only on Phases 0–2. With Phases 3 and 4 built that no longer holds: they keep membership and status state in the bucket too. Task 2b and the rulings below cover that.

## What exists, and what still needs a bucket (refresh, 2026-11-21)

**Replication: ready as is**
- `ReplicaSource` already has `latest`, `manifest`, `download`, `downloadRange`, `openRange` and `synonyms()`. `PeerReplicaSource` implements all six.
- `ObjectStoreReplicaSource` reads `catalog/synonyms.txt` through `synonyms()`, so synonyms need no separate path.
- `Backends.create` switches on `BackendKind {LOCAL, OBJECT_STORE}`. Add `PEER_PULL("peer-pull")` there, and update the error message.
- `ReplicaSyncer` runs `syncOnce` on a fixed delay (`pollInterval`). Long-poll mode replaces that loop for peer sources only.

**Still in the bucket, and what replaces it in peer mode:**

| Bucket use today | Who reads it | Replacement in peer mode |
|---|---|---|
| `catalog/shards.json` (`ObjectStoreCatalog`, Phase 4 `source` field) | readers (`CachedCatalog`), writers | `PeerCatalog`, merged from every writer's `/replication/catalog` (Task 2) |
| `catalog/readers/<node>.json` (`ReaderRegistry`, bucket-clock liveness) | coordinators (`ShardRouter`), `pharos router` (`ReaderRegistry.observer`), Phase 4 status | Writers keep a reader registry fed by `POST /replication/readers`, judged by the writer's own clock. Coordinators and the router merge `GET /replication/readers` from every discovered writer (Task 2b) |
| `catalog/synonyms.txt` | readers via `ReplicaSource.synonyms()` | `GET /replication/synonyms`. When writers disagree, the newest one by content hash and mtime wins, and the conflict is reported like a shard claim conflict (Task 1) |
| Phase 4 `catalog/writers/`, leases, `writer-state/` | writers, status | Not used: there is no takeover in peer mode (spec). Each writer reports itself in `/replication/catalog`. `WriterStateStore` falls back to local disk |
| Phase 4 proposal hysteresis object | status on every node | Kept by the live writer with the lowest node id and served at `GET /replication/status-state`. Ruling P5-R2 |

**Process model.** Today a writer can be just `pharos index` runs. In peer mode the writer must also run a long-lived `pharos web` (role `writer`) to serve `/replication/*`. Its local index directory is what it serves.

**Web stack.** The web server is Javalin 6 (Jetty), with no TLS today. TLS goes through Javalin's Jetty server config, adding an `SslContextFactory.Server` connector from `cluster.tls`. Don't add the community SSL plugin. The `ShardClient` and router `HttpClient`s get an `SSLContext` from the same truststore.

**Shutdown.** `ShutdownSequence` gains a writer step: stop accepting `/replication` transfers, let in-flight ranges finish within `drainSeconds`, then exit.

**Rulings (refresh):**
- **P5-R1.** Phase 4's `/cluster/status` works in peer mode. It is built from `PeerCatalog`, the writers' reader registries and the writers' catalog entries. Fields that only exist with leases (`lease`, takeover) are null. *Cost if wrong:* the Cluster page shows less in peer mode.
- **P5-R2.** The proposal hysteresis state lives on the lowest-id live writer. If that writer dies, the state restarts on the next one, and proposals may re-appear after their wait window. *Cost if wrong:* a proposal delayed once per writer loss.
- **P5-R3.** Every new route (`/replication/catalog`, `/replication/readers`, `/replication/peers`, `/replication/synonyms`, `/replication/status-state`) requires `X-Pharos-Cluster-Token`, like the file routes.

## Global Constraints

- **Phase 5 gate (spec):** "Same Phase 1–2 gates with no bucket; kill the writer and readers keep serving their cached generation."
- **Writer endpoint (spec):** `GET /replication/<shard>/latest`, `/manifests/gen-N.json` and `/files/<name>` with `Range` support, served from the writer's local index directory. Files of retained generations stay on disk under GC's retention rule: the last 3 generations plus 1 hour.
- **Reader (spec):** `PeerReplicaSource` implements `latest`, `manifest` and `open`. `LocalCache`, warming and the reopen path are unchanged. "Readers long-poll `latest` instead of polling every 10 s."
- **Catalog and leases (spec, amended):**
  - The spec's static writer → shard map is replaced by dynamic discovery (user decision, 2026-11-17). Each writer still serves `/replication/catalog`, listing the shards it owns, and readers merge these into a live catalog.
  - Readers register with the writers they pull from.
  - "there is no automatic writer takeover in this mode."
- **Writer load (spec):** "cap concurrent transfers per writer, and let readers fetch files from a peer reader that already holds the generation (manifest checksums verify them)."
- **Writer down (spec):** readers keep serving the generation they hold. Files not yet cached fail, and so do queries that touch that shard. `WriterStateStore` falls back to local disk.
- **Security (spec):** "TLS plus a shared cluster token on the replication endpoint." Reuse `cluster.token` from Phase 3.
- **Config:**
  - `cluster.backend: peer-pull`.
  - `cluster.peers.discovery`: either `dns:<name>[:port]` (every A/AAAA record is a writer, such as a Kubernetes headless service) or `seeds:<url>,<url>` (each seed's `/replication/peers` lists the writers it knows; writers gossip their peer lists).
  - `cluster.peers.refreshSeconds` (default 15): how often a reader re-resolves discovery and re-reads each writer's catalog. A writer missing for 3 refreshes is dropped.
  - `cluster.peers.maxTransfersPerWriter` (default 8).
  - `cluster.peers.longPollSeconds` (default 30).
  - `cluster.tls.{keystore, truststore, password env}`.
  - The `object-store` backend stays unchanged.
- **Content-named keys:** the manifest's `key` for each file is already content-named (`lucene/<file>@<crc>`). In peer mode, `/files/<key>` serves those same names from the writer's local generation, so the Phase 2 block cache keys stay valid.
- **Isolation:** tests and the gate never touch `~/.pharos` or :7171. `./pharos` stays in sync.

**Out of scope:** writer takeover in peer mode (the spec says there is none), and mixing bucket and peer sources for one shard.

## Review Focus

0. **Two discovered writers claim the same shard** (misconfiguration, or a writer moved without stopping the old one). Readers sync from the writer with the higher `LATEST` generation, report the conflict in `/health` and the Cluster status as `shard X claimed by A and B`, and never alternate between them on each refresh. Test owned by Task 2.

1. **The writer prunes a generation while a reader is mid-fetch of one of its files.** Retention keeps files of the last 3 generations plus 1 hour on the writer. A request for a pruned file gets 410 with the current generation, and the reader re-syncs to `LATEST` instead of retrying forever. Test owned by Task 1.
2. **A peer reader serves a corrupted or stale file.** The checksum check against the manifest rejects it, the reader falls back to the writer, and the peer is marked suspect. Test owned by Task 3.
3. **The writer is down at reader start.** The reader serves the generations it already has on disk (the Phase 1–2 restart path). Shards it has never synced report "unavailable" with a clear message. The reader keeps long-polling with backoff. Test owned by Task 2.
4. **Too many cold readers hit one writer.** Transfers above `maxTransfersPerWriter` get 503 with `Retry-After`, the readers back off with jitter, and nothing fails permanently. Test owned by Task 1.
5. **A wrong or missing token, or no TLS when `cluster.tls` is configured.** The endpoint answers 401, or the connection is refused, and never serves bytes. Test owned by Task 1.

---

### Task 1: Writer replication endpoint

**Files:** `cluster/peer/ReplicationEndpoint.java` (Javalin routes on writers), `cluster/peer/TransferLimiter.java`, writer-side retention (keep files of retained generations; reuse the GC retention numbers), `web/WebServer.java` wiring, TLS configuration for the web server

**Interfaces:**
- `GET /replication/catalog` → `{writer, shards[{shard, latest}]}`.
- `GET /replication/<shard>/latest?wait=<s>&after=<gen>` long-polls until a generation newer than `after` exists, or until timeout. It returns `{"generation": N, "manifest": "gen-N.json"}`.
- `GET /replication/<shard>/manifests/gen-N.json`.
- `GET /replication/<shard>/files/<key>` with `Range`. It answers 206 with `Content-Range`, 410 for a pruned file, and 503 with `Retry-After` above the transfer cap.
- Every route requires `X-Pharos-Cluster-Token`.
- **Refresh:**
  - Also serve `GET /replication/synonyms`. It returns the bytes plus an `ETag` of their SHA-256, and 404 when there are none.
  - When two writers' synonyms differ, readers take the newest and report `synonyms differ between A and B` in `/health` and status. Test: `synonymsConflictReported`.
  - Writers in peer mode run `pharos web --role writer` to serve these routes.
  - TLS goes through the Javalin/Jetty connector, as in the refresh section above.

- [ ] **Tests:**
  - `latestLongPollsUntilPublish`
  - `rangeServesExactBytes`
  - `prunedFileIs410WithCurrentGeneration`: Review Focus 1.
  - `transferCapReturns503RetryAfter`: Review Focus 4.
  - `tokenRequired`
  - `tlsWhenConfigured`: Review Focus 5. Uses a self-signed test keystore.
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: writer replication endpoint`.

### Task 2: `PeerReplicaSource`, writer discovery and the peer-pull backend

**Files:** `cluster/peer/PeerReplicaSource.java`, `PeerDiscovery.java` (DNS re-resolution with JDK `InetAddress`, ignoring the JVM DNS cache TTL via `networkaddress.cache.ttl` or a direct lookup each refresh; seed URLs with gossip via `/replication/peers`), `PeerCatalog.java` (merges every discovered writer's `/replication/catalog`; conflict handling), `cluster/Backends.java` (`peer-pull`), `cluster/ReplicaSyncer.java` (long-poll mode), `config/ClusterConfig.java`

**Interfaces:**
- `PeerReplicaSource implements ReplicaSource`: `latest`, `manifest`, `download`, `downloadRange` and `openRange` over the endpoint, with backoff and jitter on 503 and connection errors.
- In peer mode the syncer long-polls each shard's writer instead of sleeping for `pollInterval`.

- [ ] **Tests:**
  - `writersDiscoveredByDnsAndSeeds`: a fake resolver returns 1, then 3, then 2 writers; the catalog follows within one refresh.
  - `writerJoinsAndLeaves`: a new writer's shards become syncable; a writer gone for 3 refreshes is dropped while its cached generations keep serving.
  - `conflictingClaimsPickHigherGenerationAndAreReported`: Review Focus 0.
  - `roundTripPublishSyncSearch`: an in-JVM writer and reader with no `ObjectStore`.
  - `lazyModeWorksOverPeer`: the Phase 2 block cache works over HTTP ranges.
  - `writerDownAtStartServesOnDiskGenerations`: Review Focus 3.
  - `writerDownMidRunKeepsServingCachedAndFailsColdReadsClearly`
  - `objectStoreBackendUnchanged`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `cluster: peer-pull backend`.

### Task 2b: Membership without a bucket (refresh, 2026-11-21)

**Files:**
- `cluster/peer/PeerReaderRegistry.java`: the writer side, fed by `POST /replication/readers`.
- `cluster/membership/MembershipSource.java`: a new interface over "live readers", implemented by the bucket `ReaderRegistry` and by a peer observer that merges `GET /replication/readers` from every discovered writer.
- `ShardRouter`, `RouterCommand`, `ClusterRuntime`: take a `MembershipSource` instead of a `ReaderRegistry`.
- `cluster/peer/PeerStatusState.java`: the hysteresis holder from Ruling P5-R2.

**Interfaces:**
- A reader's heartbeat body is the existing `ReaderHeartbeat` JSON, unchanged. It is POSTed to every discovered writer every `heartbeatSeconds`.
- A writer drops a reader whose last POST is older than the same liveness window the bucket registry uses, measured by the writer's own clock.
- The merged view takes a reader's newest heartbeat across writers. A reader seen by any live writer is live.

- [ ] **Tests:**
  - `peerMembershipMatchesBucketMembership`: the same heartbeats produce the same live set and routing decisions through both sources.
  - `readerVisibleWhileOneWriterDown`
  - `routerUsesPeerMembership`
  - `drainingReaderLeavesPeerMembership`
  - `statusStateMovesToNextWriter`: Ruling P5-R2.
  - `objectStoreMembershipUnchanged`
- [ ] Implement.
- [ ] Run the focused tests, then `mvn test`.
- [ ] **Commit** `cluster: membership and status state without a bucket`.

### Task 3: Fetch from peer readers

**Files:**
- `cluster/peer/PeerFileSource.java`: a reader also serves `/replication/<shard>/files/<key>` for files it holds fully, either eager files or files whose blocks are all cached.
- Reader registration with writers: `POST /replication/readers`, a heartbeat that lists the generations held.
- The writer's `/replication/<shard>/latest` response lists peers that hold that generation.

**Interfaces:** before fetching from the writer, a reader tries up to 2 peers that hold the generation. Every peer-fetched file or block is verified: whole files against the manifest CRC, and blocks against the Phase 4 tail-footer check plus a per-block length check. Any mismatch marks the peer suspect and falls back to the writer.

- [ ] **Tests:**
  - `secondReaderFetchesFromFirst`: counts writer transfers.
  - `corruptPeerFileRejectedAndWriterUsed`: Review Focus 2.
  - `stalePeerGenerationIgnored`
  - `peerFetchHonoursToken`
- [ ] Implement.
- [ ] Run the focused tests.
- [ ] **Commit** `cluster: readers fetch from peer readers`.

### Task 4: Phase 5 gate (no bucket)

**Files:** `scripts/cluster-gate-phase5.sh`, a reuse of the Phase 1 and 2 gate scripts with `--backend peer-pull`, and `docs/superpowers/gates/<date>-phase-5.md`

**Gate procedure:**
1. Re-run the Phase 1 gate with no MinIO, using the same pass criteria. Readers find the writer only through `cluster.peers.discovery` (seed URL), never a static map.
   - Add a second writer for a new project mid-run: readers discover it and serve its shard within 2 refreshes, with 0 failed queries.
2. Re-run the Phase 2 gate the same way.
   - **Refresh:** also re-run the Phase 3 fan-out checks with no bucket: keyword ≥ 98% identical top-10, and vector and hybrid under the Phase 3 rules. Coordinators and `pharos router` find readers only through peer membership (Task 2b). The pass criteria are unchanged.
3. **Writer down.** With two readers serving pharos and lucene, `kill -9` the writer.
   - **Pass:** queries keep working for 2 minutes at about 10 QPS, with 0 failures.
   - **Pass:** a reader restarted while the writer is down serves its on-disk generations.
   - **Pass:** when the writer comes back, readers resume long-polling and pick up a new publish within 5 s.
4. **Writer load.** Start 6 cold readers at once.
   - **Pass:** the writer never exceeds `maxTransfersPerWriter`, and every reader is serving within 3 × the single-reader cold time.
   - Report how many bytes came from peers versus the writer.
5. **Safety.** Marker check; kill processes by PID; leave :7171 alone.

- [ ] Write the scripts.
- [ ] Run the gate.
- [ ] Record the results.
- [ ] **Commit** `cluster: Phase 5 gate scripts and results`.
