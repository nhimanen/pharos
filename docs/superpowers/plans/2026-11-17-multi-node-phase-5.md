# Multi-node Phase 5: Peer Pull Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A cluster can run with no bucket. Readers fetch the same manifests and content-named files directly from the writer that owns a shard, over HTTP with `Range` support.

**Architecture:** A new `ReplicaSource`, `PeerReplicaSource`, plus a writer replication endpoint. They reuse the Phase 1 manifest format and the Phase 2 block cache unchanged.
- **Writers are discovered dynamically (user decision, 2026-11-17)**, not listed in a static map. A reader resolves `cluster.peers.discovery` periodically (DNS name, or seed URLs) and asks each writer which shards it owns via `/replication/catalog`.
- Readers long-poll `latest`.
- A reader may fetch a file from a peer reader that already holds the generation. The manifest checksums verify those bytes.

**Tech Stack:** Java 25, Javalin (writer endpoint, `Range`), JDK `HttpClient`, TLS through the JDK keystore, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md`, section "Peer-pull backend", and Phase 5 in "Phases and gates". The live doc is rev 61.

**Base:** branch `feat/cluster-phase-5` from the latest merged multi-node phase. Phase 5 depends only on Phases 0–2 (`ReplicaSource`, `LocalCache`, `BlockCache`). It can run before or after Phases 3–4. **Refresh this plan before execution:** check the `ReplicaSource` interface (Phase 2 added `downloadRange` and `openRange`) and how `ClusterRuntime` chooses a backend.

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
