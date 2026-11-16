# Multi-node Phase 2: On-demand Warming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reader serves a generation without copying it. Opening a generation fetches only the manifest and the small Lucene files. Everything else is read through a size-capped, block-level local cache, filled by ranged object-store reads when a query first touches it.

**Architecture:**
- A read-only Lucene `Directory` (`RemoteDirectory`) is built from a generation's manifest. Small files are local. Large files are `CachingRemoteIndexInput`s that read fixed-size blocks through a shared on-disk `BlockCache`.
- The block cache has LRU eviction under a byte cap. Blocks are keyed by content-named object key, so generations share them for free.
- `LuceneIndexer` opens directories through a factory, and reader nodes plug the remote directory in. Phase 1's full copy remains as `readerMode: full`.

**Tech Stack:** Java 25, Lucene 10.4 (`Directory`, `IndexInput`, `RandomAccessInput`, `MemorySegment`), AWS SDK v2 S3 (ranged `GetObject`), JUnit 5, MinIO (gate).

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md` (snapshot of https://claude.ai/artifact/F1XtggNDsoToZMc64tSeKh rev 58; the live doc is at rev 61, which adds the Phase 4 "Cluster page (web UI)" section and changes nothing in Phase 2), sections "On-demand warming (reader)" and "Phases and gates". The [architecture overview](https://claude.ai/artifact/Nc3hmL3i1RC5zeVwvA78Wj) describes Phases 0–1 as built.

**Base:** branch `feat/cluster-phase-2` (worktree `../pharos-cluster2`), created from `feat/cluster-phase-0-1` at `f9a5f4c`. Phases 0–1 ship as the first multi-node release, without this branch.

## Global Constraints

- **Phase 2 gate (spec):** "Cold reader's first query p95 within a target (e.g. < 3 s for lucene); warm p95 within 10% of local; cache cap respected." This plan fixes the target at **< 3 s for lucene** against MinIO on the same host.
- **Warming model (spec):** "opening a generation downloads the manifest and the small files (`segments_N`, `.si`, `.fnm`) eagerly; big files … download when first touched."
- **Block-level reads (spec):** "fetches fixed blocks (e.g. 8 MB) with ranged GETs and keeps them in the local cache … HNSW graphs and term dictionaries are prefetched." Default block size: **8 MiB**.
- **Eviction (spec):** "LRU over blocks". See Deviations for the pinning rule.
- **Core rule (spec):** "indexing and search code never knows where bytes live." Only `LuceneIndexer`'s directory factory and the cluster package know about `RemoteDirectory`.
- **Single node is unchanged:** `role: all` with the `local` backend never constructs any Phase 2 class. `SingleNodeGoldenTest` must stay byte-identical.
- **Formats:** no `INDEX_FORMAT` change and no manifest format change. Phase 2 reads Phase 1 manifests as they are (`ShardManifest.FORMAT_VERSION` stays 1).
- **Config:** under `cluster.reader` in `config.json`.
  - Names and defaults: `mode` (`"lazy"` | `"full"`, default `"lazy"`); `cacheMaxBytes` (default `8 GiB`); `blockSizeBytes` (default `8 MiB`); `eagerMaxBytes` (default `1 MiB`); `prefetchExtensions` (default `["tip","tmd","tim","vex","vem"]`); `prefetchTimeoutSeconds` (default `30`); `maxConcurrentFetches` (default `16`).
  - Validation: `blockSizeBytes` is a power of two between 64 KiB and 64 MiB, and `cacheMaxBytes ≥ 4 × blockSizeBytes`. A violation fails at config load and names the field.
- **One reader process per cache dir:** enforced by the existing `SyncLock`.
- **Isolation:** tests and the gate never touch `~/.pharos` (`pharos.home`) or the user's daemon on :7171. The Python client `./pharos` stays in sync with any new command or flag (CLAUDE.md).

**Deviations from the spec text, decided here:**
- **Pinning.** The spec says eviction never evicts "files of a generation that has open searchers". With block-level reads, that would pin the whole active shard and make the cap meaningless. Here, any block may be evicted and is fetched again on demand. Only blocks with a **live read lease** (an in-flight `IndexInput` access) are never evicted. Eager small files live in the generation dir, outside the block cache, and stay until the generation is pruned. This keeps the spec's intent (never pull bytes from under a reader) and makes the cap real.
- **Compact graph.** The spec downloads it "when a graph tool or graph boost first needs it". Graph boost runs on every search, so the first need is the first query anyway. The graph (about 2 MB for lucene) is therefore downloaded at materialisation, as in Phase 1.
- **Prefetch runs before activation.** The prefetch extensions are fetched before a new generation is swapped in, bounded by `prefetchTimeoutSeconds`. After the timeout, the reader activates anyway and logs a WARN. The previous generation serves meanwhile. On a first start there is nothing to serve until activation, so the timeout bounds cold start.

## Review Focus

1. **The bucket loses an object that a cold read needs** (GC'd generation, wrong credentials, network down). The query fails with an error naming the shard, generation and object key; it never returns partial or wrong results. The reader stays up and the next query retries. Test owned by Task 3.
2. **A block file on disk is truncated or corrupt** (crash during write, disk full, manual tampering). Blocks are written to a temp file and moved into place atomically. A block whose length is wrong is deleted and fetched again, and a failed write still serves the read from the fetched bytes with a WARN. Test owned by Task 2.
3. **The working set is larger than the cache cap.** Results stay identical to a full copy, the on-disk block bytes never exceed `cacheMaxBytes` plus one in-flight block per fetch slot, and the reader logs a thrash WARN with the hit rate at most once a minute. Tests owned by Task 2 (cap) and Task 6 (gate run with a cap at 50% of the shard).
4. **Many concurrent queries miss the same block.** Exactly one ranged GET is issued per block (single-flight), and the others wait for it. Test owned by Task 2.
5. **A generation swap while a query is mid-read on the old generation's lazy file.** The old `RemoteDirectory` stays readable until its `DirectoryReader` closes, and evicted blocks of the old generation are fetched again if needed. Pruning the old generation dir never breaks an in-flight query. Test owned by Task 4.

## File Structure

- `cluster/objectstore/ObjectStore.java`, `S3ObjectStore.java`, test `InMemoryObjectStore.java`, `ObjectStoreContract.java`: ranged download.
- `cluster/ReplicaSource.java`, `objectstore/ObjectStoreReplicaSource.java`: `downloadRange`.
- New package `cluster/warm/`:
  - `BlockKey.java`: the record for a block's identity.
  - `BlockCache.java`: on-disk LRU, cap, leases, single-flight, recovery scan, stats.
  - `CachingRemoteIndexInput.java`: Lucene `IndexInput` over blocks.
  - `RemoteDirectory.java`: read-only Lucene `Directory` over a manifest.
  - `WarmStats.java`: counters.
- `indexer/LuceneDirectoryFactory.java` (new) and `indexer/LuceneIndexer.java`: the factory hook.
- `config/ClusterConfig.java`: `ReaderConfig` nested class.
- `cluster/ReplicaSyncer.java`, `cluster/ClusterRuntime.java`: lazy materialisation, prefetch, factory wiring, stats exposure.
- `cli/IndexStatusCommand.java`, `web/WebServer.java` (`/health`): cache stats for operators and the gate.
- `scripts/cluster-gate-phase2.sh`, `scripts/cluster_gate_phase2.py`, `docs/superpowers/gates/<date>-phase-2.md`: the gate.

---

### Task 1: Ranged reads from the object store

**Files:**
- Modify: `cluster/objectstore/ObjectStore.java`, `S3ObjectStore.java`, `cluster/ReplicaSource.java`, `cluster/objectstore/ObjectStoreReplicaSource.java`
- Test: `objectstore/InMemoryObjectStore.java`, `ObjectStoreContract.java` (shared by the in-memory and MinIO tests), `S3ObjectStoreMinioTest.java`

**Interfaces:**
- Produces:
  - `void ObjectStore.downloadRange(String key, long offset, long length, Path target) throws IOException`. It writes exactly `length` bytes, using a temp file in the same directory and an atomic move. It throws `NoSuchFileException` for a missing key, and `EOFException` when `offset + length` exceeds the object size. A short body throws `IOException` and leaves no target behind.
  - `void ReplicaSource.downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException`, which delegates with `file.key()`.

- [ ] **Step 1: Contract tests in `ObjectStoreContract`:**
  - `rangeReturnsExactBytes`: a 10 000-byte object, ranges `[0,1)`, `[4095,4097)` and `[9990,10)`, compared byte for byte.
  - `rangePastEndIsEof`
  - `rangeOfMissingKeyIsNoSuchFile`
  - `zeroLengthRangeWritesEmptyFile`
  - `failedRangeLeavesNoTarget`: the in-memory store is injected to fail mid-body.
- [ ] **Step 2: Run** `mvn -q test -Dtest='InMemoryObjectStoreTest'`. Expect a compile failure.
- [ ] **Step 3: Implement.** S3 uses a `Range: bytes=<offset>-<offset+length-1>` header and checks `Content-Length`. Zero length writes an empty file without a request.
- [ ] **Step 4: Run** the in-memory contract tests. They pass. Then run the MinIO contract (`S3ObjectStoreMinioTest`) against the scratch MinIO from the Phase 1 gate (`MINIO_BIN=/home/nhimanen/.cache/pharos-gate/minio`, the `PHAROS_TEST_S3_*` environment variables). Expect 0 failures.
- [ ] **Step 5: Commit** `cluster: ranged object-store reads`.

### Task 2: On-disk block cache

**Files:**
- Create: `cluster/warm/BlockKey.java`, `BlockCache.java`, `WarmStats.java`
- Test: `cluster/warm/BlockCacheTest.java`

**Interfaces:**
- Produces:
  - `record BlockKey(String objectKey, long index)`.
  - `BlockCache(Path dir, long maxBytes, int blockSize, int maxConcurrentFetches)`.
  - `BlockCache.Lease acquire(BlockKey key, long blockLength, BlockFetcher fetcher) throws IOException`. `blockLength` is the block's true length, which can be short for the last block.
  - `interface BlockFetcher { void fetch(BlockKey key, long offset, long length, Path target) throws IOException; }`.
  - `Lease implements AutoCloseable`, with `MemorySegment segment()` (read-only mapping of the block file) and `long length()`.
  - `WarmStats stats()`, with hits, misses, bytesFetched, evictions, bytesOnDisk and leasesOpen.
- On-disk layout: `<dir>/<first 2 hex of sha256(objectKey)>/<sha256(objectKey)>/<index>.blk`. Blocks are written via `<index>.blk.tmp` and an atomic move.
- At construction, the cache scans `dir`. It deletes `*.tmp`, deletes blocks whose size can't be a valid block length, and seeds the LRU by file mtime.

- [ ] **Step 1: Write the failing tests.**
  - `missFetchesOnceThenHits`
  - `concurrentMissesSingleFlight`: 16 threads on one key, so `fetcher` is called once.
  - `evictsLeastRecentlyUsedOverCap`: cap of 4 blocks, 6 acquired and released in order, so blocks 0 and 1 are gone and bytes on disk are ≤ cap.
  - `neverEvictsLeasedBlock`: cap of 2, three leases held, so the cap is exceeded only while leased; after release, eviction brings it back under cap.
  - `truncatedBlockOnDiskIsRefetched`
  - `tmpFilesRemovedOnStartup`
  - `restartSeedsLruFromDisk`
  - `fetchFailurePropagatesAndCachesNothing`
  - `diskWriteFailureStillServesRead`: the cache dir is made read-only after construction. The read succeeds, the block isn't cached, and the WARN is counted.
  - `thrashWarnRateLimited`: one WARN per 60 s via an injectable clock.
- [ ] **Step 2: Run** `mvn -q test -Dtest=BlockCacheTest`. Expect a compile failure.
- [ ] **Step 3: Implement.**
  - An access-ordered `LinkedHashMap` under one lock holds the entries.
  - Single-flight uses a per-key `CompletableFuture`. Fetches run on a bounded executor of size `maxConcurrentFetches`, and the caller waits.
  - Mapping uses `FileChannel.map(READ_ONLY)` into an `Arena.ofShared()` that is closed when the entry is evicted and has no leases.
  - Eviction runs after each insert until the cache is at or under the cap, or only leased entries remain.
- [ ] **Step 4: Run** the tests. They pass.
- [ ] **Step 5: Commit** `cluster: block cache with LRU cap, leases and single-flight fetch`.

### Task 3: Remote Lucene directory and index input

**Files:**
- Create: `cluster/warm/CachingRemoteIndexInput.java`, `cluster/warm/RemoteDirectory.java`
- Test: `cluster/warm/RemoteDirectoryTest.java`

**Interfaces:**
- Consumes: `BlockCache` (Task 2) and `ReplicaSource.downloadRange` (Task 1).
- Produces:
  - `RemoteDirectory(ShardId shard, ShardManifest manifest, Path eagerDir, BlockCache cache, ReplicaSource source)`. It extends `BaseDirectory` with `NoLockFactory`.
    - `listAll` returns the manifest's Lucene names, sorted. `fileLength` comes from the manifest.
    - `openInput` opens an `FSDirectory` input when the file exists in `eagerDir`, and a `CachingRemoteIndexInput` otherwise.
    - Every write or rename method throws `UnsupportedOperationException("read-only remote directory")`.
  - `CachingRemoteIndexInput extends IndexInput implements RandomAccessInput`.
    - It supports `clone`, `slice`, `seek`, `readByte(s)` (including reads across block boundaries), `length`, positional `readByte(long)`/`readShort`/`readInt`/`readLong`, and `prefetch(long offset, long length)`. The prefetch triggers async block fetches.
    - Every block access holds a lease only for the duration of the copy.
    - A read past the end throws `EOFException`.
    - Any fetch failure is rethrown as `IOException`, with a message that names the shard, generation, file name and object key.

- [ ] **Step 1: Write the failing tests.**
  - Index the sample-app fixture with `pharos index` into a temp home, and build a `ShardManifest` of its Lucene files. Upload them with the real publisher to an `InMemoryObjectStore`, or build the manifest by hand from the files.
  - `checkIndexPassesOverRemoteDirectory`: Lucene `CheckIndex` on the `RemoteDirectory` is clean. It reads every byte through the block path, using a 64 KiB block size so files span blocks.
  - `queriesMatchFsDirectory`: BM25 term, phrase and prefix queries; stored-field fetch; doc values; a KNN query on a small embedded fixture with a fake provider. Top docs and scores are identical to the same index opened through `FSDirectory`.
  - `sliceAndCloneAreIndependent`
  - `readsAcrossBlockBoundary`
  - `missingObjectFailsWithShardGenKeyInMessage`: Review Focus 1. After deleting an object from the store, a query that touches it throws with all three named, and a later query that touches other files still works.
  - `writeMethodsAreUnsupported`
- [ ] **Step 2: Run** `mvn -q test -Dtest=RemoteDirectoryTest`. Expect a compile failure.
- [ ] **Step 3: Implement.** Block `i` covers `[i·blockSize, min((i+1)·blockSize, length))`. Slices share the parent's file identity and offset. `prefetch` submits fetches without waiting.
- [ ] **Step 4: Run** the tests. They pass.
- [ ] **Step 5: Commit** `cluster: read-only remote Lucene directory over the block cache`.

### Task 4: Lazy materialisation, directory factory and generation lifecycle

**Files:**
- Create: `indexer/LuceneDirectoryFactory.java`
- Modify: `indexer/LuceneIndexer.java` (`openDirectory` becomes an instance method that calls the factory, default `FSDirectory::open`), `config/ClusterConfig.java` (`ReaderConfig` and validation), `cluster/ReplicaSyncer.java`, `cluster/ClusterRuntime.java`
- Test: `cluster/LazyReaderTest.java`, `config/ClusterConfigTest.java` (extend)

**Interfaces:**
- Consumes: `RemoteDirectory` and `BlockCache` (Tasks 2 and 3).
- Produces:
  - `interface LuceneDirectoryFactory { Directory open(Path luceneDir) throws IOException; }` and `LuceneIndexer.setDirectoryFactory(LuceneDirectoryFactory)`.
  - A lazy generation dir holds exactly `manifest.json`, `index-format.json`, the compact graph, a `.lazy` marker, and `lucene/` with only the eager files: `segments_N`, `*.si`, `*.fnm`, `*.cfe`, and any Lucene file of at most `eagerMaxBytes`.
  - `ClusterRuntime` (role reader, mode lazy) installs a factory. It returns a `RemoteDirectory` when `luceneDir.getParent()` contains `.lazy`, and `FSDirectory` otherwise. Full-mode generations and writer dirs are unaffected.
  - `materialise` in lazy mode does the following in order:
    1. Fetch the eager files and the graph, then write the stamp, the manifest and `.lazy`.
    2. Prefetch the blocks of `prefetchExtensions` files, bounded by `prefetchTimeoutSeconds`.
    3. Atomically move the dir to `gen-N`. Register and activate as in Phase 1.

- [ ] **Step 1: Write the failing tests.**
  - `lazyGenerationHasOnlySmallFiles`: after a sync, the gen dir's `lucene/` holds no file larger than `eagerMaxBytes`. The block cache holds the prefetch files. A search returns the same top-10 as a full-mode reader on the same published generation.
  - `fullModeUnchanged`: `mode: full` behaves exactly as Phase 1 (the existing `ReplicaSyncerTest` cases pass in both modes, via a parameterised fixture).
  - `swapDuringReadKeepsOldGenerationReadable`: Review Focus 5. A query holds an open `IndexInput` on generation 1's lazy file. Generation 2 is published, synced and activated, and generation 1 is pruned. Reads on the old input still succeed, and the old reader closes cleanly.
  - `prefetchTimeoutActivatesAnyway`: an injected slow fetcher. The generation is activated after the timeout, and a WARN is logged.
  - `singleNodeNeverBuildsRemoteClasses`: `role: all` leaves the default factory installed.
  - Config: defaults; `blockSizeBytes` not a power of two is rejected; `cacheMaxBytes < 4 × block` is rejected; an unknown `mode` is rejected. Each message names the field.
- [ ] **Step 2: Run** `mvn -q test -Dtest='LazyReaderTest,ClusterConfigTest,ReplicaSyncerTest'`. Expect them to fail.
- [ ] **Step 3: Implement.** The block cache lives at `<cluster.cacheDir>/blocks`. Restart recovery (Phase 1's `activateRegistered` and its format fallback) applies unchanged to lazy dirs.
- [ ] **Step 4: Run** the tests, then `mvn test` once, then `SingleNodeGoldenTest`. The golden file must be unchanged. Run `bash scripts/test-setup.sh`.
- [ ] **Step 5: Commit** `cluster: lazy reader generations through a remote directory`.

### Task 5: Cache stats for operators

**Files:**
- Modify: `cluster/ClusterRuntime.java`, `cli/IndexStatusCommand.java`, `web/WebServer.java` (`/health`), `./pharos` (`index-status` text output), README (reader section)
- Test: `cluster/WarmStatsSurfaceTest.java`, plus the existing `IndexStatusCommand` and web tests extended

**Interfaces:**
- Produces:
  - `index-status --json` gains, on readers only, a top-level `readerCache` object: `{mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate, bytesFetched, evictions, leasesOpen}`.
  - `/health` includes the same object on readers.
  - The text output prints one line: `reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions`.
  - Phase 4's `/cluster/status`, and the web UI's Cluster page built on it (design doc, "Cluster page (web UI)"), will consume these fields, so their names are fixed here.
- [ ] **Step 1: Write the failing tests.** `readerCache` is present on a lazy reader with a non-zero `hits` after a query, and absent on `role: all`. The Python CLI prints the line, checked with a stubbed JSON.
- [ ] **Step 2: Run** them. They fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** them. They pass. Run `mvn test` once.
- [ ] **Step 5: Commit** `cluster: reader cache stats in index-status and /health`.

### Task 6: Phase 2 gate against MinIO

**Files:**
- Create: `scripts/cluster-gate-phase2.sh`, `scripts/cluster_gate_phase2.py` (reuse `cluster_gate.py` helpers by import, not by copying), `docs/superpowers/gates/<date>-phase-2.md`

**Gate procedure.** Run everything under `systemd-run --user --scope -p MemoryMax=10G`, using the MinIO binary and heap conventions from the Phase 1 gate.

1. **Writer.** It indexes lucene with `--no-embed` and pharos **with** embeddings (jina, the vector path) and publishes both to MinIO.
2. **Cold.** 20 trials. Each trial empties the reader's block cache and generation dirs, starts the reader, and waits for "serving 2 shard(s)". It then times one query, the trial's own, chosen from 20 distinct lucene BM25 queries in `eval/benchmark.py`, plus one vector query on pharos.
   - **Pass:** cold first-query p95 < 3 s for lucene. Report the vector p95 too, but it is not gated.
   - Report the time from reader start to serving.
3. **Warm.** The same 99 queries as the Phase 1 gate run against a lazy reader with a warm cache and against a full-mode reader (local files). Interleave 3 runs per side.
   - **Pass:** lazy warm p95 ≤ 1.10 × full-mode p95.
   - **Pass:** top-10 identical on all 99 queries.
4. **Cap.** Set `cacheMaxBytes` to 50% of the lucene shard's Lucene bytes, rounded down to a block multiple, and run all 99 queries twice. Sample `bytesOnDisk` from `/health` every second.
   - **Pass:** the maximum sample ≤ `cacheMaxBytes + maxConcurrentFetches × blockSizeBytes`.
   - **Pass:** top-10 identical to full mode.
   - Record the hit rate.
5. **Safety.** Marker check: `~/.pharos` is untouched. Kill processes by PID only.

- [ ] **Step 1:** Write the scripts. Verify them with `bash -n`, `--dry-run` and `--help`. Check the Python statistics against canned inputs.
- [ ] **Step 2:** Run the gate. Record every number in the gate doc.
- [ ] **Step 3:** If a criterion fails, find the cause before tuning anything: block size, prefetch set, or concurrency. Record each change, then re-run. If cold p95 can't meet 3 s on this host, record the measured value and the dominant cost, and stop for a decision. Don't lower the bar silently.
- [ ] **Step 4: Commit** `cluster: Phase 2 gate scripts and results`.
