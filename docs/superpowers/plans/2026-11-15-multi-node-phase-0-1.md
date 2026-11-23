# Multi-node Pharos, Phases 0–1: Cluster SPI and Object-Store Replication — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put indexing and search behind a backend-neutral cluster SPI with no behaviour change (Phase 0), then let a writer publish immutable shard generations to S3-compatible storage and a separate reader download and serve them read-only (Phase 1).

**Architecture:** A new `com.pharos.cluster` package holds the SPI (`LocalCache`, `IndexPublisher`, `ShardCatalog`, `ReplicaSource`, `ReplicationBackend`). Today's behaviour is the `local` backend. Read paths stop opening ArcadeDB directly and go through a read-only `GraphView`, which an immutable compact graph file can also implement. The `object-store` backend publishes each Lucene commit plus a compact graph as generation N, flips `LATEST` with a conditional put, and garbage-collects. A reader-role daemon polls `LATEST`, materialises whole generations into a local cache directory and swaps them in.

**Tech Stack:** Java 25, Lucene 10.4, ArcadeDB 25.1.1, Jackson, picocli, Javalin, AWS SDK for Java v2 (S3 + url-connection-client), JUnit 5 + AssertJ, Testcontainers MinIO.

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md` (snapshot of the design doc at rev 58). This plan covers the spec's Phase 0 and Phase 1 rows. Phases 2–6 get their own plans.

## Global Constraints

- Phase 0 gate (spec): "All tests pass, single-node search results byte-identical, no measurable latency change".
- Phase 1 gate (spec): "Index pharos + lucene on writer, search on a separate reader against MinIO; same top-10 as single node; a new generation is visible in ≤ poll interval + download".
- Core rule (spec): "indexing and search code never knows where bytes live. They see a shard, a generation and a local directory; backends move the bytes."
- Node roles come from config (`role: writer | reader | all`); `all` is the single-node default. `all` ⇔ backend `local`; `writer` and `reader` require backend `object-store`.
- Object store: build against the S3 API (S3, GCS and MinIO all work) and test with MinIO in Testcontainers.
- Layout under `<bucket>/<prefix>/`: `catalog/shards.json`, `shards/<shard>/LATEST`, `shards/<shard>/manifests/gen-N.json`, `shards/<shard>/lucene/<file>` (immutable, content-named, shared across generations), `shards/<shard>/graph/gen-N…`, `shards/<shard>/refs/gen-N…`.
- `LATEST` content: `{"generation": N, "manifest": "gen-N.json"}`. It is written with `If-Match` on the previous ETag, or `If-None-Match: *` for the first generation.
- GC retention defaults (spec): "last 3 generations + 1 hour".
- Reader poll default: 10 s.
- The graph decision (spec): "an immutable compact graph file for readers, one per generation … ArcadeDB stays on writers."
- Shard = project in this plan (default spec `[project]`). Custom keys are Phase 6.
- FAT JAR: keep `ServicesResourceTransformer` in the shade plugin. The AWS SDK also relies on `META-INF/services`.
- Tests and gates never touch `~/.pharos`: use `@TempDir` or `-Duser.home=<copy>`.
- The Python client `./pharos` must stay in sync with new Java commands (CLAUDE.md).

**Out of scope here, deliberately:**
- Writer leases and the work queue (Phase 4). Phase 1 assumes one writer per shard and relies only on the `LATEST` CAS.
- `WriterStateStore` (Phase 4).
- `ShardRouter`, fan-out and reader membership (Phase 3).
- Block-level and lazy fetch (Phase 2). Phase 1 readers copy whole generations.
- The cross-project shard and the module graph on readers. On a reader, module and cross-project tools answer "not available on reader nodes yet".

**Deviations from the spec text, decided here:**
- Graph and refs objects are gzip (JDK), named `graph/gen-N.cgf.gz` and `refs/gen-N.json.gz`, instead of `.zst`. This avoids a native dependency.
- `search-params.json` is not shipped: no Java code reads it.
- `synonyms.txt` is global, so it is shipped as `catalog/synonyms.txt`.

## Review Focus

1. **A second `pharos index` run on the same project deletes Lucene files while the first run's publish uploads them.** Publish must fail with `PublishException` without writing a manifest or moving `LATEST`. Test owned by Task 8.
2. **The object store is unreachable, or credentials are missing, when an index run ends.** The local index is kept, the command exits with code 3 and the message names `pharos publish <project>` as the retry. Test owned by Task 11.
3. **A reader's download fails mid-file.** No `gen-N` directory appears, the previous generation keeps serving, and the leftover `.partial` directory is removed on the next sync. Test owned by Task 10.
4. **A reader machine has an unrelated or stale file at the indexed absolute path.** Bodies and snippets come from the index, never from that file. Test owned by Task 11.
5. **A project name is unsafe as an object key** (space, `/`, `..`, empty). It is rejected with a clear message before anything is uploaded. Tests owned by Task 2 (`ShardId`) and Task 8 (publisher).

---

## File Structure

```
src/main/java/com/pharos/cluster/
  ShardId.java, Generation.java, NodeRole.java, BackendKind.java     value types (Task 2)
  LocalCache.java, LocalFsCache.java                                  where a node's shard dirs live (Task 4)
  IndexPublisher.java, LocalSnapshot.java, NoOpPublisher.java,
  PublishException.java, PublishConflictException.java                writer-side SPI (Task 5)
  ReplicationBackend.java, LocalBackend.java, Backends.java,
  ClusterRuntime.java, RoleGuard.java                                 backend wiring (Tasks 5, 11)
  ShardCatalog.java, ReplicaSource.java                               reader-side SPI (Task 10)
  ReaderLocalCache.java, ReplicaSyncer.java                           reader materialisation (Task 10)
src/main/java/com/pharos/cluster/objectstore/
  ObjectStore.java, StoredObject.java, ObjectInfo.java, S3ObjectStore.java   (Task 7)
  ObjectStoreLayout.java, ShardManifest.java, ManifestFile.java,
  ObjectStoreCatalog.java, ObjectStorePublisher.java                  (Task 8)
  GenerationGc.java                                                   (Task 9)
  ObjectStoreReplicaSource.java, ObjectStoreBackend.java              (Tasks 10, 11)
src/main/java/com/pharos/config/ClusterConfig.java                    (Task 2)
src/main/java/com/pharos/graph/
  GraphView.java, GraphViews.java, EmptyGraphView.java                (Task 3)
  CompactGraph.java, CompactGraphWriter.java                          (Task 6)
src/main/java/com/pharos/cli/PublishCommand.java                      (Task 11)
src/test/java/com/pharos/cluster/...                                  tests named per task
src/test/java/com/pharos/cluster/objectstore/InMemoryObjectStore.java test double (Task 7)
scripts/cluster-gate.sh, scripts/cluster_gate.py                      Phase 1 gate (Task 12)
```

Modified: `IndexConfig`, `ProjectRegistry`, `LuceneIndexer`, `CallGraph`, `ProjectIndexManager`, `SearchEngine`, `McpToolRegistry`, `WebServer`, `SourceReader`, `ModuleBoundaryAnalyzer`, CLI commands that read graphs or write indexes, `Main`, `pom.xml`, `pharos`, `README.md`.

---

# Phase 0 — SPI refactor, no behaviour change

### Task 1: Single-node golden characterisation test

Lands before any refactor, so later tasks can prove that results are byte-identical.

**Files:**
- Create: `src/test/java/com/pharos/cluster/SingleNodeGoldenTest.java`
- Create: `src/test/resources/golden/single-node-search.json` (generated, then committed)

**Interfaces:**
- Produces: `SingleNodeGoldenTest.snapshot(SearchEngine engine, ProjectRegistry registry) -> String`. It returns canonical JSON, and Task 11 reuses it to compare a writer with a reader.

- [ ] **Step 1: Write the test.**
  - Setup: index `src/test/resources/test-projects/sample-app` and `sample-python-app` with `ProjectIndexManager` into a `@TempDir` index dir. Use no embeddings and an isolated `ProjectRegistry`; add a public static factory `ProjectRegistry.at(Path registryFile)` delegating to the existing package-private constructor.
  - Snapshot content, in fixed order:
    - For 12 fixed queries (class names, behaviour phrases, one per category A–H plus four from sample-python-app): the top-10 `id`, `score` (formatted `%.6f`) and `snippetStartLine`, for the `keyword` and `auto` pipelines, project-scoped.
    - `callers` and `callees` of 5 fixed FQNs.
    - `shortestPath` between 2 fixed pairs.
    - `getClassContext` of 2 classes (body + method ids).
  - Serialise with sorted keys.
  - If `golden/single-node-search.json` is missing, write it to `src/test/resources/golden/` and fail with "golden generated — review and commit". Otherwise `assertThat(actual).isEqualTo(expected)`.

- [ ] **Step 2: Run it twice.**
  Run: `mvn test -Dtest=SingleNodeGoldenTest`
  Expected: the first run fails with "golden generated"; the second run passes.

- [ ] **Step 3: Check for nondeterminism.** Run the test three more times. Expected: PASS every time. If a value flaps (for example score ties ordering differently), sort ties by id inside `snapshot` and regenerate.

- [ ] **Step 4: Commit.**
```bash
git add src/test/java/com/pharos/cluster/SingleNodeGoldenTest.java src/test/resources/golden src/main/java/com/pharos/config/ProjectRegistry.java
git commit -m "test: single-node golden search snapshot for the cluster refactor"
```

### Task 2: Cluster value types and `cluster` config

**Files:**
- Create: `cluster/ShardId.java`, `cluster/Generation.java`, `cluster/NodeRole.java`, `cluster/BackendKind.java`, `config/ClusterConfig.java`
- Modify: `config/IndexConfig.java` (field `cluster`, getter never null)
- Test: `src/test/java/com/pharos/cluster/ClusterConfigTest.java`, `ShardIdTest.java`

**Interfaces:**
- Produces:
  - `record ShardId(String value)`:
    - compact constructor validates `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` and rejects names containing `..`;
    - otherwise throws `IllegalArgumentException("project name '<v>' cannot be a shard id: use letters, digits, '.', '_' or '-'")`;
    - `static ShardId ofProject(String project)`.
  - `record Generation(long value) implements Comparable<Generation>`: `static final Generation FIRST = new Generation(1)`, `Generation next()`.
  - `enum NodeRole { ALL, WRITER, READER }` and `enum BackendKind { LOCAL, OBJECT_STORE }`. JSON values are lower-case (`"all"`, `"writer"`, `"reader"`, `"local"`, `"object-store"`) via `@JsonValue`/`@JsonCreator`.
  - `ClusterConfig` (Jackson POJO with getters and setters):
    - `role = ALL`, `backend = LOCAL`, `nodeId = <hostname>`;
    - `pollIntervalSeconds = 10`, `cacheDir = IndexConfig.DEFAULT_BASE.resolve("cluster-cache")`;
    - `retentionGenerations = 3`, `retentionMinutes = 60`;
    - nested `ObjectStoreConfig objectStore` with `endpoint` (null = AWS default), `region = "us-east-1"`, `bucket`, `prefix = "pharos"`, `pathStyle = true`, `accessKeyEnv`, `secretKeyEnv`;
    - `void validate()`.
  - `IndexConfig.getCluster()`. `IndexConfig.load()` calls `cluster.validate()`.

- [ ] **Step 1: Write failing tests.**
  - `ClusterConfigTest`:
    - `defaultsAreSingleNode`: a config without a `cluster` key gives `role == ALL` and `backend == LOCAL`.
    - `readerNeedsObjectStore`: reader + local → `IllegalStateException` mentioning `cluster.backend`.
    - `allNeedsLocal`: all + object-store → `IllegalStateException`.
    - `objectStoreNeedsBucket`: missing bucket → `IllegalStateException` naming `cluster.objectStore.bucket`.
    - `retentionAtLeastTwo`: `retentionGenerations: 1` → `IllegalStateException`.
    - `parsesFullObjectStoreConfig`: reads the JSON from the README example (Task 11) and asserts every field.
  - `ShardIdTest`:
    - accepts `pharos`, `my-own-gpt` and `a.b_c`;
    - rejects `""`, `"my project"`, `"a/b"`, `".."`, `"x..y"` and a 129-character name, each with the message above.

- [ ] **Step 2: Run them to verify they fail.** Run: `mvn test -Dtest='ClusterConfigTest,ShardIdTest'`. Expected: compilation failure (types don't exist).

- [ ] **Step 3: Implement the types and the `IndexConfig` field.**

- [ ] **Step 4: Run them to verify they pass**, plus `mvn test -Dtest='*Config*'`. Expected: PASS.

- [ ] **Step 5: Commit.** Message: `cluster: value types and cluster config section`.

### Task 3: Read-only `GraphView` for every graph read path

**Files:**
- Create: `graph/GraphView.java`, `graph/GraphViews.java`, `graph/EmptyGraphView.java`
- Modify: `graph/CallGraph.java` (`implements GraphView`)
- Modify read sites (replace `CallGraph.open(...)` with `GraphViews.open(meta)`):
  - `search/SearchEngine.java:644`
  - `mcp/McpToolRegistry.java:434,773`
  - `web/WebServer.java:601`
  - `cli/CallersCommand.java:75`, `cli/CalleesCommand.java:70`, `cli/PathCommand.java:60`, `cli/StatsCommand.java:65`
  - `graph/ModuleBoundaryAnalyzer.java:75,91`
- Do not change the writer sites (`ProjectIndexManager`, `CrossProjectLinker`, `KnowledgeGraphBuilder`): they keep `CallGraph`.
- Test: `src/test/java/com/pharos/graph/GraphViewsTest.java`

**Interfaces:**
- Produces:
```java
public interface GraphView extends AutoCloseable {
    Set<String> callers(String fqn);           Set<String> callees(String fqn);
    Set<String> calledTargets(Set<String> candidateFqns);
    List<String> shortestPath(String fromFqn, String toFqn);
    Stream<String> allFqns();                  long methodCount();   long callCount();
    Set<String> directSubclasses(String classFqn);  Set<String> directSuperTypes(String classFqn);
    List<String> allSubclasses(String classFqn);
    Set<String> fieldReaders(String fieldFqn); Set<String> fieldWriters(String fieldFqn);
    Set<String> declaredFields(String classFqn);    List<CallGraph.FieldInfo> getFieldsOf(String classFqn);
    Set<String> annotatedWith(String annotationFqn); Set<String> methodsReturning(String typeFqn);
    Set<String> methodsTaking(String typeFqn);
    @Override void close();
}
```
  - `GraphViews`:
    - constants `ARCADE_DIR = "callgraph.arcadedb"` and `COMPACT_FILE = "graph.cgf.gz"`;
    - `static GraphView open(Path shardDir)`, which picks the ArcadeDB directory if it exists, else the compact file if it exists (Task 6 adds that branch; until then this case throws `UnsupportedOperationException`), else `EmptyGraphView.INSTANCE`;
    - `static GraphView open(ProjectMeta meta)`, which equals `open(Path.of(meta.getIndexPath()))`.
  - Read paths no longer create an empty ArcadeDB database as a side effect.

- [ ] **Step 1: Write failing tests.** `GraphViewsTest`:
  - `missingGraphIsEmptyAndCreatesNothing`: given an empty temp dir, `callers("x")` is empty, `methodCount() == 0`, and the dir is still empty afterwards.
  - `arcadeGraphIsUsedWhenPresent`: build a graph with `CallGraph.open` + `addMethod`/`addCall`/`flush`; `GraphViews.open(dir).callers(callee)` equals `{caller}`.

- [ ] **Step 2: Run to verify they fail.** `mvn test -Dtest=GraphViewsTest`. Expected: compilation failure.

- [ ] **Step 3: Implement.** Make `CallGraph` implement `GraphView` (its existing methods already match), then replace the listed read sites. Where a site typed a variable as `CallGraph`, retype it to `GraphView`.

- [ ] **Step 4: Run tests.** Run: `mvn test -Dtest='GraphViewsTest,SingleNodeGoldenTest,CallersCalleesIntegrationTest,KnowledgeGraph*'`. Expected: PASS.

- [ ] **Step 5: Commit.** Message: `graph: read paths go through read-only GraphView`.

### Task 4: `LocalCache` decides where shard directories live

**Files:**
- Create: `cluster/LocalCache.java`, `cluster/LocalFsCache.java`
- Modify: `indexer/LuceneIndexer.java` (constructor and `getLucenePath`/`getProjectIndexDir`)
- Test: `src/test/java/com/pharos/cluster/LocalFsCacheTest.java`

**Interfaces:**
- Produces:
  - `interface LocalCache { Path shardDir(String project); }`: the directory holding `lucene/` and the graph for the generation this node serves.
  - `LocalFsCache(IndexConfig config)`: returns `config.getIndexDir().resolve(project)`.
  - `LuceneIndexer(IndexConfig config, LocalCache cache)`. The existing `LuceneIndexer(IndexConfig)` delegates with `new LocalFsCache(config)`.
  - `getProjectIndexDir(p) == cache.shardDir(p)` and `getLucenePath(p) == cache.shardDir(p).resolve("lucene")`.

- [ ] **Step 1: Write the failing test.** `luceneIndexerResolvesThroughCache`: a `LocalCache` lambda mapping `"p"` to `<tmp>/elsewhere/p` makes `getLucenePath("p")` return `<tmp>/elsewhere/p/lucene`. `defaultCacheMatchesIndexDir` covers the 1-arg constructor's path.
- [ ] **Step 2: Run it to verify it fails** (`mvn test -Dtest=LocalFsCacheTest`). Expected: compilation failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `mvn test -Dtest='LocalFsCacheTest,SingleNodeGoldenTest,LuceneDirectoryTest'`. Expected: PASS. Skip `LuceneDirectoryTest` if it doesn't exist on this base.
- [ ] **Step 5: Commit.** Message: `cluster: LocalCache resolves shard directories for LuceneIndexer`.

### Task 5: `IndexPublisher` hook, `ReplicationBackend`, local backend wiring — Phase 0 gate

**Files:**
- Create: `cluster/IndexPublisher.java`, `LocalSnapshot.java`, `NoOpPublisher.java`, `PublishException.java`, `PublishConflictException.java`, `ReplicationBackend.java`, `LocalBackend.java`, `Backends.java`
- Modify: `indexer/ProjectIndexManager.java` (canonical constructor + publish after indexing), `Main.java` (build backend, pass publisher and `LocalCache`)
- Test: `src/test/java/com/pharos/cluster/PublishHookTest.java`

**Interfaces:**
- Consumes: `ShardId`, `Generation` (Task 2); `LocalCache`, `LocalFsCache` (Task 4).
- Produces:
  - `record LocalSnapshot(ShardId shard, Path shardDir, ProjectMeta meta, String modelFingerprint)`.
  - `interface IndexPublisher { Optional<Generation> publish(LocalSnapshot snapshot) throws IOException; }`. The `NoOpPublisher.INSTANCE` implementation returns `Optional.empty()`.
  - `class PublishException extends IOException`, and `class PublishConflictException extends PublishException`.
  - `interface ReplicationBackend extends Closeable`:
    - `BackendKind kind()`, `IndexPublisher publisher()`, `LocalCache localCache()`;
    - `ShardCatalog catalog()` and `ReplicaSource source()`. These two are declared in Task 10; until then the methods are absent.
  - `LocalBackend(IndexConfig)` returns kind `LOCAL`, `NoOpPublisher` and `LocalFsCache`. Once Task 10 adds them, its `catalog()`/`source()` throw `UnsupportedOperationException("local backend has no replicas; set cluster.backend to object-store")`.
  - `Backends.create(IndexConfig config)` returns `LocalBackend` for `LOCAL`. Task 11 adds `OBJECT_STORE`.
  - `ProjectIndexManager`:
    - the canonical constructor gains a last parameter `IndexPublisher publisher`, and every other constructor passes `NoOpPublisher.INSTANCE`;
    - the public 7-argument `index(...)` runs the existing body (move it to a private `indexLocally(...)`), then calls `publisher.publish(new LocalSnapshot(ShardId.ofProject(projectName), luceneIndexer.getProjectIndexDir(projectName), meta, compositeModelFingerprint()))` and returns `meta`;
    - a failure propagates as `PublishException`, with the local index already committed and registered.

- [ ] **Step 1: Write failing tests.** In `PublishHookTest`, a recording `IndexPublisher` is used.
  - `fullIndexPublishesOnce`: indexing sample-app once gives exactly one call whose `shard().value()` is the project name, whose `shardDir()` contains `lucene/segments_*`, and whose `meta().getName()` is the project.
  - `incrementalIndexPublishesOnce`: same, after touching one file and indexing incrementally.
  - `publishFailureKeepsLocalIndex`: a publisher throwing `PublishException` makes `index` rethrow it, and `registry.find(project)` is present afterwards.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest=PublishHookTest`. Expected: compilation failure.
- [ ] **Step 3: Implement**, and wire `Main`: `ReplicationBackend backend = Backends.create(config)`, `new LuceneIndexer(config, backend.localCache())`, and pass `backend.publisher()` to `ProjectIndexManager`.
- [ ] **Step 4: Run the Phase 0 gate.**
  - Run `mvn test`. Expected: the only failures are ones that also fail on `origin/main` (compare with `git stash; mvn test`). Known flaky: `CrossProjectLinkerTest`.
  - Run `mvn test -Dtest=SingleNodeGoldenTest`. Expected: PASS (byte-identical results).
  - Latency: run `mvn test -Dtest=SearchPerformanceTest` on this branch and on `origin/main`. Expected: median search latency differs by under 5%; record both numbers in the commit message.
- [ ] **Step 5: Commit.** Message: `cluster: IndexPublisher hook and local ReplicationBackend (phase 0 gate)`, with the latency numbers in the message body.

---

# Phase 1 — Publish + full-copy reader

### Task 6: Compact graph file

**Files:**
- Create: `graph/CompactGraphWriter.java`, `graph/CompactGraph.java`
- Modify: `graph/CallGraph.java` (export iteration), `graph/GraphViews.java` (compact branch)
- Test: `src/test/java/com/pharos/graph/CompactGraphParityTest.java`

**Interfaces:**
- Consumes: `GraphView`, `GraphViews` (Task 3).
- Produces:
  - `CallGraph.forEachVertex(Consumer<VertexRecord>)`, where `record VertexRecord(String kind /* Method|CodeType|CodeField */, String fqn, String classPrefix, String ownerFqn, String fieldType, String accessMod)`.
  - `CallGraph.forEachEdge(String edgeType, BiConsumer<String,String> fromTo)`, for each edge type in `calls, inherits, implements_iface, declares_field, reads_field, writes_field, returns_type, takes_type, annotated_by`.
  - `CompactGraphWriter.export(CallGraph source, Path target) throws IOException`.
  - `CompactGraph.read(Path file) throws IOException`, implementing `GraphView`.
  - `GraphViews.open(dir)` returns `CompactGraph.read(dir.resolve(COMPACT_FILE))` when there's no ArcadeDB dir and the file exists.
- Format:
  - a gzip stream; magic `"PHGR"`, `int version = 1`;
  - a vertex table (kind byte, fqn, classPrefix, ownerFqn, fieldType, accessMod, as length-prefixed UTF-8 strings, `-1` for null);
  - then, per edge type, the edge count and parallel `int[] from, int[] to` sorted by `from`.
  - The reader builds forward and reverse CSR (offsets + targets) per edge type in memory.
  - The semantics of every `GraphView` method must equal `CallGraph`'s. Read the query bodies in `CallGraph.java:239-415` (the vertex types, edge direction and transitive closure used by `allSubclasses` and `shortestPath`) and mirror them.

- [ ] **Step 1: Write failing tests.** `CompactGraphParityTest` builds the sample-app graph (as `CallersCalleesIntegrationTest` does, plus `KnowledgeGraphBuilder` so type/field edges exist), exports it, and reads it back.
  - `everyMethodMatchesArcade`: for every fqn in `allFqns()` and every single-argument `GraphView` method, the compact result equals the ArcadeDB result. Compare sets as sets and lists as lists. Also compare `allFqns()` as sorted lists, `methodCount()`, `callCount()`, and `calledTargets(allFqns)`.
  - `shortestPathMatches`: for 50 deterministic (seeded) fqn pairs, the path lengths are equal and the compact path is a valid path (each consecutive pair is a `calls` edge).
  - `emptyGraphRoundTrips`: an empty graph exports and reads back as empty.
  - `rejectsWrongMagic`: a garbage file gives `IOException` with the message "not a pharos compact graph".

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest=CompactGraphParityTest`. Expected: compilation failure.
- [ ] **Step 3: Implement** the export iteration, the writer and the reader.
- [ ] **Step 4: Run to verify they pass.** Same command. Expected: PASS.
- [ ] **Step 5: Commit.** Message: `graph: immutable compact graph file with ArcadeDB parity`.

### Task 7: `ObjectStore` abstraction, in-memory double, S3 implementation

**Files:**
- Create: `cluster/objectstore/ObjectStore.java`, `StoredObject.java`, `ObjectInfo.java`, `S3ObjectStore.java`
- Modify: `pom.xml`
  - Add `software.amazon.awssdk:s3` and `software.amazon.awssdk:url-connection-client`, managed by the `software.amazon.awssdk:bom` (latest 2.x; `PutObjectRequest.ifMatch` needs ≥ 2.29). Exclude `netty-nio-client` and `apache-client`.
  - Add test-scope `org.testcontainers:minio` and `org.testcontainers:junit-jupiter` 1.21.3.
- Test: `src/test/java/com/pharos/cluster/objectstore/ObjectStoreContract.java` (abstract), `InMemoryObjectStore.java`, `InMemoryObjectStoreTest.java`, `S3ObjectStoreMinioTest.java`

**Interfaces:**
- Produces:
```java
public interface ObjectStore extends Closeable {
    Optional<StoredObject> get(String key) throws IOException;      // record StoredObject(byte[] data, String etag)
    void download(String key, Path target) throws IOException;     // NoSuchFileException when absent
    String put(String key, Path source) throws IOException;         // returns ETag
    String put(String key, byte[] data) throws IOException;
    Optional<String> putIfAbsent(String key, byte[] data) throws IOException;               // empty: key existed
    Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException;   // empty: ETag changed
    List<ObjectInfo> list(String prefix) throws IOException;  // record ObjectInfo(String key, long size, Instant lastModified)
    void delete(String key) throws IOException;               // absent key is not an error
}
```
  - `S3ObjectStore(ClusterConfig.ObjectStoreConfig cfg)`:
    - build the `S3Client` with `UrlConnectionHttpClient`, `endpointOverride` when set, `forcePathStyle(cfg.pathStyle)` and the region;
    - use `StaticCredentialsProvider` when both env vars named in config are set, else `DefaultCredentialsProvider`;
    - HTTP 412 and 409 from a conditional put become `Optional.empty()`; other SDK errors become `IOException` with the key in the message;
    - `list` paginates.
  - `InMemoryObjectStore` (test sources): a thread-safe map with UUID ETags and an injectable `Clock` for `lastModified`. A `failOn(String keySubstring)` hook throws `IOException` on matching downloads (used by Task 10).

- [ ] **Step 1: Write the contract tests.** `ObjectStoreContract` has `abstract ObjectStore store()` and these tests:
  - `putThenGet`;
  - `getMissingIsEmpty`;
  - `downloadMissingThrowsNoSuchFile`;
  - `putIfAbsentOnlyOnce`: the second call is empty, and the data is unchanged;
  - `putIfMatchSucceedsWithCurrentEtag`;
  - `putIfMatchFailsWithStaleEtag`: empty, and the data is unchanged;
  - `listByPrefixIncludesSizes`;
  - `deleteMissingIsNoop`;
  - `largeFileRoundTrip` (8 MB from a temp file).

  `InMemoryObjectStoreTest` extends the contract. `S3ObjectStoreMinioTest` extends it with `@Testcontainers(disabledWithoutDocker = true)` and a `MinIOContainer("minio/minio:latest")`, creating the bucket in `@BeforeAll`.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest='InMemoryObjectStoreTest,S3ObjectStoreMinioTest'`. Expected: compilation failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: `InMemoryObjectStoreTest` PASS. `S3ObjectStoreMinioTest` PASS with Docker, SKIPPED without. Also run `mvn package -DskipTests` and `java -jar target/pharos-*.jar --help`. Expected: the jar builds and starts (the shade step keeps the SDK service files).
- [ ] **Step 5: Commit.** Message: `cluster: ObjectStore with S3 implementation and contract tests`.

### Task 8: Object-store publisher

**Files:**
- Create: `cluster/objectstore/ObjectStoreLayout.java`, `ShardManifest.java`, `ManifestFile.java`, `ObjectStoreCatalog.java`, `ObjectStorePublisher.java`
- Test: `src/test/java/com/pharos/cluster/objectstore/ObjectStorePublisherTest.java`

**Interfaces:**
- Consumes: `ObjectStore`, `InMemoryObjectStore` (Task 7); `IndexPublisher`, `LocalSnapshot`, `PublishException`, `PublishConflictException` (Task 5); `CompactGraphWriter` (Task 6).
- Produces:
  - `ObjectStoreLayout(String prefix)`, with these keys under `<prefix>/`:

    | Method | Key |
    |---|---|
    | `catalogKey()` | `catalog/shards.json` |
    | `synonymsKey()` | `catalog/synonyms.txt` |
    | `latestKey(ShardId)` | `shards/<s>/LATEST` |
    | `manifestKey(ShardId, Generation)` | `shards/<s>/manifests/gen-<N>.json` |
    | `luceneKey(ShardId, String fileName, String checksumHex)` | `shards/<s>/lucene/<fileName>@<checksumHex>` |
    | `graphKey(ShardId, Generation)` | `shards/<s>/graph/gen-<N>.cgf.gz` |
    | `refsKey(ShardId, Generation)` | `shards/<s>/refs/gen-<N>.json.gz` |
    | `shardPrefix(ShardId)` | `shards/<s>/` |

  - `record ManifestFile(String role /* "lucene" | "graph" | "refs" */, String name, String key, long length, String checksum)`.
  - `record ShardManifest(int formatVersion, String shard, long generation, Long parentGeneration, Instant createdAt, String writerNode, long docCount, long sizeBytes, String modelFingerprint, List<ManifestFile> files, ProjectMeta projectMeta)`. `formatVersion` is 1, and `projectMeta` has `unresolvedRefs` cleared. Jackson must round-trip it (register `JavaTimeModule`).
  - `ObjectStoreCatalog(ObjectStore, ObjectStoreLayout)`:
    - `List<ShardId> shards()`;
    - `void register(ShardId)`: a CAS read-modify-write on `{"shards":[{"id":"..."}]}`, with up to 5 attempts, then `PublishConflictException`.
  - `ObjectStorePublisher(ObjectStore, ObjectStoreLayout, ObjectStoreCatalog, GenerationGc /* Task 9; pass null until then */, String nodeId, Path synonymsFile)` implements `IndexPublisher`. Protocol:
    1. Read `LATEST` and its ETag. The previous generation is P (or none), and N = P + 1 (or `Generation.FIRST`).
    2. Open `FSDirectory` on `shardDir/lucene` and `SegmentInfos.readLatestCommit`. For each file in `commit.files(true)`, read the checksum via `CodecUtil.retrieveChecksum` (hex, lower-case) and build `luceneKey`. Upload only keys absent from manifest P.
    3. If `callgraph.arcadedb` exists: `CompactGraphWriter.export` it to a temp file, then upload it to `graphKey`. Do the same for `unresolved-refs.json` (gzip) to `refsKey`.
    4. Put `catalog/synonyms.txt` when `synonymsFile` exists.
    5. `putIfAbsent(manifestKey(N))`. Empty → `PublishConflictException`.
    6. `LATEST`: use `putIfMatch(previous etag)`, or `putIfAbsent` for the first generation. Empty → `PublishConflictException` ("LATEST moved; another writer published shard <s>").
    7. `catalog.register(shard)`, then `gc.collect(shard, lastModified of LATEST)` when gc ≠ null. A GC error is logged and not thrown.
    8. Return `Optional.of(N)`.
  - Any missing or unreadable commit file in step 2 → `PublishException` before step 5 (see Review Focus 1).

- [ ] **Step 1: Write failing tests.** These use the sample-app index from `ProjectIndexManager` plus `InMemoryObjectStore`.
  - `firstPublishWritesGenerationOne`: `LATEST` is `{"generation":1,"manifest":"gen-1.json"}`. The manifest lists every commit file (`role = "lucene"`) plus one `graph` file. Every manifest key exists in the store. `catalog.shards()` equals `[sample-app]`.
  - `secondPublishUploadsOnlyNewFiles`: re-index incrementally after editing one file and publish again. The second manifest has `parentGeneration = 1`, and the keys uploaded by the second publish are exactly the keys absent from manifest 1 (count `put` calls via a spy wrapper). Files shared with gen 1 have the same keys.
  - `staleLatestIsAConflict`: overwrite `LATEST` behind the publisher's back between steps 1 and 6 (spy store hook). The publish throws `PublishConflictException`, and `LATEST` still holds the foreign value.
  - `missingCommitFileFailsBeforeManifest`: the spy deletes `_0.cfs` (or the first `.si`) from the local dir just before its upload. The publish throws `PublishException`, and neither `manifests/gen-1.json` nor `LATEST` exists.
  - `unsafeProjectNameRejectedBeforeUpload`: a `LocalSnapshot` for a project named `"my project"` throws `IllegalArgumentException` from `ShardId`, and the store stays empty.
  - `synonymsShipped`: after a publish, `catalog/synonyms.txt` equals the local file.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest=ObjectStorePublisherTest`. Expected: compilation failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass.** Expected: PASS.
- [ ] **Step 5: Commit.** Message: `cluster: object-store publisher with LATEST CAS`.

### Task 9: Generation garbage collection

**Files:**
- Create: `cluster/objectstore/GenerationGc.java`
- Modify: `ObjectStorePublisher` callers in tests, which now pass a real `GenerationGc`
- Test: `src/test/java/com/pharos/cluster/objectstore/GenerationGcTest.java`

**Interfaces:**
- Consumes: `ObjectStore`, `ObjectStoreLayout`, `ShardManifest` (Task 8).
- Produces: `GenerationGc(ObjectStore store, ObjectStoreLayout layout, int keepGenerations, Duration minAge)` with `int collect(ShardId shard, Instant now)`, which returns the number of deleted objects.
  - **Kept:** manifests of the newest `keepGenerations` generations, plus any manifest whose `createdAt` is within `minAge` of `now`.
  - **Deleted:** every other manifest, with its graph and refs objects.
  - **Then deleted:** `lucene/` objects referenced by no kept manifest and whose `lastModified` is more than `minAge` before `now`.
  - `now` is the object store's own clock (the `lastModified` of the `LATEST` just written), so a skewed writer clock cannot delete files that readers still use.

- [ ] **Step 1: Write failing tests.** These use `InMemoryObjectStore` with a fixed clock and five synthetic generations; each manifest references `lucene/a@1` plus one unique file.
  - `keepsLastThreeAndYoung`: gens 1–5 are 2 h old. `collect` leaves manifests 3, 4 and 5, deletes graph/refs 1–2, keeps `lucene/a@1`, deletes the unique files of gens 1–2, and returns that count.
  - `youngGenerationsSurviveEvenBeyondCount`: all 5 generations are 10 minutes old, so all 5 manifests remain.
  - `orphanUploadsYoungerThanMinAgeSurvive`: a `lucene/x@9` that no manifest references, uploaded 10 minutes ago, is kept. The same file 2 h old is deleted.
  - `usesStoreClockNotLocal`: `collect(shard, storeNow)` where the local `Instant.now()` is skewed by +1 day decides by `storeNow` only.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest=GenerationGcTest`. Expected: compilation failure.
- [ ] **Step 3: Implement**, and pass `new GenerationGc(store, layout, cfg.getRetentionGenerations(), Duration.ofMinutes(cfg.getRetentionMinutes()))` from the publisher's construction sites.
- [ ] **Step 4: Run** `mvn test -Dtest='GenerationGcTest,ObjectStorePublisherTest'`. Expected: PASS.
- [ ] **Step 5: Commit.** Message: `cluster: generation GC with retention by count and age`.

### Task 10: Reader: replica source, local cache swap, syncer

**Files:**
- Create: `cluster/ShardCatalog.java`, `cluster/ReplicaSource.java`, `cluster/ReaderLocalCache.java`, `cluster/ReplicaSyncer.java`, `cluster/objectstore/ObjectStoreReplicaSource.java`
- Modify:
  - `cluster/objectstore/ObjectStoreCatalog.java` (`implements ShardCatalog`);
  - `cluster/ReplicationBackend.java` (add `catalog()` and `source()`);
  - `cluster/LocalBackend.java` (throwing implementations of those two);
  - `indexer/LuceneIndexer.java` (reopen on path change, retire old readers).
- Test: `src/test/java/com/pharos/cluster/ReplicaSyncerTest.java`, `src/test/java/com/pharos/indexer/LuceneIndexerSwapTest.java`

**Interfaces:**
- Consumes: `ObjectStorePublisher`, `ShardManifest`, `ManifestFile`, `ObjectStoreLayout` (Task 8); `InMemoryObjectStore.failOn` (Task 7); `LocalCache` (Task 4); `ProjectRegistry.at` (Task 1).
- Produces:
  - `interface ShardCatalog { List<ShardId> shards() throws IOException; }`
  - `interface ReplicaSource`:
    - `Optional<Generation> latest(ShardId) throws IOException`;
    - `ShardManifest manifest(ShardId, Generation) throws IOException`;
    - `void download(ManifestFile file, Path target) throws IOException`;
    - `Optional<byte[]> synonyms() throws IOException`.
  - `ReaderLocalCache(Path cacheDir) implements LocalCache`:
    - `void activate(String project, Path generationDir)`;
    - `shardDir(project)` returns the active generation dir, or `cacheDir.resolve(project).resolve("none")` before the first activation;
    - `Optional<Path> active(String project)`.
  - `ReplicaSyncer(ShardCatalog catalog, ReplicaSource source, ReaderLocalCache cache, ProjectRegistry registry, Path cacheDir, Path synonymsFile, Duration pollInterval) implements Closeable`.
    - `void syncOnce() throws IOException`: for each catalog shard whose `latest` is newer than the served generation:
      1. Delete any `<cacheDir>/<shard>/*.partial`.
      2. Materialise into `<cacheDir>/<shard>/gen-<N>.partial/`: lucene files go to `lucene/<name>`, hard-linked (`Files.createLink`) from the active generation dir when the key matches that dir's `manifest.json`, else downloaded; the graph goes to `graph.cgf.gz`; the manifest goes to `manifest.json`. Refs are not downloaded.
      3. Atomically move the dir to `gen-<N>`.
      4. `cache.activate`.
      5. `registry.register(manifest.projectMeta()` with `indexPath` = the gen dir).
      6. Delete this shard's gen dirs other than the active and the previous one.

      Then write the synonyms file atomically when its bytes changed. One shard failing is logged; other shards continue.
    - `void start()` runs `syncOnce` on a single daemon thread every `pollInterval`.
    - `Map<ShardId, Generation> served()`.
  - `LuceneIndexer`:
    - each cached reader records its lucene `Path`;
    - when `getLucenePath(p)` differs from the cached path, open a fresh `DirectoryReader` on the new path, swap it in, and close the old one after `RETIRE_DELAY = Duration.ofSeconds(60)` on a daemon scheduler;
    - `close()` closes retired readers immediately.

- [ ] **Step 1: Write failing tests.**
  - `ReplicaSyncerTest`: the writer side is `ProjectIndexManager` + `ObjectStorePublisher` over `InMemoryObjectStore`, using sample-app; the reader side uses a separate temp cache dir and `ProjectRegistry.at(...)`.
    - `firstSyncMaterialisesGeneration`: after `syncOnce`, `served()` equals `{sample-app=1}`, `<cache>/sample-app/gen-1/lucene` opens as an index with the writer's `numDocs`, `graph.cgf.gz` exists, and the registry's `indexPath` is that dir.
    - `secondGenerationSwapsAndReusesFiles`: publish gen 2 after an incremental edit, then `syncOnce`. `served()` equals `{sample-app=2}`, files with unchanged keys are hard links (same `fileKey` attribute as in gen-1), and gen-1 still exists as the previous generation.
    - `onlyActiveAndPreviousKept`: after gens 1–3, only `gen-2` and `gen-3` remain.
    - `failedDownloadKeepsServingPrevious` (Review Focus 3): `failOn("lucene/")` during the gen-2 sync. `syncOnce` logs the failure, `served()` still shows gen 1, no `gen-2` dir exists, and the next `syncOnce` without the failure removes `gen-2.partial` and activates gen 2.
    - `synonymsWrittenWhenChanged`: the local synonyms file equals `catalog/synonyms.txt`.
  - `LuceneIndexerSwapTest`:
    - `pathChangeOpensNewReader`: with two index dirs holding different docs, swapping the `LocalCache` mapping makes `openReader` return the new docs, and the old reader stays open until the retire delay.
    - `closeClosesRetired`.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest='ReplicaSyncerTest,LuceneIndexerSwapTest'`. Expected: compilation failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify they pass.** Expected: PASS. Then run `mvn test -Dtest=SingleNodeGoldenTest`. Expected: PASS (the local path is unchanged).
- [ ] **Step 5: Commit.** Message: `cluster: reader syncer materialises and swaps generations`.

### Task 11: Wiring, roles, `publish` command, end-to-end round trip

**Files:**
- Create: `cluster/objectstore/ObjectStoreBackend.java`, `cluster/ClusterRuntime.java`, `cluster/RoleGuard.java`, `cli/PublishCommand.java`
- Modify:
  - `cluster/Backends.java` (`OBJECT_STORE`);
  - `Main.java`: the reader registry is `ProjectRegistry.at(cluster.cacheDir/registry.json)`; `SourceReader.setEnabled(false)` for readers; `ClusterRuntime` goes into `DependencyFactory`;
  - `cli/CodeSearchCommand.java` (register `publish`);
  - `cli/WebCommand.java` and `cli/McpServerCommand.java` (call `clusterRuntime.startServing()` before serving);
  - `cli/IndexCommand.java`, `LinkCommand.java`, `RemoveIndexCommand.java`, `EmbedCommand.java`, `MineCommand.java` and `BackfillEmbeddingCacheCommand.java` (call `RoleGuard.requireIndexing(config)` first);
  - `IndexCommand` maps `PublishException` to exit code 3;
  - `search/SourceReader.java` (`setEnabled`);
  - `mcp/McpToolRegistry.java` and `web/WebServer.java`: module and cross-project tools on readers return "not available on reader nodes yet";
  - `pharos` (Python): `publish` subcommand, and exit code 3 handling in `cmd_index`;
  - `README.md` ("Multi-node (experimental)" section with a writer and a reader config example).
- Test: `src/test/java/com/pharos/cluster/ClusterRoundTripTest.java`, `ClusterRoundTripMinioTest.java`, `RoleGuardTest.java`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `ObjectStoreBackend(IndexConfig)` implements `ReplicationBackend`.
    - Writers get `LocalFsCache` and an `ObjectStorePublisher`; readers get `ReaderLocalCache` and `NoOpPublisher`.
    - `catalog()` returns an `ObjectStoreCatalog` and `source()` an `ObjectStoreReplicaSource`.
  - `ClusterRuntime(IndexConfig, ReplicationBackend, ProjectRegistry, LuceneIndexer)`:
    - `void startServing() throws IOException`: for readers, it builds the `ReplicaSyncer`, runs `syncOnce()` synchronously (logging the shards served), then calls `start()`; it is a no-op otherwise;
    - `IndexPublisher publisher()`, `void close()`.
  - `RoleGuard.requireIndexing(IndexConfig)` throws `IllegalStateException("this node has role reader; run indexing on a writer node")`.
  - `SourceReader.setEnabled(boolean)`: when disabled, `readRange` returns null.
  - `pharos publish <project>` (Java and `./pharos`):
    - requires the writer role;
    - builds the `LocalSnapshot` from the registry meta and `LocalFsCache`;
    - prints `published <project> generation <N>`, with exit code 0, 3 on `PublishException`, 2 on a role error.

- [ ] **Step 1: Write failing tests.**
  - `ClusterRoundTripTest`: one JVM, two `IndexConfig`s in separate temp dirs: a writer and a reader sharing one `InMemoryObjectStore`. Inject the store through a package-private `ObjectStoreBackend(IndexConfig, ObjectStore)` constructor.
    - `readerServesSameResultsAsWriter`: index sample-app and sample-python-app on the writer; reader `startServing()`; `SingleNodeGoldenTest.snapshot(readerEngine, readerRegistry)` equals `snapshot(writerEngine, writerRegistry)`. Call `SourceReader.setEnabled(false)` for the comparison (and reset it in `@AfterEach`). The toggle is JVM-wide, and a writer normally reads class bodies from disk, which a reader never does by design.
    - `newGenerationVisibleAfterOnePoll`: edit a source file, re-index incrementally on the writer, then one reader `syncOnce()`. A query for the new method name returns it on the reader.
    - `readerIgnoresStaleSourceFile` (Review Focus 4): the reader config resolves file paths to the writer's source dir. Overwrite one source file with different text after publishing; reader `getClassContext` returns the indexed body, not the new file text.
    - `publishFailureExitCode3` (Review Focus 2): the writer store throws on `put`. `IndexCommand` exits 3, the message contains `pharos publish sample-app`, and the writer's local registry has the project.
  - `ClusterRoundTripMinioTest`: `readerServesSameResultsAsWriter` against MinIO, with `@Testcontainers(disabledWithoutDocker = true)`.
  - `RoleGuardTest`:
    - `readerRefusesIndexing`: `IndexCommand` on a reader config exits non-zero with the guard message, and nothing is created under the cache dir;
    - `writerAndAllMayIndex`.

- [ ] **Step 2: Run to verify they fail.** Run: `mvn test -Dtest='ClusterRoundTripTest,RoleGuardTest'`. Expected: compilation failure.
- [ ] **Step 3: Implement.** Wire `Main` with `ReplicationBackend backend = Backends.create(config)`, the reader registry, `ClusterRuntime` and `SourceReader.setEnabled(role != READER)`. Add `publish` to `./pharos` the same way `cmd_backfill_embedding_cache` spawns the jar. Write the README section.
- [ ] **Step 4: Run.**
  - `mvn test`. Expected: same baseline as the Task 5 gate, plus the new tests passing.
  - `./pharos publish nonexistent` on a default (all/local) config. Expected: exit 2 with "requires role writer".
- [ ] **Step 5: Commit.** Message: `cluster: object-store backend wiring, node roles and publish command`.

### Task 12: Phase 1 gate run against MinIO (pharos + lucene)

**Files:**
- Create: `scripts/cluster-gate.sh`, `scripts/cluster_gate.py`
- Create: `docs/superpowers/gates/2026-11-15-phase-1.md` (the recorded results)

**Interfaces:**
- Consumes: `./pharos`, `pharos publish`, writer and reader configs (Task 11); `eval/benchmark.py` `QUERIES`.

- [ ] **Step 1: Write `scripts/cluster-gate.sh`.**
  - Download the MinIO server binary to `~/.cache/pharos-gate/minio` (Docker-free) and start it on `:9000` with data in a temp dir. Create a bucket `pharos-gate` with the bundled `mc` or with an S3 `CreateBucket` from `cluster_gate.py`.
  - Create isolated homes `$GATE/writer` and `$GATE/reader` (`-Duser.home`) with `cluster` configs (writer/reader, endpoint `http://127.0.0.1:9000`, path-style, credentials via env).
  - On the writer, index pharos (this repo) and the lucene checkout. Take the lucene source path read-only from `~/.pharos/registry.json` `rootPath`, and use `--no-embed`.
  - Start the writer web on `:7300` and the reader web on `:7301`.
- [ ] **Step 2: Write `scripts/cluster_gate.py`.**
  - For every `eval/benchmark.py` query whose project is pharos or lucene, compare the top-10 ids from `/api/search?pipeline=keyword` on both ports. Also compare `/api/callers` for the top result of each query.
  - Print mismatches, and exit 1 on any mismatch.
  - Then append a method to a copy of one pharos source file, re-index the writer incrementally, record `t0` after the publish, and poll the reader until the method is found. Print the elapsed time, the poll interval and the generation download time from the reader log.
- [ ] **Step 3: Run the gate.** Run: `scripts/cluster-gate.sh`. Expected:
  - 0 mismatches;
  - "visible after X s" with X ≤ 10 s + the logged download time;
  - exit 0.
- [ ] **Step 4: Record the results** (query count, mismatches, visibility time, generation sizes, reader cache size) in `docs/superpowers/gates/2026-11-15-phase-1.md`.
- [ ] **Step 5: Commit.** Message: `cluster: phase 1 gate script and results`.
