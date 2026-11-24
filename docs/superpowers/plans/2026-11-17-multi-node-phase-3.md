# Multi-node Phase 3: Multi-shard Fan-out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Any reader can take any query.
- Readers announce themselves in the bucket, and clients reach them through one URL: `pharos router` or an external load balancer.
- When a reader doesn't hold every shard a query needs, it coordinates. It embeds the query once, fans out to the readers that do hold those shards, merges with global BM25 statistics, and applies graph boost and reranking once over the merged lists.

**Architecture:**
- **Membership:** heartbeat objects in the bucket.
- **Routing:** a `ShardRouter` maps a query's projects to shards and live readers.
- **Internal API:** each reader exposes token-protected endpoints for shard search, term statistics, snippets and callees.
- **Pipeline split:** shard-local retrieval returns **raw candidates** (raw scores plus the inputs the boosts need), and the score shaping that today happens per candidate list runs on the merged list.
- **Local path:** a reader that holds every requested shard keeps today's local path unchanged. Fan-out is used only when shards are pinned elsewhere.

**Tech Stack:** Java 25, Lucene 10.4 (`IndexSearcher` term and collection statistics), Javalin (the existing web server), JDK `HttpClient`, Jackson, JUnit 5, MinIO (gate).

**Spec:** `docs/superpowers/specs/2026-11-15-multi-node-design.md` (snapshot of https://claude.ai/artifact/F1XtggNDsoToZMc64tSeKh rev 58; the live doc is rev 61). The relevant sections are "Query path", "Reader membership", "Request distribution" and "Phases and gates".

**Base:** branch `feat/cluster-phase-3` (worktree `../pharos-cluster3`), created from `feat/cluster-phase-2`. Phase 2 has finished Tasks 1–5; its gate (Task 6) is still pending. This plan starts after the Phase 2 gate passes. If the gate forces changes, rebase first.

## Global Constraints

- **Phase 3 gate (spec):** "Cross-project query ranking matches single node (rank correlation on the eval set); adding or removing a reader under load fails no queries."
- **Membership (spec):**
  - Each reader heartbeats into `catalog/readers/<node>.json` with a **TTL of 30 s**, listing the shards it serves.
  - `ShardRouter` spreads each shard's queries across live readers and drops a reader whose heartbeat lapses.
  - "By default every reader can serve every shard … config can pin a reader to a subset."
- **Query path (spec):**
  - Embed once at the coordinator and send the vector.
  - Each shard returns its top k with BM25 scores, vector scores and its `inDegree`/`maxInDegree` stats.
  - BM25 uses global term statistics, in DFS query-then-fetch style.
  - Hybrid fusion, graph boost (with a global `maxInDegree`), neighbourhood expansion and cross-encoder rerank all run at the coordinator.
  - Snippets and bodies come from the owning shard.
- **Request distribution (spec):** external routing (`/health`), `pharos router` (stateless, round-robin, skips readers failing `/health`), and `GET /cluster/readers`. "A draining reader answers `/health` with 503."
- **Single node is unchanged:** `role: all` never constructs Phase 3 classes, and `SingleNodeGoldenTest` stays byte-identical. The pipeline split (Task 4) must keep the local path's results bitwise identical.
- **A coordinator that holds every requested shard runs the local path unchanged.** The default is every reader holds every shard, so the default single-reader cluster behaves exactly as in Phase 2.
- **Config:** extends `cluster` and `cluster.reader`:
  - `cluster.advertiseUrl`: the base URL other nodes use. Required on readers.
  - `cluster.token`: a shared secret, required on readers and routers. It is sent as `X-Pharos-Cluster-Token`.
  - `cluster.reader.shards`: a list of project names. Empty means all.
  - `cluster.heartbeatSeconds`: default 10.
  - `cluster.readerTtlSeconds`: default 30.
  - `cluster.fanoutTimeoutMillis`: default 5000.
  - `cluster.connectTimeoutMillis`: default 1000.
  - Validation names the field.
- **Internal endpoints** live under `/internal/` on the reader's web server. They require the token and return 401 without it. `pharos router` never proxies `/internal/`.
- **Isolation:** tests and the gate never touch `~/.pharos` or the user's daemon on :7171. `./pharos` stays in sync with new commands and flags (CLAUDE.md).

**Out of scope here, deliberately:**
- The cross-project graph shard. Graph tools that span projects still answer "not available on reader nodes yet".
- Writers, leases, `/cluster/status`, proposals and the Cluster page (Phase 4).
- Peer pull (Phase 5).
- Custom shard keys (Phase 6). A shard is still a project.

**Deviations from the spec text, decided here:**
- **When to fan out.** A coordinator that holds every shard a query needs searches locally, using today's `MultiReader` and exact single-node semantics, even if other readers exist. Fan-out happens only for shards it doesn't hold. This keeps the default deployment (every reader holds everything) at single-node quality with zero network hops. The spec's fan-out applies wherever shards are pinned.
- **Heartbeat liveness uses the bucket's `LastModified`, not node clocks.** This makes it immune to clock skew between nodes.

## Review Focus

1. **A reader dies mid-query without draining.** The coordinator retries that shard once on another live reader that serves it. If none exists, the response carries the other shards' results plus a warning naming the missing shard. The coordinator never silently drops a shard, and never fails the whole query when only one shard is lost. Test owned by Task 5.
2. **A dead reader's heartbeat is still within its TTL.** Calls to it fail fast (connect timeout), and the reader is marked suspect for one TTL, so it gets no new shard calls. The gate's "remove a reader under load" case depends on this. Test owned by Task 2.
3. **Two replicas of a shard serve different generations during a swap.** Within one query, every request for a shard (stats, search, snippets) goes to the same reader, which is pinned per query. Each shard response states its generation, and the coordinator never mixes generations of one shard. Test owned by Task 5.
4. **A missing or wrong cluster token on `/internal/`.** The endpoint answers 401 and does no work. The router refuses to forward `/internal/` paths. A reader without a configured token fails at startup. Test owned by Task 3.
5. **Global statistics change ranking only where they should.** A single-shard query through fan-out (the coordinator doesn't hold that shard) returns bitwise the same results as that shard's local search. A local query on a reader holding everything is unchanged. Test owned by Task 5.

## File Structure

- `cluster/membership/`:
  - `ReaderHeartbeat.java`: record `{node, advertiseUrl, shards, startedAt, draining, inFlight, qps, readerCache}`.
  - `ReaderRegistry.java`: writes our heartbeat; reads and caches the others.
  - `ReaderHealth.java`: suspect marking.
- `cluster/routing/ShardRouter.java`: projects → shards → candidate readers; selection.
- `cluster/internal/`:
  - `InternalApi.java`: Javalin routes and token filter.
  - `ShardSearchRequest.java` / `ShardSearchResponse.java`, `TermStatsRequest.java` / `TermStatsResponse.java`: wire records.
  - `ShardClient.java`: HTTP client with timeouts and retry-on-another-replica.
- `search/distributed/`:
  - `RawCandidate.java`: raw score, plus the boost inputs (`inDegree`, `filePath`, `docType`, `classType`, `access`), plus the result fields.
  - `CandidateShaper.java`: today's `KeywordSearchStrategy.toResults` second pass, extracted.
  - `GlobalStats.java`: summed term and collection stats.
  - `GlobalStatsSearcher.java`: an `IndexSearcher` subclass that answers `termStatistics`/`collectionStatistics` from `GlobalStats`.
  - `DistributedSearch.java`: the coordinator.
- `search/SearchEngine.java`, `search/KeywordSearchStrategy.java`, `search/VectorSearchStrategy.java`, `search/pipeline/UnifiedRetrievalStage.java`: the pipeline split and the searcher factory seam.
- `cli/RouterCommand.java` (`pharos router`), `web/WebServer.java` (`/cluster/readers`, `/health` 503 when draining, the drain hook), `./pharos`, README.
- `scripts/cluster-gate-phase3.sh`, `scripts/cluster_gate_phase3.py`, `docs/superpowers/gates/<date>-phase-3.md`.

---

### Task 1: Reader heartbeats and `/cluster/readers`

**Files:**
- Create: `cluster/membership/ReaderHeartbeat.java`, `ReaderRegistry.java`
- Modify: `config/ClusterConfig.java` (`advertiseUrl`, `token`, `heartbeatSeconds`, `readerTtlSeconds`, `reader.shards`), `cluster/ClusterRuntime.java` (start and stop the registry on readers), `web/WebServer.java` (`GET /cluster/readers`)
- Test: `cluster/membership/ReaderRegistryTest.java` (InMemoryObjectStore with an injectable clock and LastModified), `config/ClusterConfigTest.java`

**Interfaces:**
- Produces:
  - `ReaderRegistry(ObjectStore store, ObjectStoreLayout layout, ClusterConfig cfg, Supplier<ReaderHeartbeat> self)`.
  - `start()` writes now and then every `heartbeatSeconds`. `drain()` sets `draining=true` and writes immediately. `close()` deletes our object.
  - `List<ReaderHeartbeat> live()` returns the cached list, refreshed every `heartbeatSeconds`. An entry is excluded when its `LastModified` is older than `readerTtlSeconds` by the store's time.
  - `ObjectStore.list` must expose `lastModified`. Add it to `ObjectInfo` if it's missing; the S3 `ListObjectsV2` result provides it.
  - The key layout is `ObjectStoreLayout.readerKey(String node)`, which gives `catalog/readers/<node>.json`.
  - `GET /cluster/readers` returns `{readers:[{node, advertiseUrl, shards, draining, ageSeconds}]}`.
- [ ] **Step 1: Tests.**
  - `heartbeatAppearsAndRefreshes`
  - `expiredByStoreTimeNotNodeClock`: the store's LastModified is old while the node clock says fresh, so the entry is excluded.
  - `drainingIsVisible`
  - `closeRemovesHeartbeat`
  - `pinnedShardsAdvertised`
  - Config tests: a reader without `advertiseUrl` or `token` fails, naming the field; `reader.shards` must name valid ShardIds.
- [ ] **Step 2:** Run the tests. They fail.
- [ ] **Step 3:** Implement. The heartbeat writer runs on a daemon thread. A write failure logs a WARN and retries on the next tick, and it never takes the reader down.
- [ ] **Step 4:** Run the tests. They pass.
- [ ] **Step 5: Commit** `cluster: reader heartbeats and /cluster/readers`.

### Task 2: Pinned shards, `ShardRouter` and suspect readers

**Files:**
- Create: `cluster/routing/ShardRouter.java`, `cluster/membership/ReaderHealth.java`
- Modify: `cluster/ReplicaSyncer.java` and `ClusterRuntime.java` (a pinned reader syncs only its shards and unregisters the others)
- Test: `cluster/routing/ShardRouterTest.java`, `cluster/ReplicaSyncerTest.java` (pinning)

**Interfaces:**
- Produces:
  - `record Route(ShardId shard, List<ReaderHeartbeat> candidates)`.
  - `ShardRouter.route(Collection<String> projects)` returns `List<Route>` in shard order. Empty projects means every shard in the catalog.
  - Candidates are live readers that are neither draining nor suspect and that serve the shard, ordered by least `inFlight` with round-robin among ties.
  - `ReaderHealth.markSuspect(node)` excludes the reader for `readerTtlSeconds`. A successful call clears it.
  - `boolean ShardRouter.servesLocally(ShardId)` is true when this reader holds the shard.
- [ ] **Step 1: Tests.**
  - `routesOnlyToReadersServingTheShard`
  - `skipsDrainingAndSuspect`
  - `spreadsAcrossReplicas`: 1 000 routes over 3 replicas, each between 30 and 37%.
  - `suspectClearsAfterTtl`
  - `pinnedReaderSyncsOnlyItsShards`
  - `unpinningSyncsTheRest`
  - `noLiveReaderForShardYieldsEmptyCandidates`
- [ ] **Step 2:** Run the tests. They fail.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. They pass.
- [ ] **Step 5: Commit** `cluster: shard routing over live readers; pinned readers`.

### Task 3: Internal shard API with token auth

**Files:**
- Create: `cluster/internal/InternalApi.java`, `ShardClient.java`, and the wire records
- Modify: `web/WebServer.java` (mount `/internal/*` on readers only)
- Test: `cluster/internal/InternalApiTest.java` (Javalin on an ephemeral port)

**Interfaces:**
- Produces:
  - `POST /internal/term-stats`: `{shard, fields, terms[]}` → `{generation, docCount per field, sumTotalTermFreq, sumDocFreq, per-term docFreq and totalTermFreq}`.
  - `POST /internal/shard-search`: `{shard, generation?, retriever, request, queryVector?, globalStats?}` → `{generation, candidates: [RawCandidate]}`. `retriever` names one retrieval stage of the configured pipeline (`keyword`, `vector`, `unified`). `generation`, when given, must match the served one; otherwise the reader answers 409 with its generation.
  - `POST /internal/snippets`: `{shard, generation, ids[], query, queryVector?}` → snippets.
  - `POST /internal/callees`: `{shard, generation, fqns[]}` → `{fqn: [callees]}`.
  - Every endpoint requires a valid `X-Pharos-Cluster-Token`, compared in constant time. A missing or wrong token gets 401 and the handler never runs.
  - `ShardClient.call(Route, Function<ReaderHeartbeat, Req>)` uses the connect and request timeouts. On a connection error or 5xx it marks the reader suspect and retries once on the next candidate. It surfaces `ShardUnavailableException(shard)` when no candidate is left.
- [ ] **Step 1: Tests.**
  - `noTokenIs401AndHandlerNotInvoked`
  - `wrongTokenIs401`
  - `generationMismatchIs409WithServedGeneration`
  - `retriesOnceOnAnotherReplica`
  - `unavailableWhenNoReplicaLeft`
  - `readerWithoutTokenFailsStartup`
  - `internalRoutesAbsentOnRoleAll`
- [ ] **Step 2:** Run the tests. They fail.
- [ ] **Step 3:** Implement. The handlers delegate to Task 4's raw-candidate retrieval and to the existing snippet and callee code.
- [ ] **Step 4:** Run the tests. They pass.
- [ ] **Step 5: Commit** `cluster: internal shard API with token auth`.

### Task 4: Pipeline split: raw candidates, shaping and global statistics

This is the riskiest task. It must leave local results **bitwise identical**.

**Files:**
- Create: `search/distributed/RawCandidate.java`, `CandidateShaper.java`, `GlobalStats.java`, `GlobalStatsSearcher.java`
- Modify:
  - `search/KeywordSearchStrategy.java`: `toResults` becomes raw retrieval plus `CandidateShaper.shape(List<RawCandidate>, type, intent)`, and the local path calls both.
  - `search/VectorSearchStrategy.java` and `search/pipeline/UnifiedRetrievalStage.java`: the same split. Every `new IndexSearcher(reader)` on a search path goes through a `SearcherFactory` that is a plain `IndexSearcher` by default.
- Test: `search/distributed/PipelineSplitParityTest.java`, `GlobalStatsTest.java`, plus `SingleNodeGoldenTest` (unchanged)

**Interfaces:**
- Produces:
  - `List<RawCandidate> RetrievalStage.retrieveRaw(IndexSearcher, SearchRequest, SearchTrace)`. It is a default method that throws for stages that don't support it. The keyword, vector and unified stages implement it.
  - `List<SearchResult> CandidateShaper.shape(List<RawCandidate>, String type, String intent)`. This is today's second pass: in-degree normalisation by the max over **the given list**, source-path penalty, doc-type and class-type bonuses, and the access multiplier, then a sort.
  - `GlobalStats.merge(List<TermStatsResponse>)`.
  - `GlobalStatsSearcher(IndexReader, GlobalStats)`, which overrides `termStatistics` and `collectionStatistics`.
- [ ] **Step 1: Tests.**
  - `localResultsBitwiseIdentical`: for the eval set's pharos and sample-app queries on every pipeline type, results (ids, order and float scores) before and after the split are equal. Record the old outputs as a fixture before changing any code.
  - `globalStatsSearcherOverSingleShardEqualsPlainSearcher`: global stats built from one shard give identical scores.
  - `twoShardGlobalStatsEqualMultiReader`: BM25 scores over shard A with stats summed from A and B equal the scores of the same docs in a `MultiReader(A, B)`.
  - `SingleNodeGoldenTest` unchanged.
- [ ] **Step 2:** Run the tests. The new ones fail to compile.
- [ ] **Step 3:** Implement as a pure refactor first. Commit once the parity tests are green, then add `GlobalStats`.
- [ ] **Step 4:** Run the tests, then `mvn test` once.
- [ ] **Step 5: Commits:**
  - `search: split retrieval into raw candidates and shaping (no behaviour change)`
  - `search: global term statistics searcher`

### Task 5: The coordinator

**Files:**
- Create: `search/distributed/DistributedSearch.java`
- Modify: `search/SearchEngine.java` (when this is a reader and `ShardRouter` says some requested shard isn't local, delegate to `DistributedSearch`), the graph-tool entry points in `SearchEngine` and `McpToolRegistry` (a project's graph call goes to a reader serving that shard when it isn't local)
- Test: `search/distributed/DistributedSearchTest.java` (two or three in-JVM reader nodes on ephemeral ports over one InMemoryObjectStore, using `ClusterFixture`)

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces the coordinator flow:
  1. Route and pin one reader per shard for this query.
  2. Classify and embed once, locally.
  3. Gather term stats from every shard: local shards directly, remote ones via `/internal/term-stats`. Merge them into `GlobalStats`.
  4. For each retriever in the pipeline, fetch raw candidates from every shard with `globalStats` and the vector. Concatenate, then take the top `k × oversample` by raw score per retriever.
  5. `CandidateShaper.shape` over each merged list. This gives the global `maxInDegree` per list, the same rule as today.
  6. Run the pipeline's merger and rerankers. They work on result fields only. A grep found no reranker that reads the index; the implementer confirms this for every configured merger and reranker, and adds a test that fails if one does.
  7. Neighbourhood expansion through `/internal/callees` on the owning shard. Snippets through `/internal/snippets` on the same pinned reader.
  - Response warnings: one per unavailable shard, `shard <name> unavailable: <reason>; results exclude it`.
- [ ] **Step 1: Tests.**
  - `crossProjectFanOutMatchesSingleNode`: reader A serves {pharos-fixture} and reader B serves {sample-app}. A query with no project filter on A gives BM25 top-10 identical to a single node holding both. For hybrid, Kendall τ ≥ 0.95 over the fixture query set. Vector top-10 is identical within the ulp tolerance from the Phase 2 gate.
  - `singleShardThroughFanOutIsBitwiseLocal`: Review Focus 5.
  - `localWhenCoordinatorHoldsAll`: no `/internal` call is made, which a call counter checks.
  - `deadReplicaRetriedElsewhere` and `noReplicaGivesPartialWithWarning`: Review Focus 1.
  - `perQueryPinningNeverMixesGenerations`: replicas on different generations; every call for a shard in one query hits the same reader. Review Focus 3.
  - `graphToolsRouteToOwningShard`
- [ ] **Step 2:** Run the tests. They fail.
- [ ] **Step 3:** Implement. Shard calls run in parallel on a bounded executor.
- [ ] **Step 4:** Run the tests, then `mvn test` once.
- [ ] **Step 5: Commit** `search: distributed coordinator with global statistics and per-query shard pinning`.

### Task 6: Draining, `/health` and `pharos router`

**Files:**
- Create: `cli/RouterCommand.java`
- Modify: `web/WebServer.java` (`/health` answers 503 with `{"status":"draining"}` while draining), `cluster/ClusterRuntime.java` (a shutdown hook drains: heartbeat `draining=true`, wait up to `drainSeconds` (default 20) for in-flight requests to reach 0, then stop), `Main`/`CodeSearchCommand` wiring, `./pharos router`, README
- Test: `cli/RouterCommandTest.java`, `web/DrainTest.java`

**Interfaces:**
- Produces:
  - `pharos router --port <p>` reads `ReaderRegistry.live()`. It round-robins `/api/*` and `/health` across readers whose last `/health` check, polled every 2 s, returned 200.
  - It retries an idempotent request (any `GET`, and `POST /api/search`) once on another reader after a connect error or 5xx. It never forwards `/internal/*` and answers those with 404.
  - It needs `cluster.token` only to read the bucket, and no index.
  - Draining makes in-flight requests complete and new ones see 503 through the router.
- [ ] **Step 1: Tests.**
  - `routerSpreadsAcrossReaders`
  - `routerSkipsUnhealthyAndRetriesOnce`
  - `routerRefusesInternalPaths`
  - `drainingAnswers503ButFinishesInFlight`
  - `heartbeatShowsDrainingBeforeStop`
- [ ] **Step 2:** Run the tests. They fail.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests, then `mvn test` once, then `bash scripts/test-setup.sh`.
- [ ] **Step 5: Commit** `cluster: draining, health and pharos router`.

### Task 7: Phase 3 gate against MinIO

**Files:** `scripts/cluster-gate-phase3.sh`, `scripts/cluster_gate_phase3.py` (reuse the Phase 1/2 gate helpers by import), `docs/superpowers/gates/<date>-phase-3.md`

**Gate procedure.** Run under `systemd-run --user --scope -p MemoryMax=10G`, with the MinIO binary and heap conventions from earlier gates.

1. **Setup.**
   - The writer indexes pharos (with embeddings) and lucene (`--no-embed`) and publishes both.
   - Reader R1 is pinned to {pharos}, R2 is pinned to {lucene}, and R3 holds all.
   - A `pharos router` runs in front.
   - A single-node reference holds both projects locally.
2. **Ranking.**
   - Run the 99 pharos and lucene eval queries (`eval/benchmark.py`, filtered to those projects) with no project filter on R1 (forces fan-out) and on the reference, using pipelines `keyword`, `hybrid` and `hybrid-reranked`.
   - **Pass, keyword:** top-10 identical on at least 98% of queries.
   - **Pass, hybrid and hybrid-reranked:** mean Kendall τ ≥ 0.95, and top-1 identical on at least 95% of queries.
   - Report vector results under the Phase 2 ulp rule.
   - Also report R3 against the reference. Its local path must be identical.
3. **Elasticity.**
   - Through the router, drive about 20 QPS of the eval queries for 3 minutes, with a mix of filtered and unfiltered queries.
   - At t=60 s, start R4 (all shards). At t=120 s, drain R2 with SIGTERM while R3 still covers lucene.
   - **Pass:** 0 failed requests, and 0 responses with an "unavailable" warning.
   - Report the p95 before, during and after.
4. **Safety.**
   - Marker check: `~/.pharos` is untouched.
   - Kill by PID only. Leave :7171 alone.

- [ ] **Step 1:** Write the scripts. Verify them with `bash -n`, `--dry-run` and the statistics on canned inputs.
- [ ] **Step 2:** Run the gate. Record every number.
- [ ] **Step 3:** If a criterion fails, find the cause before tuning anything. Record each change. Never lower a bar silently: stop for a decision.
- [ ] **Step 4: Commit** `cluster: Phase 3 gate scripts and results`.
