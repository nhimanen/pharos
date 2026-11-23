# Round 4 report: rulings P6-R19 and P6-R20, gate run 5

**Status: DONE_WITH_CONCERNS. Gate run 5 did NOT pass (2c: 8 / 4 953; 2d keyword: the new 1e-4 drift clause).**
**Branch:** feat/cluster-phase-6, BASE acb5cfb, HEAD after this report: see `git log`. Nothing pushed.

## Commits
| SHA | Subject |
|---|---|
| 2dcc50f | cluster: readers keep a shard's previous generation answerable for a grace after switching (P6-R19 1, 2, 4, 6) |
| 6571b91 | cluster: GC keeps a generation superseded within the readers' grace (P6-R19 3) |
| bd22cd1 | cluster: the run after a reshard flip clears the pins that equal LATEST before its first commit (P6-R19 5) |
| 23697f7 | test: a lagging reader's split project is read at the older run from retained generations |
| e334e6c | search: a synonyms publish seen at different moments no longer leaves shards out (P6-R19, synonyms window) |
| 8f89d1a | search: raw scores in the trace; the gate compares raw vector scores (P6-R20) |
| 1f74519 | test: the peer pruning test runs without the grace (follow-up to 6571b91) |
| 7acb0c7 | docs: CHANGELOG and README |

## What was built

### P6-R19 (1, 2, 4): the previous generation stays answerable
- `cluster.reader.previousGenerationGraceSeconds` (`ClusterConfig.ReaderConfig`); `ClusterConfig.previousGenerationGrace()` =
  the setting, else `max(30 s, 3 × CATALOG_CACHE_SECONDS + 2 × heartbeatSeconds)` (35 s by default, 30 s in the gate with
  5 s heartbeats). 0 turns it off; negative is refused. `ClusterRuntime.CATALOG_CACHE` now derives from the same constant.
- `ReaderLocalCache`: an activation keeps the replaced generation dir as the shard's `Retained(dir, generation, until)`.
  One per shard; a newer activation or the end of the grace releases it (graph evicted, release listeners told).
  `LocalCache.retainedDir(project, generation)` (default empty) is the lookup.
- `LuceneIndexer`: on a path swap the old reader moves to `previousReaders` (instead of being retired) when the cache
  retains its dir; `openPinnedPreviousReader(project, gen)`; the cache's release retires it (closed after
  `RETIRE_DELAY`, pins hold references meanwhile).
- `LocalShardService`: `servesGeneration`, `pin(shard, generation)` (current, else retained, else current — the
  caller then refuses the mismatch), `pinRun(shard, run)`. Every `/internal` request is pinned at its generation.
- `InternalApi`: a requested generation the reader does not serve but retains passes the check.
  `TermStatsRequest` gains optional `generation` and `run` (additive; an older reader ignores them and answers its
  served generation, which the coordinator then handles as before).
- `DistributedSearch`: a pinned version's shard asks for its expected generation from its first call (local pin and
  remote term stats), every later call for its pinned one. `alignRuns`: when shards of a split project can't reach the
  newest run (their reader has not activated it), the others are moved back onto the older run if their readers
  (here or remote) still answer it from the retained generation — all or nothing (`onOlderRun`).
- `ReaderHeartbeat.retained` (NON_EMPTY, additive; the operator reads `/cluster/status`, untouched). `ShardRouter`
  counts a retained generation as serving (`answersGeneration`) in `servable` and `servingFirst`.
- `ReplicaSyncer`: pruning never deletes a retained dir; each pass calls `cache.expireRetained()`.

### P6-R19 (6): the activation hold is gone
The grace covers the hold's window (pre-flip coordinators route the previous version by pins; they are answered from
the retained generation for ≥ 30 s, the hold was 15 s), so `activationHold`/`flipSeen`/`setActivationHold` were removed:
rewritten shards activate at the flip at once. `ReshardJobTest.aCoordinatorWithThePreFlipCatalogStillReadsEveryFile`
was rewritten accordingly (served at the new pins at once, previous ones retained, released after the grace).

### P6-R19 (3): GC
- Reader disk: a retained dir is pruned only after its release (then it is an ordinary old generation).
- Bucket: `GenerationGc` gains `supersededGrace` (wired from `previousGenerationGrace()` in `ObjectStoreBackend`,
  `PeerBackend` and the reshard job's GC): a generation whose successor was published less than the grace ago is
  kept, also beyond `retentionGenerations` and with `retentionMinutes` 0. (With the defaults the 60 min minimum age
  already covered it; this matters for a reader that skipped a generation and for `retentionMinutes: 0`.)
- The writer's `DiskGuard` is not involved (it guards writer builds; readers hold no writer files).

### P6-R19 (5): pins that equal LATEST
**Choice: clear before the commits, no minPinned wait (rely on (1)).** The next run of a project whose catalog pins
equal every shard's `LATEST` clears them in one catalog CAS before its first commit (`clearPinsBeforeCommit`, both
publish paths). Nothing a reader serves changes (they serve exactly those generations); a coordinator that still has
the pins in its cached catalog asks for generations readers serve until the commits and retain for the grace after,
which is longer than the 3 catalog-cache periods any coordinator needs to re-read the catalog, so the wait buys
nothing. Guard (found while testing): within the grace after the flip (`setFlipSettle`, wired to the grace) the pins
are cleared after the commits as before, because a reader may not have activated the flip yet and an unpinned query
could then read a rewritten shard's previous generation at the same index run as the new version (N and N+1 share
one `projectGeneration`).

### Synonyms window
`KeywordSearchStrategy` keeps the analyzer a reload replaced for the grace (`setPreviousSynonymsGrace`, wired on readers
in `ClusterRuntime.internalApi`); `forSynonyms(hash)` returns the current strategy or a frozen view of the previous one.
Readers analyse a request with the view its hash names (`LocalShardService.keywordFor`; unified via `withKeyword`).
The 409 names the reader's own hash; a coordinator whose stats round finds a reader on the coordinator's previous
synonyms analyses the whole query with them (trace note "analysed with the previous synonyms"). Pin-only term-stats
calls and pure vector retrieval no longer send a synonyms hash (they analyse no text).

### P6-R20
`SearchTrace.rawScores()` (type → id → raw score), recorded by `CandidateShaper.shape` through a thread-local set
around the pipeline; web `searchMeta.rawScores` with `trace=true`. The internal shard result already carried raw
scores (`RawCandidate.rawScore`). Gate driver: ranking passes are fetched traced; the vector shared-score clause
compares raw vector scores; keyword pages with identical ids must stay within 1e-4 relative score drift.

## Tests (each fails without its fix; mutation-checked)
- `ReshardQueryTest.aCoordinatorRoutingThePreFlipVersionIsAnsweredByReadersThatSwitchedAtDifferentTimes`: readers x, y;
  a coordinator routing the pre-flip catalog (its own router on a frozen catalog, re-plans off); x switches, then y;
  every query complete, no warning, no re-plan. With grace 0: "no shard answered … generation changed".
- `ReshardJobTest.theRunAfterAFlipClearsThePinsBeforeItsFirstCommit`: after a flip and finishFlip, B takes over and runs;
  at every LATEST CAS of its run the pins are already empty; every file read once before/after the reader syncs, with
  the pinned and the fresh catalog. With the clear disabled: fails on "cleared before the first commit".
- `ReplanTest.aLateSynonymsPublishOnOneNodeStillAnswersComplete`: publish reaches the coordinator first, then (reverse)
  the reader first; complete, no warning, no re-plan. With the synonyms grace 0: "the reader is late: no warning" fails.
- `ReplicaSyncerTest.thePreviousGenerationIsAnsweredForTheGraceThenReleased` (+ lazy): answered during the grace, dir
  kept; a newer activation releases and prunes it; after the grace the reader is retired.
- `GenerationGcTest.aGenerationSupersededWithinTheGraceIsKeptThenCollected` (control without grace deletes it).
- `ReplanTest.aReaderSwappingAShardDuringTheQueryAnswersFromItsRetainedGeneration` (grace 0: fails).
- `RawScoreTraceTest.theTraceCarriesEveryResultsRawScore`; `ClusterConfigTest.previousGenerationGrace`.
- Updated: `ReshardJobTest` (hold → grace), the two P6-R16 `ReplanTest` cases (readers without retention, so the
  re-plan is still exercised), `SplitProjectSearchTest.aQueryNeverMixesTwoIndexRunsAcrossReaders` (older run read whole;
  left-out expectation kept after the grace), `ReplicationEndpointTest` (pruning test without the grace).

## Suites
- Local at 1f74519: 1 850 run, 0 failures, 0 errors, 5 skipped.
- Build server at 1f74519: 1 851 run, 0 failures, 0 errors, 15 skipped.
- (First runs at 7acb0c7 both failed only `ReplicationEndpointTest.prunedFileIs410WithCurrentGeneration`: the new GC
  grace kept the generation it expects pruned; fixed in 1f74519.)

## Gate run 5
See the "Round 4" section of docs/superpowers/gates/2026-11-23-phase-6.md. 2a PASS 19.7 %; 2b PASS (vacuous); 2c FAIL
8 / 4 953 (hybrid only, 3 s after the interrupted 5 → 6 flip); 2d keyword FAIL on the P6-R20 drift clause (1.85e-3,
pre-existing: identical in run 4), hybrid PASS τ 0.9996, vector PASS (0 raw-score diffs); 2e PASS 0 / 4 770; 3a/3b
PASS; 4a PASS 79 / 79; 4b PASS; 5 PASS.

## Bugs found
- (Outside the original design, found while testing) Without the activation hold, a coordinator whose catalog predates
  the reshard's *pin* reads a flipped version's rewritten shards unpinned at the same index run as the previous
  version and silently misses moved files. In production the flip's minPinned (3 catalog periods) makes every
  coordinator see the pins first, except one whose catalog reads keep failing (up to `maxCatalogAgeSeconds`); the hold
  only covered 15 s of that too. Not fixed; see concerns. The same N/N+1 same-run ambiguity is why bd22cd1 keeps the
  pins until after the commits within the grace after a flip.
- `ReplicationEndpointTest` follow-up (test only).

## Concerns
1. **2c, 8 hybrid failures at the 5 → 6 flip (not isolated).** All lucene shards were called remotely on gate-r2, which
   still served the ring-5 run; the request carried another generation (the new version's), and the alternate switch
   did not happen. Only hybrid, and remote calls on coordinators that hold the shards. Needs a debug pass with traces
   (heartbeat lag after a flip vs `servable`, the alternate switch after a vector round).
2. **2d keyword drift clause (needs a ruling).** The 1e-4 bar was set from round 3's typical value; the maximum was
   already 1.85e-3 in run 4 and is unchanged. Ids identical 99/99. It vanishes in the final state (drift 0 vs before),
   so it is tied to the "after" catalog state (both projects pinned + previous), likely which shards' statistics an
   unscoped query sums, not the split. Worth a follow-up; or relax the clause.
3. **Same-run ambiguity of N and N+1** (bug above): rewritten shards of a flipped version share the previous version's
   `projectGeneration`, so only pins distinguish them. A later fix: give rewritten shards a new run number, or make
   coordinators refuse to route a project with a `previous` version unpinned.
4. **Cost:** a reader holds up to two generations of each changed shard for the grace (disk, file handles, an extra
   open reader); lazy mode reads the retained one from the bucket, which GC now keeps for the grace.
5. **No minPinned wait for the pins clear** (choice in P6-R19 5); if (1) is ever disabled (grace 0) the takeover window
   of run 4 can come back for one catalog period.
6. Server: `~/builds/phase6-gate/run5` (3 GB) left for inspection with runs 1–4 (12 GB). Nothing of mine runs there.
