# Changelog

All notable changes to Pharos. Every release section starts with an `**Index format:**` line;
`scripts/release-notes.sh` (and the release workflow) fail without it.

## [Unreleased]

**Index format:** 2 (unchanged)

### Changed
- ArcadeDB, which stores the call graph, the module graph and the cross-project graph, is upgraded from
  25.1.1 to 26.11.1. No re-index is needed: 26.11.1 opens graphs written by 25.1.1 as they are and keeps
  writing them in their layout (a full re-index starts a new graph in 26.11.1's layout), and 1.11.1 still
  reads graphs this release wrote, so a downgrade or a mixed-version cluster needs no step either (readers
  import the published `graph.cgf.gz`, pharos' own format, which did not change). 26.11.1 also fixes a
  25.1.1 bug where a database occasionally reopened with no records after it was closed.
- A new graph database is now closed and reopened right after ArcadeDB creates it, so no write from its
  creation is still queued when the schema is added. This is the workaround for ArcadeDB
  [#13441](https://github.com/ArcadeData/arcadedb/issues/13441), which 26.11.1 still has. It replaces the
  1.10 workaround, which polled the dictionary file and called an ArcadeDB-internal wait.
- Damaged-graph detection (`index-status`, `upgrade-indexes`, publishing) reads ArcadeDB 26's multi-page
  dictionary file as well as 25.1.1's single-page one.
- A full index no longer truncates and rebuilds the indexes of the graph database it has just created
  (empty by construction). On ArcadeDB 26 that in-place rebuild of empty indexes made later lookups log
  "Compacted index … main page 0 has totalPages=N" (1,513 warnings for one lucene index, while the lookups
  were still right) and cost about 2 s; the graph phase of a lucene full index now takes 6.8 s, against
  6.5 s on 1.11.1.
- Indexed projects that use libraries pharos itself bundles can resolve slightly more or fewer calls,
  because JavaParser's reflection solver sees pharos' own classpath, which ArcadeDB 26 changes (lucene:
  +6,645 resolved calls, +2.8 %; pharos' own sources: −2, through the changed ArcadeDB API).
- The fat jar keeps a single Lucene release: the Lucene modules that ArcadeDB 26 adds are pinned to pharos'
  Lucene version.
- ArcadeDB 26 no longer prints its startup warnings (Truffle fallback runtime, channel interrupt) on every
  command.

## [1.11.1] - 2026-11-26

**Index format:** 2 (unchanged from 1.11.0)

The first published build of 1.11: the 1.11.0 tag produced no release, because the release workflow failed
in `scripts/test-setup.sh`. 1.11.1 has the same code as 1.11.0, so read 1.11.0's notes below (including its
index format change 1 → 2 and "Upgrading to 1.11.0") for what changed since 1.10.0. The container images
`ghcr.io/<owner>/pharos:1.11.1` and `ghcr.io/<owner>/pharos-operator:1.11.1` are the first ones published;
`deploy/` names them.

### Fixed
- `scripts/test-setup.sh`: two installer tests used an unstamped index as their "one format behind, still
  searchable" project. Unstamped reads as format 0, which 1.11's format 2 no longer serves, so the installer
  correctly asked to re-index and the tests failed. They now stamp the project with INDEX_FORMAT − 1, read
  from the source (test only; no runtime change).

## [1.11.0] - 2026-11-25

**Index format:** 1 → 2 (documents carry key fields for the search filters and shard keys; format-1
indexes stay searchable unfiltered under the N-1 rule, the next index run rewrites them as format 2;
format 0 is no longer read — see UPGRADING.md)

Multi-node Phase 6 (sharding and live resharding) and the Kubernetes operator v1. A project on an
object-store cluster can be split into shards by a **shard spec** (`[project, module]`,
`[project, ring(path, shards=N)]`, …) and resharded live with `pharos reshard`, while readers keep
answering; the same documents carry key fields that every node can filter on (`module`, `pathPrefix`,
`language`, `kind`). The operator deploys and runs a whole cluster from one `PharosCluster` resource.
Gates: the Phase 6 gate (two work-queue writers, three lazy readers, the router, 10 QPS throughout,
three live reshards of lucene and pharos and a writer killed mid-reshard) answered 0 of 4 883 queries
with a failure, kept keyword top-10s identical 99 / 99 (score-bitwise), hybrid τ 1.0000 and vector
overlap 0.997 after the reshards, re-indexed 19.7 % of lucene's files for a 4 → 5 ring change and
called the embedding model 0 times while resharding pharos (4 786 cache hits); the operator's
minikube end-to-end run passed 65 of 65 checks (install, scaling, takeover, writer scale-down,
pre-flight, upgrade, a live reshard, secrets, re-apply, teardown).

Single nodes: the format change makes each project's next `pharos index` a full run (or run
`pharos upgrade-indexes`); nothing else changes. Clusters: upgrade every reader before resharding
(UPGRADING.md "Upgrading to 1.11.0").

### Added (multi-node Phase 6: sharding and live resharding)
- Shard specs (object-store clusters): a project is split by shard keys: `project`, `module` (the
  nearest directory with a build file), `language`, `kind`, `prefix(path, depth=D)` and
  `ring(path, shards=N[, vnodes=V])` (a consistent-hash ring over file paths: growing it from N to
  N + 1 shards moves about 1/(N + 1) of the files). Every project starts as `[project]` (one shard, as
  before) and `pharos reshard` (below) sets its spec. A split project is one work unit on one writer
  (one lease per shard, taken all or none, one index run publishing every shard) and its shards are
  served like any other (`<project>~<key>=<value>` ids); the coordinator plans each query over the
  project's shards. `cluster.sharding.default` and `cluster.sharding.projects.<name>` are validated at
  startup (a non-default spec needs `cluster.backend: object-store`) but not applied yet.
- Search key filters: `module` (the module's directory relative to the project root, the
  nearest directory holding a build file — not the Maven artifactId; `_` for files outside any
  module), `pathPrefix` (a directory relative to the project root, whole path segments),
  `language` (the file extension, case-insensitive) and `kind` (`code` or `doc`), on `/api/search`
  (`module`, `pathPrefix`, `language`, `kind`), `pharos search` (`--module`, `--path-prefix`,
  `--language`, `--kind`) and MCP `search_code` (`module`, `path_prefix`, `language`, `kind`).
  They only select documents: scores are those of the unfiltered query. On a cluster they also
  prune the shards of split projects; `--trace` shows the routing notes, and a filter that matches
  no shard of a project says so in the warnings. Readers advertise filter support in their
  heartbeat; a coordinator never sends a filtered query to a reader without it.
- `cluster.reader.shards` accepts shard ids of split projects and project wildcards (`lucene~*`:
  every shard of the project).
- Live resharding (object-store clusters): `pharos reshard <project> --spec "[project, ring(path,
  shards=4)]"` requests a new spec version; `--status` shows the project's versions and the job's
  progress, `--cancel` abandons it. The writer holding the project builds the new shards from its
  index (a ring change re-indexes only the files that change shard; shards that neither gain nor
  lose files keep their generations; embeddings come from the cache), readers keep serving the
  current version and prepare the new one, and one catalog CAS flips routing once they have it
  (or after `cluster.writer.reshardSyncTimeoutSeconds`, default 600). A query reads one version:
  right after the flip it reads the previous one until its readers serve the new one. The retired
  version is removed after `retentionMinutes`. An index run never adds shards: a new module under
  `[project, module]` needs `pharos reshard` with the same spec.
  A writer whose local index is behind the published run (it got the project back after another
  writer published, e.g. a writer scale-down and up that kept its disk) catches up with a full
  run before it splits the project, so the new version never lacks that writer's files.
- Writer leases on a reshard: another writer taking any shard lease of the project (or the reshard
  lease) aborts the running build at its next safe point and releases the reshard lease within one
  renewal interval of the writer seeing it; a renewal that only fails to get an answer from a slow
  bucket does not (as for index runs since 1.9.0), so the build is not thrown away and the reshard
  stays with its writer.
- A lease loss without a definite answer (a renewal's I/O error or timeout) is now resolved while
  the run or reshard build goes on: every renewal round re-reads such a lease until the bucket
  answers, reclaims it when it is untouched (still this writer's last write) and treats it as taken
  otherwise. Before, nothing re-read it during a run, so only runs shorter than about one lease TTL
  survived a bucket blip: a longer one lost its lease to expiry, kept working for nothing (holding
  the reshard lease, which blocked the project's new owner), and failed at its publish. Now a long
  run or build survives the blip and publishes, and one whose lease was taken meanwhile is aborted,
  and its reshard lease released, within a renewal interval of the bucket answering.
- Resharding is visible: `/cluster/status` gains `projects[]` (a split project is one entry over
  its shards, with its staged and retired spec versions and the reshard's phase — requested,
  building, pinned, flipped, abandoned — progress in shards and in files, and the reshard lease
  holder, and `published`, `generation`, `docCount` and `pending` over its active shards only) and
  `specs[]` (every spec version: active, staged, requested, retired, abandoned or
  superseded); shards gain `project`, `role`, `staged` and `stagedGeneration` (fields are only
  added). `/cluster/events` sends a `reshard` event at each phase and at each quarter of the
  shards or files. The Cluster page shows a Resharding panel, one sharding table per spec version,
  and marks staged shards in the layout. The `reshard` proposal targets a project and names the
  exact command, e.g. `pharos reshard lucene --spec "[project, ring(path, shards=4)]"` (a ringed
  project gets one more shard, or more for its size). MCP `list_projects` on a reader lists a split
  project once, with its shards.

### Added (Kubernetes operator v1)
- `pharos-operator` (image `ghcr.io/<owner>/pharos-operator:<version>`): a namespaced operator for the
  `PharosCluster` resource (`pharos.dev/v1alpha1`, CRD in `deploy/crd/`, least-privilege RBAC in
  `deploy/rbac.yaml`, one replica). From one resource it runs the writers and readers as
  StatefulSets with volume claims, the router as a Deployment with its Service, PodDisruptionBudgets,
  NetworkPolicies, the rendered `config.json` (validated by `ClusterConfig` before it is applied) and
  a generated cluster token; secrets reach the pods only as environment variables, never a ConfigMap,
  the resource or its status. Examples: `deploy/examples/minio.yaml`, `deploy/examples/s3.yaml`.
- Storage: `storage.minio` (the operator deploys MinIO and creates the bucket with `mc`) or `storage.s3`
  (an existing S3-compatible bucket; its keys come from a Secret with `accessKey` and `secretKey`).
  MinIO no longer publishes images, so the defaults are the community builds
  `docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z` and `docker.io/pgsty/mc:RELEASE.2026-11-12T00-00-00Z`;
  `MINIO_IMAGE` and `MC_IMAGE` on the operator Deployment select a mirror. Deleting a `PharosCluster`
  keeps the bucket: the MinIO volume and its credentials Secret have no owner reference, so applying a
  cluster of the same name again adopts them (indexes and keys intact); writer and reader claims also
  outlive their StatefulSets. `kubectl delete pvc,secret -l pharos.dev/cluster=<name>` removes the data.
- Projects: `spec.projects[]` (`name`, `git.url`, `git.ref`, `git.credentialsSecret` for an SSH key or
  HTTPS credentials, `embed`) become Jobs: registration runs `pharos index --project <p> --source-git …
  --ref …` without a path, a removed project runs `pharos remove-index --from-catalog <p>`; the work
  queue does the indexing. `embeddings.model` picks the model as `pharos embeddings enable` does.
- Autoscaling: `writers` and `readers` take `min`/`max` ranges, and `autoscaling` (`cooldownSeconds`,
  `maxStepUp`, `maxStepDown`) applies pharos's own scaling proposals from `/cluster/status` within them.
  `autoscaling.triggers` set the thresholds pharos raises the proposals at (readers: warm p95, QPS per
  reader, load below a percentage; writers: backlog shards, oldest pending change, idle time), and
  `autoscaling.schedule` raises the floors on a timetable (5-field cron with a timezone). The last
  scale decision is kept on each StatefulSet, so cooldowns survive an operator restart; a writer is
  removed only once its heartbeat and leases are gone.
- Upgrades: changing `spec.version` first runs a pre-flight Job with the new image
  (`pharos upgrade-indexes --check`; a shard the new release would refuse blocks the upgrade with the
  `UpgradeBlocked` condition and nothing is applied), then rolls readers, writers and the router in that
  order, each once the previous role is rolled out and heartbeating. A downgrade rolls writers first and
  checks the bucket again before readers and the router move.
- Status: `phase` and the conditions `StorageReady`, `ReadersReady`, `WritersReady`,
  `ProjectsPublished` and `UpgradeBlocked`; per-project published state, generation and document count
  (a split project once, over its active shards); the scaling proposals in force.
- `pharosConfig`: extra `cluster.*` keys (for example `writer.reshardSyncTimeoutSeconds`,
  `reader.previousGenerationGraceSeconds`) merged into the rendered config. `pharos reshard` runs from a
  writer pod or a one-off Job.

**Installing the operator** (the images are published for each release from 1.11.0 on; on ghcr.io they
are private until the package is made public, otherwise add an image pull secret):

```bash
kubectl apply -f deploy/crd/ -f deploy/rbac.yaml        # CRD, namespace pharos, RBAC
# deploy/operator.yaml: replace OWNER with the lowercased GitHub owner and VERSION with 1.11.0
kubectl apply -f deploy/operator.yaml
kubectl apply -f deploy/examples/minio.yaml             # or s3.yaml, after creating its Secret
kubectl -n pharos get pharoscluster demo -w             # phase, conditions, projects
```

### Added (for the operator, also without it)
- Scaling proposals for an operator: every scaling proposal on `/cluster/status`, in
  `/cluster/events` and to the webhook carries a signed `delta` (`+N` start, `-N` stop, the N its
  `action` names; none on `reshard` and `writer-disk`). New `cluster.status` keys for what were
  constants: `removeReadersLoad` (0.2), `writers.backlogShards` (1),
  `writers.oldestPendingSeconds` (300) and `writers.idleSeconds` (the remove-writers window;
  unset, `windows.removeWritersSeconds` applies; both set to different values is a startup error).
  README "Scaling proposals".
- `cluster.tokenEnv`: read the cluster token from the named environment variable instead of
  `cluster.token` (mutually exclusive with it; surrounding whitespace is stripped; an unset or
  empty value, or one with inner whitespace or control characters, is a startup error).
- `pharos config validate <file>` (and `./pharos config validate <file>`): runs a node's startup
  validation on a config file; prints `ok` and exits 0, or prints the problem and exits 2.
- Registering a cluster source without indexing: `pharos index --project <name> --source-git <url>
  --ref <ref> [--no-embed]` with no path only records the source in the shard catalog (also
  `./pharos index` without a path). It publishes nothing, so a work-queue writer runs it too and no
  writer needs to be running; the work queue then indexes the project. `pharos remove-index
  --from-catalog <name>` drops a project from the catalog in one write (a split project's every
  shard, its staged and retired versions and an open reshard too): writers release it and readers stop
  serving it, the last project too (readers keep their shards only while the catalog object is
  missing, no longer when it lists no shard); its generations stay in the bucket, and registering
  it again resumes from them. The
  Kubernetes operator runs both as Jobs.
- Per-project git credentials on writers: `cluster.writer.gitCredentials` maps a project to a
  directory holding `ssh-privatekey` (optional `known_hosts`; without it host keys are learned on
  first contact into `<workDir>/.ssh-known-hosts`) and/or `username` and `password` (HTTPS). Only
  that project's git calls use them, through `GIT_SSH_COMMAND` and a credential helper that reads
  the files when git asks; the config holds paths only.
- Container image: arguments run one pharos command instead of the server (`docker run pharos:<v>
  index --project p --source-git <url> --ref main`).
- `pharos upgrade-indexes --check` changes nothing and exits 3 when this release would refuse an
  index: with an object-store backend it reads every catalog shard's `LATEST` manifest from the
  bucket (on any role, work-queue writers included), otherwise the local
  registry's projects. The Kubernetes operator runs it with the new image before an upgrade.
  A catalog entry this release cannot parse (a newer release's shard id) counts as refused, and
  readers no longer treat such a catalog as authoritative: they keep serving what they have instead
  of dropping every shard.

### Changed (upgrade note)
- `add-writers` is now raised when a shard's unpublished change is older than
  `cluster.status.writers.oldestPendingSeconds` (default 300 s), no longer
  `cluster.status.targets.backlog` (600 s), which now drives only the `shard-lagging` health
  finding. The proposal's metric `targetBacklogSeconds` is replaced by `targetOldestPendingSeconds`
  and `targetBacklogShards`. Set `writers.oldestPendingSeconds` to keep the old threshold.

### Changed
- Keyword search ranks equal scores in a stable, build-independent order (document id, type and line
  range) instead of by Lucene doc id, on a single node too: it reads the whole group of equal scores
  at the page's cut (up to 10 000 hits), so two builds of the same source (a re-index, a resharded
  project) return the same page, scores included. Results with equal scores may come back in another
  order than before, and where another tied document is kept at the cut the page's graph-boosted
  scores can change slightly.
- Multi-node: a query that loses a shard because a reader switched the shard's generation during
  the query (right after a reshard flip or a publish) or analyses with other synonyms (a synonyms
  publish not yet everywhere) is planned again once: the coordinator waits up to 2 s (at most half
  of the query's remaining budget) for its view to move on and runs the query again within the
  original timeout, at most 16 such re-plans at once. Those queries can take up to that much longer
  (the trace notes "re-planned once"); a re-plan that fails returns the first, partial answer with
  its warnings, as before.
- Multi-node: a reader keeps a shard's previous generation open and answerable for a grace after it
  activates a new one (`cluster.reader.previousGenerationGraceSeconds`; default max(30 s,
  3 × 5 s + 2 × `heartbeatSeconds`), 0 turns it off), so a coordinator still routing the previous
  generation (a reshard flip, a split project's next run, a reader lagging another) is answered
  instead of losing the shard. Requests carry their generation; heartbeats add `retained`; the
  bucket GC keeps a generation superseded less than the grace ago. A reader's disk and memory hold
  up to two generations of a changed shard during the grace. After a flip the rewritten shards are
  activated at once (the 15 s activation hold is gone), and the next run of the project clears the
  flipped version's pins before its first commit. Likewise the synonyms a publish replaced stay
  usable for the grace: a coordinator whose reader has not synced new synonyms yet analyses the
  query with the previous ones, and a reader answers a coordinator still on them.
- `?trace=true` search answers carry `searchMeta.rawScores`: each retrieval stage's raw
  (pre-graph-boost) score per result id, and `searchMeta.statistics`: the shards, generations and
  field totals a fanned-out query's statistics round read.
- Multi-node: a shard's first request names the spec version of its project the coordinator
  planned; a reader whose catalog has moved on answers with the generation that version names
  (the previous one, retained) or refuses, so a coordinator whose catalog predates a reshard never
  silently misses the files that moved. Every shard of a reshard's new version is at one new index
  run number: a shard that neither gains nor loses files gets a metadata-only manifest at that run
  (its Lucene files unchanged, nothing uploaded or downloaded again; readers hard-link them), so once
  the next run clears the reshard's pins a query no longer leaves such a shard out, or reads the
  rewritten shards' previous generations and misses a removed shard's files.
- Multi-node: `pharos reshard` is refused while a live reader runs an older release without
  sharding (a rolling upgrade in progress), and an open reshard holds before staging and before its
  flip until every live reader is upgraded: an older coordinator would otherwise route only the
  previous version and lose the project from its answers once retention dropped it (UPGRADING.md).
- Multi-node: a replica's generation mismatch no longer hides the first reader's "no vectors for
  the model" answer (a vector or hybrid query right after a flip left every shard of a project
  without vectors out).

### Fixed
- Multi-node: a reader frees the generations it prepared for a reshard that was cancelled or rebuilt
  within one sync pass (their dirs are deleted unless active or retained for the grace, and
  heartbeats stop reporting them); before, they stayed on disk until the shard's next activation.
- A reshard rewrites each affected shard whole: the moved files' documents are expunged (their
  deletes merged away) before the commit, so BM25 statistics count every document once. Those
  shards, HNSW vector graphs included, get new segment files, are re-uploaded in full and
  re-downloaded in full by readers. The expunge can be aborted (a graceful stop, a lost lease) and
  rolls back; the disk guard admits a reshard build only when about twice the project's Lucene
  files fit (old and new segments coexist until the commit).

### Build
- Multi-module Maven build: the root `pom.xml` is now the parent of `app` (pharos itself, sources
  still in `src/`), `pharos-api` (`ClusterConfig` and the `/cluster/status` records, Jackson only) and
  `operator`. `mvn clean package` at the root still produces `target/pharos-<version>.jar`; a release
  bumps every pom (`mvn versions:set`).
- Container images: `docker/pharos/Dockerfile` (the image the operator deploys: Temurin 25 JRE, the
  fat jar, python3 with `python-hcl2`, node, git; uid 10001; arguments run one pharos command) and
  `docker/operator/Dockerfile`. A release tag now also pushes `ghcr.io/<owner>/pharos:<version>` and
  `ghcr.io/<owner>/pharos-operator:<version>`.

### Maintainers
- The Phase 6 gate (`scripts/cluster-gate-phase6.sh`) gains main's gate settings: `GATE_KEYWORD_ONLY=1`
  (no embedding model on any node, pharos indexed `--no-embed`, only the keyword pipeline compared; a
  `GATE` dir set up in the other mode is refused), `GATE_ROUTER_PORT`, `GATE_R1_PORT`..`GATE_R3_PORT`,
  `GATE_W1_PORT`, `GATE_W2_PORT`, `GATE_PORT_PROBE_TIMEOUT`, a header showing how to run it under
  `scripts/gate-watchdog.sh`, and the jar picked by version (`sort -V`). The defaults are unchanged.
- `scripts/operator-e2e.sh` runs the operator's minikube end-to-end test (its own `pharos-e2e`
  profile with calico; images built inside minikube, no registry).

## [1.10.0] - 2026-11-25

**Index format:** 1 (unchanged)

Multi-node Phase 5: peer pull, a cluster **without a bucket** (experimental). With
`cluster.backend: peer-pull` each writer keeps its published generations in a local peer store and serves
them from its `pharos web` on the token-guarded `/replication/*` endpoint; readers find the writers by DNS
(`dns:<name>`, e.g. a Kubernetes headless service) or a seed list with gossip (`seeds:<url>,…`), long-poll
each shard for new generations, and fetch whole files from other readers that already hold them. Reader
membership and the cluster status state live on the writers, so coordinators, `pharos router` and
`/cluster/status` work as with a bucket. Optional TLS protects reader-to-writer pulls. Phase 5 gate: top-10,
callers and usages identical to a single node on 99/99 queries, a new publish visible to readers within
0.6 s, 0 failed queries over 2 minutes with the writer killed, and the Phase 2/3 cold, warm, fan-out and
router-elasticity criteria passed as in bucket mode. **Single-node installs and bucket clusters are unaffected**
by peer pull (`object-store` stays the default backend). The index format is unchanged, but run
`pharos index --full` once for every project and re-run `pharos link`: older releases could write a call
graph that silently decodes wrong (see Fixed; UPGRADING.md "Upgrading to 1.10.0").

**When to use it.** Peer mode is for small or on-premises clusters where running S3 or MinIO is not worth
it. Prefer the bucket when you have one: peer mode has **no writer takeover** (no leases; the Phase 4 work
queue is forced off, so writers index and publish by hand), a writer that is down publishes nothing until
it is back (its shards keep serving what readers hold; a lazy reader's cold reads of them fail), the writer's
disk is the only copy of its published generations, and `cluster.token` is the only credential, visible on
the network except reader-to-writer with TLS. Read README "Without a bucket: peer-pull" and UPGRADING.md
("Upgrading to 1.10.0").

### Added (multi-node Phase 5: peer pull without a bucket)
- Peer-pull backend without a bucket (experimental, README "Without a bucket: peer-pull"):
  `cluster.backend: peer-pull`. A writer keeps its published generations in a local peer store
  (`cluster.peers.storeDir`; Lucene files hard-linked) and its `pharos web` serves them on the
  token-guarded `/replication/*` endpoint (catalog, `latest` long-poll, manifests, whole files
  and `Range`s, synonyms; at most `maxTransfersPerWriter` transfers, more get 503 with
  `Retry-After`; optional TLS on writers via `cluster.tls`). Readers find writers through
  `cluster.peers.discovery` (`dns:<name>[:port]` or `seeds:<url>,…` with gossip limited to
  `allowHosts`), long-poll each shard's `latest` (`longPollSeconds`), merge the writers' catalogs
  (a writer missing for 3 refreshes is dropped; its shards keep serving their cached generation)
  and fetch whole files from peer readers that hold the generation (checked against the
  manifest; `maxTransfersPerReader`). Reader membership and the cluster status state live on the
  writers (`POST /replication/readers`), so coordinators and `pharos router` work without a
  bucket. No writer takeover in this mode (`cluster.writer.workQueue` is forced off). `/health`
  reports `peers` (writers, conflicts, `peerFetch` files and bytes; on a writer `transfers`).
  - In peer mode `cluster.token` is the membership credential. A writer admits a reader heartbeat
    only when its `advertiseUrl` host resolves to the request's address or is in
    `cluster.peers.allowHosts` (403 otherwise). The token travels in clear text on the reader tier
    even with `cluster.tls` (which protects reader-to-writer pulls only): restrict the network.
  - `/health` shows the discovery spec, writer URLs and peer URLs only with
    `X-Pharos-Cluster-Token` (as `/cluster/status`); the public view names nodes only.
  - With `cluster.tls.keystore` a writer also listens with plain HTTP on
    `127.0.0.1:<cluster.tls.localPort>` (default web port + 1, no `/replication/*`), which
    `./pharos` uses.
  - `cluster.peers.maxLongPolls` defaults to 4096; size it to readers × shards per writer.
- A peer reader's shutdown is bounded as a bucket reader's is (1.9.0): on a slow or frozen writer
  the draining heartbeat and the withdraw each give up after 5 s with a `Stopping: gave up …` WARN,
  so the drain stays within `cluster.drainSeconds`.

### Fixed
- `./pharos` in a repo checkout runs the newest `target/pharos-*.jar` by version number. It sorted
  the names as strings, so a leftover `pharos-1.9.0.jar` (a `mvn package` without `clean`) would have
  won over `pharos-1.10.0.jar`. The gate scripts pick their default jar the same way (`sort -V`).
- A newly created call graph could be written with a stale property dictionary (rare; every release
  since the ArcadeDB graph backend, single-node and clusters alike). ArcadeDB queues a new database's
  first dictionary page for asynchronous flushing, and pharos then rewrote that page synchronously;
  when the queued copy reached the disk last, every later open decoded the records' property names
  wrongly: methods and edges vanished from the graph and other vertices came back with shuffled
  properties, with no error. Any graph creation could hit it: a full index, a writer takeover's
  restore, the cross-project link graph, the module graph. A new graph now waits until that page is
  on disk before anything else is written.
- Graphs damaged that way by an older release are now detected when opened (pharos always added the
  dictionary names in a fixed order) and logged as a WARN with the repair:
  - a project's call graph: the project's next index run is a full one, which rebuilds it; work-queue
    writers (which refuse `index --full`) repair their shards this way on their own. A standalone
    `pharos index --full <project>` is still recommended, to find the damage before a run does. As a
    backstop a writer refuses to publish a damaged graph (and to export it as `graph.cgf.gz`), so
    readers and takeovers never receive it; `index-status` reports it (`graphDamage` in `--json`),
    and `upgrade-indexes` re-indexes it.
  - the cross-project graph: the next `pharos link` rebuilds it.
  - the module graph (`~/.pharos/module-graph.arcadedb`, never re-created by a re-index): rebuilt
    empty automatically; each project's modules return when it is next indexed.
  A reader serving a published `graph.cgf.gz` cannot check it (the file has no dictionary): re-index
  on the writer, which then publishes a clean generation.

### Maintainers
- Phase 5 gate (`scripts/cluster-gate-phase5.sh`): every port is a setting (`GATE_W1_PORT`,
  `GATE_W2_PORT`, `GATE_P1_PORT`, `GATE_REF_PORT`, `GATE_ROUTER_PORT`, `GATE_D1_PORT`/`GATE_D2_PORT`,
  `GATE_LOAD_BASE_PORT`, the Phase 2/3 drivers' `GATE_LAZY_PORT`/`GATE_FULL_PORT` and
  `GATE_R1_PORT`..`GATE_R4_PORT`; probe bound `GATE_PORT_PROBE_TIMEOUT`), `GATE_PHAROS_SRC` snapshots
  pharos from a git tree, and `GATE_SEED_FROM` seeds the pharos (and mypal) embedding caches; defaults
  unchanged. `GATE_KEYWORD_ONLY=1` loads no embedding model anywhere: pharos and mypal are indexed
  `--no-embed`, ranking compares the keyword pipeline only and the router load is keyword only; a
  `GATE` set up in the other mode is refused. The header shows how to run it under
  `scripts/gate-watchdog.sh`.

## [1.9.0] - 2026-11-24

**Index format:** 1 (unchanged)

Multi-node Phase 4: work-queue writers. Writers take shards from the catalog under leases, poll each
shard's source (a git remote or a shared directory), index and publish its changes on their own, share the
shards among however many writers run, and take over a dead writer's shards with its writer state restored
from the bucket (Phase 4 gate: a writer killed mid-commit or mid-embed was replaced within 25–31 s, the resume
re-embedded only the changed files, 51 changes over scaling 1→4→1 writers were each published exactly once,
0 of 23 881 queries failed). New: a writer disk guard, `/cluster/status` with an SSE stream and a webhook for
scaling proposals, and a **Cluster** page in the web UI. **Single-node installs are unaffected** (they gain the
Cluster page, showing one node). No re-index.

**Behaviour change for cluster writers: `role: writer` now defaults to the work queue**
(`cluster.writer.workQueue: true`). A writer config without the key no longer accepts hand-run
`pharos index` / `pharos publish`; add `"writer": {"workQueue": false}` to keep indexing by hand. Its
`cluster.heartbeatSeconds` must be below `cluster.writer.leaseTtlSeconds` (30), or it does not start. Read
UPGRADING.md ("Upgrading to 1.9.0") before restarting writers.

### Added (multi-node Phase 4: work-queue writers)
- Work-queue writers (README "Work-queue writers"): `role: writer` nodes take shards from the
  catalog under leases (`cluster.writer.leaseTtlSeconds`, 30), detect source changes every
  `pollSeconds` at a fixed rate, also during runs (`pharos index|publish --source-git <url> --ref
  <ref>` or `--source-path` record the source; `--no-embed` is recorded as `"embed": false` and
  work-queue runs of that shard skip embeddings), publish changes, share shards
  (`minHoldSeconds`, 120) and take over a dead writer's shards with its writer state restored from
  the bucket. Writer disk guard (`cluster.writer.disk`), `/cluster/status`, `/cluster/events`, a
  webhook for scaling proposals (`cluster.status`) and the Cluster page. A writer's `pharos web`
  no longer loads the search models.

### Changed (upgrade note)
- `cluster.writer.workQueue` defaults to `true` on `role: writer` (after the Phase 4 gate). A
  writer config without the key now runs the work queue and refuses hand-run `pharos index` /
  `pharos publish` (exit 2); add `"writer": {"workQueue": false}` to keep indexing and publishing
  by hand. On a work-queue writer `remove-index` and `embed` are refused too (exit 2), and
  `upgrade-indexes` / `./setup.sh --upgrade-indexes` (exit 5): restarting the writer on a new
  release upgrades it, because its work loop gives a full run (and a re-publish) to every held shard
  whose local or published index format, chunking version or model is out of date.
- `cluster.heartbeatSeconds` must be below `cluster.writer.leaseTtlSeconds` (default 30) on a
  work-queue writer, validated at startup: a Phase 1–3 writer config with `heartbeatSeconds` of 30
  or more stops starting after the upgrade until it is lowered.
- `cluster.writer.disk.minFreePercent` defaults to 0 (was 10): the threshold is `minFreeBytes`
  (5 GiB), so a writer on a large disk no longer stays "low" and publishes nothing. A low writer
  logs a WARN once per low episode.
- Only one process per writer node runs the work loop (`<workDir>/.work-loop.lock`); another daemon
  with the same config (e.g. one `./pharos` auto-starts) logs a WARN and serves without a loop.
- Synonyms: a publish replaces only its own shard's section of the cluster's `synonyms.txt`
  (conditional read-modify-write), so writers holding different shards no longer drop each other's
  rules, and every reader serves the same file.
- `git ls-remote` of a work-queue source is killed (with its helper processes) after
  `cluster.writer.lsRemoteTimeoutSeconds` (default 30); an unreachable remote stalls only its own
  shard, and its failed polls count toward `maxConsecutiveFailures`.
- A takeover that restores a generation with an older writer-state backup (the newest one was lost)
  runs a full index, so documents of files deleted since never stay behind.
- A writer's graceful stop is bounded when the bucket is slow or hangs (gate run 2: two writers
  needed SIGKILL 120 s after SIGTERM): the stopping heartbeat, the lease releases and the heartbeat
  delete each have a deadline, so a stop ends within `cluster.drainSeconds` + 10 s and logs each call
  it gave up on. A lease taken by another writer aborts that shard's run (a renewal that merely
  timed out does not), and a full run's parse checks the abort between files (it could only stop after the parser's last file).
- Object-store calls are bounded (new `cluster.objectStore.callAttemptTimeoutSeconds`, default 20,
  `callTimeoutSeconds`, default 60, and `uploadMinBytesPerSecond`, default 1 MiB/s): a heartbeat put
  on the gate's saturated MinIO took about 4 minutes (three 30 s socket timeouts). Uploads get the
  call bounds plus their size at the minimum rate, so large generation uploads are not cut; a
  download's body stays bounded by the 30 s idle-read timeout.
- A reader's SIGTERM drain is bounded on a slow bucket too (the same pattern, also in 1.8.1): the
  draining heartbeat waits at most 5 s (and never past `drainSeconds`) for a tick stuck on the
  bucket and for its put, the heartbeat delete on close at most 5 s, and each call given up on is
  logged (`Stopping: gave up …`); the drain's `drainSeconds` now includes the announce.

### Maintainers
- Phase 4 gate (`scripts/cluster-gate-phase4.sh`): ports (`GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`,
  `GATE_ROUTER_PORT`, `GATE_WEBHOOK_PORT`, `GATE_R1_PORT`/`GATE_R2_PORT`, `GATE_W1_PORT`..`GATE_W6_PORT`,
  `GATE_WLOW_PORT`), sources (`GATE_LUCENE_SRC`, `GATE_PHAROS_SRC`), `GATE_MODELS_SRC` and an optional
  embedding-cache seed (`GATE_SEED_FROM`) are configurable, with a bounded port probe and curl connects
  (`GATE_PORT_PROBE_TIMEOUT`). `GATE_KEYWORD_ONLY=1` runs failover, scaling, status and the disk guard with no
  embedding model on any node (K2 then kills the `GATE_K2_SHARD` holder in `GATE_K2_PHASE`). Run it under
  `scripts/gate-watchdog.sh`. Defaults are unchanged.

## [1.8.1] - 2026-11-24

**Index format:** 1 (unchanged)

The v1.8.0 tag produced no release: its release build failed in a test, so no 1.8.0 jar was published.
1.8.1 is 1.8.0 plus the test fix below; there are no product changes. Upgrade from 1.7.x straight to 1.8.1
and read the 1.8.0 notes below (and UPGRADING.md) for what changed.

### Fixed
- Build: `PipelineSplitParityTest` failed on GitHub runners with an AMD CPU without AVX-512. Lucene picks fused
  or unfused multiply-add per CPU, so every vector score moved by one ulp against the fixture recorded on an
  Intel machine. Surefire now pins `-Dlucene.useScalarFMA=true -Dlucene.useVectorFMA=true`, and the test
  skips on hardware that cannot reproduce the recorded bits (no FMA, vectors narrower than 256 bits).

## [1.8.0] - 2026-11-23

**Index format:** 1 (unchanged)

Multi-node Phase 3: multi-shard fan-out and the router. A reader no longer has to hold every project: a query
that needs shards held elsewhere is coordinated by the reader that received it, and `pharos router` spreads
requests over the healthy readers. Fan-out rankings equal a single node's (Phase 3 gate: keyword 99/99
bitwise, hybrid τ 0.9996, vector 99/99; 3 600/3 600 requests answered while a reader joined and another
drained). **Single-node installs are unaffected** apart from the multi-project vector change under Changed.
Requires no re-index; cluster readers should read UPGRADING.md (config validation, health probes, stop timeouts).

### Added (multi-node Phase 3: fan-out and router)
- `pharos router --port <p>` (`./pharos router`): a stateless router over the healthy readers. It polls each
  reader's `/health` every 2 s, round-robins, retries an idempotent request once on another reader, never
  forwards `/internal/*`, and answers 504 when a forwarded exchange (body included) exceeds 120 s. It needs
  the object-store config and `cluster.token`, but no index.
- Fan-out on readers: a reader that does not hold every shard a query needs coordinates it. It classifies
  and embeds the query once, gathers BM25 term statistics from every shard (so keyword ranking equals a single
  node's), fetches raw candidates from the readers serving the other shards, and applies graph boost, fusion
  and reranking once over the merged lists. Vector first passes and unified recall pools are merged in two
  rounds (global top candidates, then rescored on their shards). Snippets and neighbourhood expansion come
  from the owning shard; each query pins one reader and generation per shard. A reader holding every
  requested shard searches locally, unchanged.
- Graph and lookup tools (callers, callees, usages, call chains, call paths, method and class lookup) reach
  shards held elsewhere.
- Partial answers: a shard no reader can answer for is named in the response warnings
  (`shard <name> unavailable: <reason>; results exclude it`) and the other shards still answer. When none
  answers and the reader holds none of them, the call answers 503 (`no shard answered`; MCP: a retryable tool
  error), so the router retries it on another reader.
- An internal shard API on readers (`/internal/term-stats`, `/internal/shard-search`, `/internal/snippets`,
  `/internal/callees`, `/internal/graph`), guarded by `cluster.token` (`X-Pharos-Cluster-Token`,
  constant-time compare, 401 without it). Coordinator and shard must use the same `synonyms.txt`; a shard
  refuses otherwise (409). Right after a `synonyms.txt` change that refusal lasts up to one
  `pollIntervalSeconds` and those shards are left out with a warning; it heals by itself.
- Draining on SIGTERM: a serving reader announces `draining` (heartbeat, and `/health` 503
  `{"status":"draining"}`), keeps serving for a quiet period (at least 4 s and one heartbeat plus 2 s, and
  until nothing has been in flight for 1 s) and stops after at most `cluster.drainSeconds` (default 20).
  Give the process manager a stop timeout above `drainSeconds` + 15 s (Kubernetes
  `terminationGracePeriodSeconds`, e.g. 45).
- Reader heartbeats carry `state` (`warming|ready|degraded|draining`) and `degradedShards`; coordinators skip
  a reader only for its degraded shards. `GET /cluster/readers` lists the live readers; it needs the cluster
  token (401 without it), as it names every reader's `advertiseUrl`.
- New config: `cluster.fanoutTimeoutMillis` (5000), `cluster.connectTimeoutMillis` (1000),
  `cluster.queryTimeoutMillis` (20000: one budget for all rounds of a query), `cluster.maxCatalogAgeSeconds`
  (600: past it while catalog reads are failing, or before the catalog was first read, unscoped queries
  answer from the reader's own shards with a warning naming the shards not searched; the catalog is re-read
  every 5 s) and `cluster.drainSeconds` (20).

### Fixed
- `pharos mcp-server` on a reader no longer joins reader membership. It wrote the heartbeat of the `web`
  daemon's node id (same config), and on exit marked it draining and deleted it; it now serves the reader's
  own shards only, and exits without a drain wait.
- Shutdown order on SIGTERM/SIGINT/SIGHUP: the reader drain, then the running model calls, then exit; JVM
  shutdown hooks (ONNX Runtime's environment closer among them) start only after that, so a signal no longer
  frees the model under a running query (SIGSEGV in `OrtSession.run`). Applies to `web`, `mcp-server`,
  `index`, `embed` and `upgrade-indexes`.
- Lazy readers missed a corrupt checksum when a file's footer straddles two blocks (or fills the last block
  alone); present since 1.7.0. The real footer is now compared wherever it falls in the blocks.

### Changed
- A vector, hybrid or unified search over several projects no longer fails when one of them has no vectors
  of the search model: that project is left out of the vector part with the warning
  `project <name> has no vectors for model <id>; vector results exclude it`, on a single node and on every
  cluster reader alike. A search scoped to that one project still fails, now with HTTP 409 on the web API
  (was 500) and a tool error on MCP; `./pharos search` exits 6 for it (exit 4 stays "index format
  incompatible").
- `/health` on a reader whose lazy generation has lost objects on the bucket now answers HTTP 200
  `{"status":"degraded","degradedShards":[…],"reason":…}` (1.7.0: 503), so the node stays routable for its
  other shards. Probes that treated "degraded" as down need no change; probes that expected 503 for it do.
- The S3 client waits up to `readTimeoutSeconds` or 30 s, whichever is longer, between reads of a response
  (ranged block reads keep their `readTimeoutSeconds` bounds), so whole-object downloads and uploads on a slow
  bucket are not cut short.
- Config validation: `cluster.retireGraceSeconds` must be at least `2 x cluster.pollIntervalSeconds` and at
  least `cluster.fanoutTimeoutMillis` (in seconds); a reader config that violates either now fails at startup
  with a message naming both fields (see UPGRADING.md).

### Maintainers
- Phase 3 gate driver: `scripts/cluster-gate-phase3.sh` and `scripts/cluster_gate_phase3.py`. Ports are
  configurable (`GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`, `GATE_R1_PORT`..`GATE_R4_PORT`,
  `GATE_REF_PORT`, `GATE_ROUTER_PORT`; defaults 9000, 9001, 7301-7304, 7305, 7080), and so are the
  sources, embed-cache seed and model source for hosts without a `~/.pharos` registry (`GATE_LUCENE_SRC`,
  `GATE_EXTRA_PROJECT` / `GATE_EXTRA_SRC`, `GATE_SEED_FROM`, `GATE_MODELS_SRC`).

## [1.7.1] - 2026-11-23

**Index format:** 1 (unchanged)

The first published build of 1.7: the 1.7.0 tag never produced a release, because the release
workflow failed on a flaky test fixture. 1.7.1 has the same code as 1.7.0; read 1.7.0's notes below
for what changed since 1.6.0.

### Fixed
- `RemoteDirectoryTest`'s flat fixture is always rewritten without compound files. Lucene sometimes
  flushed the sample index as a single compound segment, which left the fixture without a `.tim`
  file and failed `openingAReaderWithEdgesFetchesNothing` (test only; no runtime change).

## [1.7.0] - 2026-11-23

**Index format:** 1 (unchanged)

Multi-node Phase 2: on-demand warming. Readers no longer need a full copy of every generation: by default
they keep only the small Lucene files locally and read the rest in blocks, with ranged GETs, through a
size-capped block cache. **Single-node installs are unaffected** apart from the body fix below: the next index run
re-processes every file once and re-embeds only the documents whose text changed.

### Fixed
- Two files yielding the same qualified name (for example a Python module and a YAML or Markdown file with the
  same path stem, such as `releaseWizard.py` and `releaseWizard.yaml`) got each other's synthesized document
  body: it was cached by qualified name, so both documents carried the text of whichever file was indexed
  first, and which one won depended on file order. The cache is now keyed by file and qualified name.
  `CHUNKING_VERSION` 4 → 5: the next `pharos index` re-processes every tracked file once; embedding texts that
  did not change come from the embedding cache, so only the affected documents are re-embedded.
- On a lazy reader, `pharos embeddings status` reported "no" (and a `pharos index --full` hint) for every
  project, because it probed the generation dir, where most Lucene files are not local. It now reports
  "unknown (lazy reader; …)", and `index-status` no longer logs a WARN for the same probe.

### Added (multi-node Phase 2: on-demand warming)
- Reader modes (`cluster.reader.mode`): `lazy` (default) downloads only the manifest, the graph
  and the small Lucene files, and reads the rest in blocks through a size-capped local block cache
  (`<cacheDir>/.blocks`) with prefetch of term dictionaries and vector graphs before a new
  generation is swapped in; `full` copies every file. Keys under `cluster.reader`: `mode`,
  `cacheMaxBytes` (8 GiB), `blockSizeBytes` (8 MiB, power of two 64 KiB–64 MiB), `eagerMaxBytes`
  (1 MiB), `prefetchExtensions`, `prefetchTimeoutSeconds` (30), `maxConcurrentFetches` (16),
  `privateCacheMaxBytes` (0 = derived). Size the cache dir with the README's disk formula.
- `/health` on a reader: HTTP 503 `{"status":"warming"}` until its first sync and its search
  models are ready (model warm-up runs in parallel with the first sync; a model that fails or
  hangs for 5 minutes does not hold readiness), and HTTP 503 `{"status":"degraded","reason":…}`
  while a served lazy generation has lost objects on the bucket (it lags beyond retention); 200
  otherwise. Searches answered while warming carry a notice in `X-Pharos-Warnings`.
- `readerCache` (`mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate, bytesFetched,
  evictions, leasesOpen, shared, source`) in `index-status --json` and `/health` on readers, and a
  `reader cache: …` line in `index-status`.
- A lazy reader held on an older generation by a refused newer one downloads the rest of it in
  the background, so the bucket's GC can't strand it; switching a reader from lazy to full deletes
  the unused block cache.

### Changed
- **Readers upgraded from 1.6.0 switch to `lazy` mode** unless `cluster.reader.mode` is set; set
  `"mode": "full"` to keep 1.6.0's full download per generation (recommended for readers that may lag beyond
  the writer's retention, see README "Retention and lagging lazy readers").

### Maintainers
- Gate scripts (`scripts/cluster-gate.sh`, `scripts/cluster-gate-phase2.sh`): ports are configurable with
  `GATE_MINIO_PORT`, `GATE_MINIO_CONSOLE_PORT`, `GATE_WRITER_PORT` / `GATE_READER_PORT` (Phase 1) and
  `GATE_LAZY_PORT` / `GATE_FULL_PORT` (Phase 2); defaults are unchanged (9000, 9001, 7300, 7301, 7302). The
  free-port probe and curl connects are bounded by `GATE_PORT_PROBE_TIMEOUT` (default 2 s) for hosts where
  an unused port hangs instead of refusing.

## [1.6.0] - 2026-11-23

**Index format:** 1 (unchanged)

The first multi-node release (Phases 0 and 1): one writer and any number of readers sharing an
S3-compatible object store. **Without a `cluster` section in `config.json` nothing changes:** pharos stays the
single-node tool (`role: all`, `backend: local`), and its search and graph output is byte-identical to 1.5.0
(`SingleNodeGoldenTest`).

### Added (multi-node: writer + readers over an object store)
- `cluster` config section: `role` (`all` | `writer` | `reader`), `backend` (`local` | `object-store`), `nodeId`,
  `pollIntervalSeconds`, `cacheDir`, retention (`retentionGenerations`, `retentionMinutes`) and `objectStore`
  (`endpoint`, `region`, `bucket`, `prefix`, `pathStyle` for MinIO, `accessKeyEnv` / `secretKeyEnv`). Validated at
  startup; a writer requires the `object-store` backend, and missing S3 credentials fail fast. See README,
  "Multi-node (experimental)".
- **Writer:** after each index run, the project is published as a new immutable generation (Lucene commit files,
  a compact call-graph file, unresolved refs, and a manifest with the index format stamp), and `LATEST` is
  flipped with a conditional put. Unchanged files are reused across generations. Old generations are
  garbage-collected by count and age; GC validates manifests and re-checks data before deleting.
- `pharos publish <project>` (Java CLI and `./pharos`, writer only) retries the upload without re-indexing. A
  failed publish at the end of `pharos index` keeps the local index and exits **3**; `publish` refuses (exit
  **4**) a broken `callgraph.current` pointer or an index format other than this build's.
- **Reader:** polls `LATEST`, downloads whole generations into `cacheDir` (checksummed end to end, hard-linking
  files it already has), and swaps them in; handles `LATEST` going back. Readers serve search and call-graph
  queries from an immutable compact graph file (parity-tested against ArcadeDB) and class bodies/snippets from
  the index, never from source files. Only one process per `cacheDir` syncs (`.sync.lock`).
- A reader only materialises generations whose index format it can serve (its own or N-1); otherwise it keeps
  the generation it has and logs a WARN naming the fix.
- `pharos web` on a reader binds its port at once; until the first sync finishes `/health` reports
  `{"status": "syncing", "ready": false}` and searches carry the notice in `X-Pharos-Warnings`.
- `index-status --json` reports the node's `role`.

### Changed
- Readers refuse `index`, `link`, `embed`, `mine`, `remove-index`, `backfill-embedding-cache` and
  `upgrade-indexes` (also `./setup.sh --upgrade-indexes`, which points readers at the writer).
  `upgrade-indexes` on a reader exits with its own code **5**.
- Internal: graph read paths go through a read-only `GraphView`; index publishing and local caching go through
  the new `com.pharos.cluster` SPI (`LocalCache`, `IndexPublisher`, `ReplicationBackend`). No user-visible change
  on single-node installs.

### Not yet on readers
- Module tools (MCP `list_modules`, `get_module_deps`, `find_module_path`, `get_module_boundary`, web UI module
  views) answer "not available on reader nodes yet"; the cross-project graph is not replicated, so call paths
  spanning projects are only found on the writer.

## [1.5.0] - 2026-11-23

**Index format:** 1 (unchanged)

### Fixed (cross-encoder reranking lowered quality)
- `hybrid-reranked` scored below plain `hybrid`. Three causes, all fixed: the reranker passage never contained the
  class or camelCase method name (it now starts with `ClassName.methodName`); `CrossEncoderReranker` and
  `CrossEncoderMerger` replaced the first-stage score with a saturated cross-encoder probability (they now fuse
  first-stage rank and cross-encoder rank with equal-weight RRF, k=60); and `hybrid-reranked` reranked a smaller
  pool than `hybrid` (it now gets the same adaptive oversample). Measured on the real daemons (99 queries,
  MRR@10 / P@1): jina hybrid 0.588 / 0.495, hybrid-reranked 0.579 -> **0.647** / 0.444 -> **0.566**;
  granite hybrid 0.664 / 0.566, hybrid-reranked 0.569 -> **0.661** / 0.414 -> **0.556**.
  `auto` no longer dispatches to the reranked child (`_auto-hybrid-rrf` instead): reranked did not reach hybrid on
  both models (granite -0.003, within noise).

### Added (embeddings opt-in)
- `pharos embeddings enable|disable|status` (Java CLI and `./pharos`). **No default changes for anyone:**
  semantic search stays off until you enable it, and `enable` never downloads or embeds by itself (it prints
  the cost, about 600 MB download and a CPU-heavy first embed). **granite-embedding-english-r2 is now the
  recommended model** (`enable` default; CLS pooling, 768 dims, pinned commit) based on the embedding A/B
  (`docs/superpowers/gates/2026-11-16-embedding-ab.md`): hybrid-rrf MRR@10 +0.056 [+0.002, +0.112], hybrid
  +0.068 [+0.008, +0.129], tie with the cross-encoder reranker (-0.009); peak RSS about 2.8 GB vs 2.3 GB and
  slower CPU embedding. Existing jina installs keep jina; switch with
  `pharos embeddings enable --model granite-r2 --force` plus a re-embed (embedding caches do not carry over).
  `enable` refuses to replace a different provider without `--force`; asking for the model that is already
  configured (same URL ignoring an `@<sha>` pin, including the old `embeddingModelUrl` form) reports "already
  configured" and exits 0. `setup.sh` prints a hint that semantic search is off only when no embedding provider is
  configured.
- **Opt-in flow: `pharos index --full <root path>` embeds a project.** `enable` and `embeddings status` print it
  (status prints the exact command per project without vectors: `pharos index --full --single --project <name>
  <root path>`; `index` takes the project's directory, not its name). `pharos embed` is not the opt-in path: it
  only adds a model next to vectors an index already has. On an index with no vector fields it now fails with
  "this index has no vectors yet; run `pharos index --full …` to embed it" (nonzero exit), and it records a model
  in the project's `embeddedModels` only when the index holds that model's vectors afterwards (before, a run that
  embedded 0 documents registered the model anyway).
- The jina preset id is **`jina-v2-code`** (`pharos embeddings enable --model jina-v2-code`), the id README
  documented for the jina provider block before 1.5.0; the `jina-code-v2` spelling used during 1.5.0 development
  is gone. Existing configs keep whatever id they have (the vector field is `vec.<id>`).

### Changed
- DJL 0.28 → 0.40 (its tokenizers parse list-form BPE merges, which granite-r2's `tokenizer.json` uses). jina
  vectors are byte-identical, so existing caches and indexes stay valid.
- Embedding failures are loud: a broken model logs a WARN and aborts the embedding pass (a batch whose every text
  fails is retried with a canary text first; only a failing canary aborts), while a single failing text is
  tolerated and logged. The embedding cache is saved on abort. Search warns when the query cannot be embedded and
  results fall back to keyword-only.
- `hybrid-reranked` scores are now on the RRF scale (first-stage rank fused with cross-encoder rank), not
  cross-encoder probabilities; tools that thresholded the old scores need new thresholds.
- `embeddings status` reads vectors from each project's Lucene index (live documents in the model's vector field)
  instead of trusting the registry; it no longer reports "yes (legacy)" for keyword-only projects.
- `index-status` sets `needsReembed` only when an embedding provider is configured; its hint and
  `upgrade-indexes`' hints print the registered root path (`pharos index --full --single --project <name> <root
  path>`) instead of passing the project name where `index` needs a path.
- `upgrade-indexes` lists projects with legacy-model vectors and the exact `pharos index …` command for each; it
  does not run them (exit codes unchanged).
- A `config.json` that fails validation no longer locks you out: `pharos embeddings disable|status` still run,
  and every other command prints the error plus how to fix the file.
- Legacy (`url:dims`) embedding-cache and file-state fingerprints are accepted only when the provider uses mean
  pooling and no prefixes, the only settings older pharos versions embedded with.
- The CLI, the daemon and `setup.sh` run the JVM with `-DOPT_OUT_TRACKING=true` (and `Main` sets it when unset):
  DJL's telemetry otherwise calls the EC2 instance-metadata endpoint.

### Added
- Embedding pooling (`pooling`: `mean` or `cls`) and `queryPrefix` / `documentPrefix` per provider.
- `hf://owner/name@<sha>` pins a HuggingFace model to a commit (7-40 hex chars).
- External ONNX weights (`model.onnx_data`) are downloaded when a repo has them. Model caches downloaded by
  earlier versions are probed once for a missing `model.onnx_data` and completed; offline, the probe is skipped
  and retried on the next start.

### Fixed
- Vector search returned nothing (hybrid silently fell back to keyword) for configs using the old
  single-model form (`embeddingModelUrl`), or any provider with model id `legacy`: indexing wrote
  `vec.legacy` while search reads `vectorEmbedding`. Writers and readers now share one mapping
  (`DocumentMapper.vectorFieldFor` / `chunkVectorFieldFor`); named models keep `vec.<id>`.
  The first `pharos index` after upgrading re-indexes affected projects (indexes built by
  1.4.0-1.4.1 with the legacy model) in full, reusing cached embeddings (`index-status` flags such projects as
  `needsReembed` and prints the command). `pharos embed` migrates only single-chunk `.java` documents; the next
  `pharos index` finishes the rest. `pharos upgrade-indexes` lists these projects but does not migrate them.
  Index format unchanged.

## [1.4.1] - 2026-11-16

**Index format:** 1 (unchanged)

### Fixed
- Project-name safety: `pharos index .` registered a project named `.`, whose index directory was
  the indexes root, so removing it could delete every project's index. Default names now come from
  the directory name, invalid names (empty, `.`, `..`, separators, anything outside the indexes
  directory) are refused, and deletes and writes never leave the indexes directory. Existing bad
  registrations are reported as `invalid` and ignored; `pharos remove .` unregisters them without
  deleting anything (see UPGRADING.md, "Invalid project names").
- `setup.sh` printed "pharos pharos X" on its final line.

### Added
- `pharos --version` prints the installed jar's version and the CLI location.

## [1.4.0] - 2026-11-16

**Index format:** 0 → 1 (format 0 still served; re-index recommended, see UPGRADING.md)

First tagged release. Existing indexes keep working after upgrading (the previous format is
still served); re-index when convenient to move them to format 1.

### Added
- Index format stamp (`index-format.json`) per project, with N-1 compatibility: format 0 indexes
  are still searchable (status `upgrade-available`), older or newer formats are refused with a
  clear message.
- `pharos index-status` and `pharos upgrade-indexes` to inspect and re-index projects whose format
  is not current.
- Searchable-while-reindexing: the call graph is rebuilt into a new generation directory and
  switched in atomically, so readers are never disturbed by a full re-index.
- `setup.sh --release latest|vX.Y.Z` installs a published release with checksum verification,
  warns about projects that would become unsearchable, and `--upgrade-indexes` re-indexes them.
- GitHub release workflow publishing the jar, its checksum, the skill bundle, `pharos` and `setup.sh`.
- `UPGRADING.md` and this changelog.

### Changed
- Tests are isolated from the real `~/.pharos` via `-Dpharos.home` (set by surefire).

### Fixed
- Java nested and anonymous members are no longer indexed twice; `pharos embed --model` works (#18).
- Embedding caches survive fingerprint changes and are kept per model (#19).
- The search model is loaded once instead of per request (#20).
- Model loading is lazy, with background warm-up for the web UI and MCP server (#21).
- File size limits: `maxDocumentFileSizeKb` (10240) and `maxSourceFileSizeKb` (102400) (#22).
- JS extractor no longer indexes control flow as methods; script and doc ids are unique (#23).
- Call-graph index repair; callers are kept across incremental patches (#24).
- `daemon.log` is rotated (#25).
- No ONNX SIGSEGV on shutdown (#26).
- Unresolved call references are kept on disk instead of in memory (#27).
- Lucene directories are memory-mapped (#28).
- `index --no-embed` never loads the embedding model (#29).
