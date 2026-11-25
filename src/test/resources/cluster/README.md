# `status-golden.json` and `status-golden-token.json`

Two `GET /cluster/status` answers for the same three-shard cluster, both written by `GoldenStatusTest`:

- `status-golden.json` is the **public view**, served without the cluster token. It is the one the
  Cluster page uses, and it has no `advertiseUrl` fields.
- `status-golden-token.json` is the **token view**, served to a request that carries a valid
  `X-Pharos-Cluster-Token`. It is identical except that each reader also has its `advertiseUrl`.

Regenerate both with `mvn test -Dtest=GoldenStatusTest -Dpharos.regenerateStatusGolden=true`.
The Cluster page (Task 6) reads its fields from the public fixture. Every list is sorted and map keys
are in order. Ages are whole seconds of bucket time, measured to `asOf`.

- `mode`: `cluster`, or `single-node` on `role: all` (one node, both writer and reader, holding every shard).
- `asOf`: the snapshot's bucket time (the newest LastModified it read).
- `layout.summary`: `{shards, writers, readers}`, the live counts. A change emits a `layout` event.
- `layout.shards[]`: `shard`, `specVersion` (the spec version of the project version the shard belongs
  to; 1 for the implicit `[project]`), `keys` (`{project, …}`: the project and the shard id's key values),
  and from the `LATEST`
  manifest `docCount`, `sizeBytes`, `generation`, `writer` (`writerNode`) plus `ageSeconds` of
  `LATEST`. Also:
  - `lease`: `{writer, ageSeconds, expired, released}` or null;
  - `readers`: the live readers serving the shard;
  - `backlog`: `{sourceRevision, publishedRevision, pending, oldestUnpublishedChangeAgeSeconds}`.
    It comes from the lease holder's heartbeat, with the age in bucket time from the first
    sighting, so it survives a takeover. While no writer reports a pending shard, the shard keeps
    `pending: true` with a null `sourceRevision`;
  - `state`: `current`, `lagging` (pending change), `lease-expired`, `unpublished`, or `staged` (a
    shard only a reshard's staged version has). It drives the chip colour;
  - `project`: the project the shard serves (null for an orphan);
  - `role`: `active`, `staged` (only in a reshard's staged version), `previous` (only in the version a
    flip retired, kept for the retention window), `abandoned` (waiting for GC) or `orphan`. Health
    findings about routing (`shard-unassigned`, `shard-unserved`) skip the non-active roles;
  - `staged`: the staged version names the shard; `stagedGeneration`: the generation it pins for the
    shard once published (a shared shard the reshard rewrote has one other than `generation`).
- `layout.writers[]`: the writer heartbeat fields `node, workQueue, leases, pinned, indexing
  {shard, phase parse|embed|commit|publish, progress 0..1, startedAt} | null, startedAt, stopping,
  disk {freeBytes, totalBytes, low} | null`, plus `heartbeatAgeSeconds`.
- `layout.readers[]`: the reader heartbeat fields `node, advertiseUrl, shards, state,
  degradedShards, readerCache {…}, inFlight, qps, startedAt, warmP95Millis, catalogAgeSeconds`
  (the last two may be null), plus `heartbeatAgeSeconds`. `advertiseUrl` is in the token view only.
- `backlogSummary`: `{shardsPending, oldestPendingAgeSeconds}`.
- `health[]`: `{kind, target, message}`. The kinds are `lease-expired`, `no-readers`,
  `reader-degraded`, `reader-draining`, `reader-warming`, `shard-lagging`, `shard-unassigned`,
  `shard-unserved`, `writer-low-disk` and `writer-stopping`.
- `proposals[]`: `{id (sha1 of kind+target), kind, target, reason, metrics, action, delta, since}`.
  The kinds are `add-readers`, `remove-readers`, `add-writers`, `remove-writers`, `reshard` and
  `writer-disk`. `delta` is the signed node count of the four scaling kinds (`+N` start, `-N`
  stop, the N the `action` names); it is absent on `reshard` and `writer-disk`. A `reshard` proposal targets a project and its action names the exact command, e.g.
  `pharos reshard alpha --spec "[project, ring(path, shards=2)]"` (ruling P6-R14: an unringed project gets
  a ring of `S = ceil(largestShardBytes / trigger)` shards, at least 2; a ringed one `max(N + 1, S)`;
  ruling P6-R15).
- `projects[]` (Phase 6): `{project, specVersion (null: [project]), spec, shards, docCount, sizeBytes,
  staged, previous, reshard, published, generation, pending}`. A split project is one entry over its
  shards. `published` (every active shard has a generation), `generation` (their lowest), `docCount`
  (their sum) and `pending` (one of them has an unpublished change) count the active shards only, never a
  staged or retired one (ruling P6-R15): per-project consumers read these, not `layout.shards[]`.
  - `staged`: `{specVersion, spec, shards, state building|pinned|abandoned}` or null;
  - `previous`: `{specVersion, spec, shards, retiredAt}` or null (the version a flip retired);
  - `reshard`: null, or `{specVersion, spec, from, phase requested|building|pinned|flipped|abandoned,
    requestedAt, shardsDone, shardsTotal, shardsRewritten, shardsKept, filesDone, filesTotal, holder (the
    reshard lease holder), lease, jobPhase, jobWriter, jobAgeSeconds, waitingFor, error}`. Unknown counts
    are null.
- `specs[]`: `{version, project, spec, state active|staged|requested|retired|abandoned|superseded}`.

The golden cluster has no reshard (its `specs` is empty); `ClusterPageFixtures.resharding()` is the
fixture with one, building and pinned.

`/cluster/events` sends `{type: appeared|changed|cleared, proposal}` (a change of reason or action, and so of
delta, is `changed`), `{type: layout, summary}` and
`{type: reshard, reshard: {project, specVersion, phase, shardsDone, shardsTotal, filesDone, filesTotal}}`
at a reshard's phase transitions (`ended` once it is no longer shown) and at each quarter of its shards
or files.
Events carry no addresses, with or without the token. The stream needs `Accept: text/event-stream`
(which `EventSource` sends), and a node allows at most 64 streams; above that it answers 503.
