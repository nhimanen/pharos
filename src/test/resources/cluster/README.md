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
- `layout.shards[]`: `shard`, `specVersion` (1), `keys` (`{project}`), and from the `LATEST`
  manifest `docCount`, `sizeBytes`, `generation`, `writer` (`writerNode`) plus `ageSeconds` of
  `LATEST`. Also:
  - `lease`: `{writer, ageSeconds, expired, released}` or null;
  - `readers`: the live readers serving the shard;
  - `backlog`: `{sourceRevision, publishedRevision, pending, oldestUnpublishedChangeAgeSeconds}`.
    It comes from the lease holder's heartbeat, with the age in bucket time from the first
    sighting, so it survives a takeover. While no writer reports a pending shard, the shard keeps
    `pending: true` with a null `sourceRevision`;
  - `state`: `current`, `lagging` (pending change), `lease-expired` or `unpublished`. It drives
    the chip colour.
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
- `proposals[]`: `{id (sha1 of kind+target), kind, target, reason, metrics, action, since}`.
  The kinds are `add-readers`, `remove-readers`, `add-writers`, `remove-writers`, `reshard` and
  `writer-disk`.

`/cluster/events` sends `{type: appeared|changed|cleared, proposal}` and `{type: layout, summary}`.
Events carry no addresses, with or without the token. The stream needs `Accept: text/event-stream`
(which `EventSource` sends), and a node allows at most 64 streams; above that it answers 503.
