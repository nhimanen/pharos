# Upgrading

## Upgrading to 1.4.0 (index format 1)

### What changed

1.4.0 stamps every project index with a format number (`index-format.json`). Indexes built by
earlier versions have no stamp and count as format 0. Document ids, the call-graph layout and the
embedding caches changed, so format 1 is what new indexing produces.

**Who is affected:** everyone with existing indexes.

**Compatibility (N-1):** a release serves its own format and the one before it. After upgrading,
your format 0 indexes keep working: search, callers and callees all still answer, and
`pharos projects` / `pharos index-status` show them as "upgrade available (still searchable)".
Nothing breaks, but you should re-index when convenient to get the fixes that depend on format 1.
Indexes more than one format behind (or newer than the installed release) are refused until they
are re-indexed.

### Steps

1. Pull and install the release:
   ```bash
   git pull && ./setup.sh --release v1.4.0
   ```
   (From a checkout older than 1.4.0 the `git pull` is what brings in the `--release` option.)
2. Review the warning `setup.sh` prints. Projects listed as "searchable now" keep working; projects
   listed as unsearchable need a re-index and the script asks before continuing.
3. Re-index:
   ```bash
   ./setup.sh --upgrade-indexes      # or: pharos upgrade-indexes
   ```
   You can check progress any time with `pharos index-status`.

### Time and cost

Re-indexing is a full run per project. Parsing dominates for large projects; embeddings are cheap
because the persistent embedding caches are reused, so unchanged text is not sent to the model
again. Projects stay searchable during the re-index (the old data is served until the new one is
published).

### Rolling back

```bash
./setup.sh --release <previous-version>
```
Format 0 indexes you did not re-index are untouched and work with the older release. Projects you
already re-indexed to format 1 need to be re-indexed again by the older release if it cannot read
format 1.
