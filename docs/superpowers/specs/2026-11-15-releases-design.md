# Pharos Releases and Safe Index Upgrades: Design

Status: draft for review · 2026-11-15

## Intent

Pharos is public: there are a few external users and one fork. Today they install by cloning the repository and running `setup.sh`, which builds whatever is on `main`. Some changes on `main` are not backward compatible with indexes that already exist. Examples:
- the document id changes in #18 and #23;
- the call-graph index repair in #24;
- per-model embedding caches in #19.

When such a change lands, nothing tells the user. Search quietly returns duplicates, misses results or errors. The goal is versioned GitHub releases, so that an incompatible change can never silently break someone's indexes. Users are told before they upgrade, searches on an outdated index stop with a clear fix, and one documented command upgrades their indexes.

## Goals

- Tagged, versioned GitHub releases with a built jar, built and tested the same way every time.
- Every index records which index format and pharos version built it.
- **When the running pharos meets an index with a different format, searches on that project stop with a clear message naming the fix. Other projects keep working.** (Decided: option A.)
- **`setup.sh` warns before switching versions.** It lists which installed projects the new version would invalidate and asks whether to continue. (Decided: the upgrade-time warning from option C.)
- **An upgrade option re-indexes exactly the outdated projects:** `./setup.sh --upgrade-indexes`, backed by `pharos upgrade-indexes`. (Decided.)
- **A documented upgrade path.** Release notes and `UPGRADING.md` say which releases need a re-index and give the exact steps. (Decided.)

## Non-goals

- Automatic migration of old index data, or automatic background rebuilds (option B was rejected).
- Keeping the old jar around to serve old indexes (the full option C was rejected; only its warning was kept).
- Tying the release's major version to the index format. **Decided: the index format has its own number; release notes say whether a re-index is needed.**
- A CI pipeline for every PR or push. That's a possible follow-up; this design only covers releases.
- Package managers (Homebrew, apt), or publishing to Maven Central.

## Anti-goals (failure even if it "works")

- An upgrade that leaves a user with silently wrong search results.
- A check so strict that users get blocked on harmless changes. The format number moves only for changes that really invalidate existing data.
- An upgrade command that starts hours of re-embedding without saying so first.
- Losing the user's embedding caches during an upgrade. They are what makes a re-index affordable.

## Approaches considered (how releases are produced)

1. **GitHub Actions on a version tag (chosen).** Pushing `vX.Y.Z` builds, runs the default test suite and publishes a GitHub release with the jar, a checksum and notes from `CHANGELOG.md`. Releases are reproducible and independent of anyone's machine.
2. **A local release script using `gh`.** Simpler, with no CI, but releases depend on the maintainer's laptop and nothing tests them independently.
3. **Release automation from commit messages** (for example release-please). Rejected: the repo's commit style ("Indexer: …") isn't the convention those tools parse.

The user picked 1.

## Design

### 1. Index format stamp

- `IndexVersions.INDEX_FORMAT` is a new `int` constant, separate from `CHUNKING_VERSION`.
  - Bump it for any change that makes existing per-project index data wrong or unreadable for the new code: document ids, Lucene field names or semantics, call-graph schema or index layout, embedding-cache layout, `unresolved-refs.json` shape.
  - Its Javadoc keeps a version history in the same style as `CHUNKING_VERSION`.
  - **Starting value: 1.** Everything before this change counts as format 0.
- Each project index dir gets `index-format.json`: `{"format": 1, "pharosVersion": "1.4.0", "writtenAt": "<ISO-8601>"}`.
  - A **full** index run writes it after its commit succeeds.
  - An incremental run leaves it alone, because an incremental run is only allowed on a matching format (see 3).
- A missing stamp means format 0. Indexes built before this release are therefore outdated by definition, which is correct: #18 and #23 already require a full re-index.
- The pharos version comes from the jar: Maven filters the `pom.xml` version into a resource, falling back to `"dev"`.

### 2. Read-time check (option A)

- A single helper `IndexFormat.check(projectName, projectIndexDir)` throws `IncompatibleIndexException` (unchecked) when the stamp's format ≠ `INDEX_FORMAT`.
  - Message: `Project 'lucene' was indexed by pharos 1.3.0 (index format 0); this pharos (1.4.0) needs index format 1. Run: pharos index --full lucene   (or upgrade all: ./setup.sh --upgrade-indexes)`.
  - A **newer** format than the code knows (a user downgrading) gets the same exception with wording that says the index is newer and to re-index or reinstall the newer version.
- Read paths call it before opening a project's Lucene reader or call graph: `LuceneIndexer.openReader`, plus the graph read sites (`SearchEngine`, MCP tools, web API, CLI graph commands).
  - Results are cached per project directory and invalidated when the stamp file's mtime changes, so a re-index clears the error without restarting the daemon.
- Behaviour per surface:
  - **Search scoped to an outdated project:** the error message only.
  - **Search across projects:** outdated projects are skipped. Results come from the healthy ones, plus a notice listing the skipped projects and the fix. In the web API this is a `warnings` field. In MCP it's a line before the results, and in the CLI it's a line on stderr.
  - **Graph tools on an outdated project:** the error message.
  - **`pharos projects`:** marks outdated projects (`needs upgrade`).

### 3. Write-time behaviour

- `pharos index <project>` (incremental by default) on an outdated index does not attempt an incremental update. It prints `index format changed (0 → 1): running a full re-index` and runs a full index. This is a builder default, recorded as the designer's call: the user explicitly asked to index, and an incremental run on a stale format is exactly what corrupts data.
- `--full` behaves as today and writes the stamp.
- Full runs reuse the persistent embedding caches as today (per-model files from #19), so a re-index only re-embeds text that changed.

### 4. `pharos index-status` and `pharos upgrade-indexes`

- `pharos index-status [--json]` lists every registered project with:
  - its stamped format and pharos version;
  - the required format;
  - its status: `ok`, `outdated`, `newer` or `missing` (index dir gone);
  - its root path;
  - whether the root path still exists.
- `--json` is machine-readable, and `setup.sh` uses it.
- `pharos upgrade-indexes [--dry-run] [--yes] [project...]` re-indexes outdated projects with a full index, one at a time, from each project's recorded root path.
  - **Before starting,** it prints the plan (the projects, their sizes from the registry, and whether embeddings will run). Without `--yes` it asks for confirmation; on a non-interactive terminal without `--yes` it exits with an error.
  - **Projects whose root path no longer exists** are skipped with a message (`re-index manually: pharos index --full <name> <path>`), and the exit code is non-zero.
  - **Embeddings and other flags** follow the user's config exactly as a normal `pharos index` would.
  - **On failure,** it continues with the next project and prints a summary at the end.
- `./pharos` (the Python CLI) gets both commands, since CLAUDE.md requires it to stay in sync.

### 5. `setup.sh`: install modes and the upgrade warning

- **Source mode:** `./setup.sh` stays as today (build from the checkout).
- **Release mode:** `./setup.sh --release latest|vX.Y.Z` downloads the release's `pharos-X.Y.Z.jar` and its `.sha256`, verifies the checksum, and also takes `pharos` (the CLI script) and `setup.sh` from the same release. This removes the need for Maven; Java 25 is still required, and the existing SDKMAN bootstrap handles it.
- **Upgrade warning:** before replacing `~/.pharos/bin/pharos.jar`, setup runs the **new** jar's `index-status --json` against the user's registry and lists the projects that would become outdated:
  ```
  This version needs index format 1. These projects must be re-indexed before you can search them:
    lucene   (548 MB, last indexed 2026-11-12)   pharos index --full lucene
    solr     (501 MB, last indexed 2026-11-10)
  Continue installing? [y/N]   (re-index afterwards with: ./setup.sh --upgrade-indexes)
  ```
  - `--yes` skips the prompt.
  - Declining leaves the old jar installed.
- **Upgrade option:** `./setup.sh --upgrade-indexes` runs the installed jar's `upgrade-indexes`, then restarts the daemon if it was running.

### 6. Releases (GitHub Actions)

- `.github/workflows/release.yml` triggers on tags `v*.*.*`. Steps:
  1. Set up Temurin Java 25, Python 3 and Node.
  2. Fail if the tag ≠ the `pom.xml` version.
  3. `mvn -B clean package`, which runs the default test suite; slow groups are already excluded by `excludedGroups`.
  4. Create a GitHub release named after the tag, with the notes taken from that version's section of `CHANGELOG.md`.
  5. Attach `pharos-X.Y.Z.jar`, `pharos-X.Y.Z.jar.sha256`, `pharos` and `setup.sh`.
- `CHANGELOG.md` (Keep a Changelog style) has an `Unreleased` section. Every release section starts with a line `**Index format:** N (unchanged)` or `**Index format:** N → M — re-index required, see UPGRADING.md`. The workflow fails if a release section lacks that line.
- `UPGRADING.md` has one section per format change: what changed, who is affected, and the steps (`./setup.sh --release vX.Y.Z` → review the warning → `./setup.sh --upgrade-indexes`). It also covers how to roll back (`./setup.sh --release <previous>`, and re-index if that version's format differs).
- `CLAUDE.md` and `README.md`:
  - a short "Releasing" section, covering bumping the pom version, moving `Unreleased` into a version section, tagging and pushing;
  - a rule for when to bump `INDEX_FORMAT` (next to the `CHUNKING_VERSION` rule).
- **First release (designer's call):** `v1.4.0`, containing all the fixes merged on 2026-11-15, with `INDEX_FORMAT` 1. Its notes say a re-index is required.

## Constraints

- Java 25. FAT jar via the shade plugin (keep the `ServicesResourceTransformer`).
- Never touch `~/.pharos` in tests: use temp dirs or `-Duser.home`.
- `./pharos` must stay in sync with new Java commands.
- `INDEX_FORMAT` and `CHUNKING_VERSION` are independent. A `CHUNKING_VERSION` bump keeps its current automatic selective re-embed and does not trigger the read-time check.
- The multi-node work (branch `feat/cluster-phase-0-1`) is changing `LuceneIndexer` and the graph read sites in parallel. Whichever lands second calls `IndexFormat.check` from the new `GraphViews` choke point, if the cluster branch has introduced it.

## Settled during review (designer's calls unless noted)

- **Base and scope.** PRs #18–#29 are all merged into `main`, and this branch starts from that. The `INDEX_FORMAT` 1 history entry names #18 (Java ids), #23 (script/doc ids), #24 (graph index repair) and #19 (per-model caches). The plan includes the pom bump to 1.4.0, `CHANGELOG.md` and `UPGRADING.md`. It **stops before pushing the tag**: publishing a release is the maintainer's action.
- **Valid but unstamped indexes are re-indexed anyway.** No "adopt this index" escape hatch: an unstamped index can't be told apart from a pre-fix one, and a wrong adoption is exactly the silent breakage this design exists to prevent. The cost is bounded, because the embedding caches make the forced re-index skip ONNX work for unchanged text.
- **Read-site guards live in this branch.** Guard `LuceneIndexer.openReader` plus every current graph read site, and implement the cross-project "skip + warning" in `SearchEngine`'s project resolution. When the cluster branch's `GraphViews` lands, the graph guards collapse into it during that merge.
- **`setup.sh` release mode.**
  - Downloads go to a temp dir.
  - The new jar's `index-status --json` runs from there with `-Duser.home` unchanged. That command only reads `registry.json` and the stamp files; it never starts the daemon or writes anything.
  - Installed files (jar, `pharos`, and a copy of the release's `setup.sh` at `~/.pharos/bin/setup.sh`) are replaced with `mv` (a new inode), so the running script is never overwritten in place.
  - The first-time path from an older install is documented in `UPGRADING.md` as `git pull && ./setup.sh --release v1.4.0`. The old source-mode script is not changed retroactively.
- **Sizes and embeddings in plans.** Size is computed by walking the project index dir at status time. "Embeddings will run" follows the user's config (any embedding provider configured, `--no-embed` not given).
- **Where `upgrade-indexes` runs.** In-process in its own JVM, exactly like `pharos index` today (the Python CLI spawns the jar). A running daemon picks up the change through the stamp-mtime invalidation; `setup.sh --upgrade-indexes` additionally restarts it. A concurrent index of the same project is still refused by Lucene's `write.lock`, as today: no new lock.
- **Workflow details.**
  - `pip install python-hcl2` in the workflow, so the Terraform tests run.
  - The jar stays `pharos-X.Y.Z.jar` (shade output).
  - The `.sha256` file is in `sha256sum` format (`<hex>  pharos-X.Y.Z.jar`), and `setup.sh` verifies it with `sha256sum -c` or `shasum -a 256 -c` (macOS).
  - The tag pattern is plain `v[0-9]+.[0-9]+.[0-9]+`. Pre-release tags are out of scope.

## Left to the builder

- The exact wording of messages, apart from the content listed above.
- How the version resource is generated (resource filtering or manifest `Implementation-Version`).
- The format of `index-status` table output.
- How setup's interactive prompt detects a non-TTY.
