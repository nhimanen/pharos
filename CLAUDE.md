# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
mvn clean package           # Build FAT JAR (target/pharos-*.jar)
mvn clean package -DskipTests  # Build without running tests
mvn test                    # Run all tests
mvn test -Dtest=ClassName   # Run a single test class
mvn clean compile           # Compile only
```

**Test isolation:** surefire sets `-Dpharos.home=target/test-pharos-home`, so tests never touch the real `~/.pharos`
(`IndexConfig.DEFAULT_BASE` honors `pharos.home`; unset means `~/.pharos`). Pass `-Dpharos.home=<dir>` to relocate state when running the CLI.

**Java 25+ required.** The shade plugin produces a single executable JAR.

**Maven layout.** The root `pom.xml` is the aggregator and parent (`pharos-parent`); run every command above
from the root. Modules:
- `app/pom.xml` — the pharos application (artifactId `pharos`). Its sources stay in the root `src/` and it
  builds into the root `target/`, so the jar is still `target/pharos-<version>.jar`; its tests run with the
  repository root as working directory.
- `pharos-api/` — what the operator shares with pharos: `ClusterConfig` (+ `PharosHome`, `DiscoverySpec`,
  `BackendKind`, `NodeRole`, `ShardId`) and the status records (`ClusterStatus`, `StatusState`,
  `StatusEvent`, `WriterHeartbeat`), same packages as before. Jackson only — `ApiModuleHasNoHeavyDepsTest`
  fails if Lucene, DJL/ONNX or Javalin reach its dependency tree. Code that needs them stays in `src/`
  (move a constant into the api class rather than referencing e.g. `LuceneIndexer` from it).
- `operator/` — the Kubernetes operator (plain JOSDK on the fabric8 client); depends on `pharos-api`, never
  on the fat jar. The `PharosCluster` CRD is generated from `com.pharos.operator.api` into `operator/target/crd/`;
  the committed copy is `deploy/crd/`, and `CrdGenerationTest` fails when they drift. After changing the api
  classes: `mvn -pl operator -am test -Dtest=CrdGenerationTest -Dpharos.regenerateCrd=true`.
  Spec defaults live only in `operator/.../Defaults.java` (validator, renderers and autoscaler share them);
  every derived object name comes from `Names` (project names only via `Names.k8sName`). The renderers
  (`render/`) build a `DesiredState` that JOSDK dependent resources apply with server-side apply.
  `ReconcilerMockServerTest` runs a real `Operator` on fabric8's mock server, which has no SSA, so it
  switches JOSDK to create/update; a second reconcile must leave every resourceVersion unchanged.
  Project Jobs (registration `pharos index --project <p> --source-git … --ref …` without a path, removal
  `pharos remove-index --from-catalog <p>`) are dynamic, so they are not dependents: `ProjectReconciler`
  plans them from the Jobs in the informer (pure, unit-tested) and the reconciler creates/deletes them.
  Writers mount each project's `git.credentialsSecret` at `/etc/pharos/git/<k8sName>`, named in
  `cluster.writer.gitCredentials`, which `SourceCheckout` applies to that project's git calls only.
  Replicas come from `scale/Autoscaler` (pure; fake clock in tests): pharos's proposals from
  `/cluster/status`, read by `status/ClusterStatusClient` straight from the pods (readers' headless
  Service, then writers'; never the router) on its own threads, behind the JOSDK event source
  `ClusterStatusEventSource`. Its source of truth is each StatefulSet's `pharos.dev/last-scale`
  annotation (`LastScale`, applied together with `replicas`, OP-R5), with `status.proposals` as the
  informational copy, so cooldown and episodes survive a restart or a failed status patch; writer scale-down passes `ScaleDownGate` (removed ordinals' heartbeats and leases gone);
  `ScheduleFloors` raises `min` (cron via `CronSchedule`, also the validator's parser). Tests use
  `ClusterStatusEventSource.NEVER_ANSWERS` or a fake `Watch`.
  Workload versions come from `upgrade/UpgradeCoordinator` (pure): a changed `spec.version` first runs
  Job `<name>-preflight-<version>` (`PreflightJobRenderer`: the new image, `pharos upgrade-indexes --check`,
  exit 3 = a shard refused → `UpgradeBlocked`, nothing applied), then rolls readers, writers, router, each
  only once the previous role is rolled out and every pod heartbeats in `/cluster/status` (started after
  the `pharos.dev/version-since` annotation, applied with the image). A downgrade (OP-R6) rolls writers
  first, then re-checks the bucket (`<name>-preflight-<version>-post`) before readers and router move.
  Project Jobs use the writers' version.
  Phase and conditions (StorageReady, ReadersReady, WritersReady, ProjectsPublished, UpgradeBlocked) come
  from `status/PhaseEvaluator` (pure), merged by the reconciler into its one status patch.

**Terraform (.tf) indexing** requires `python3` plus the third-party `python-hcl2` package
(`pip install python-hcl2`) — the first extractor script with a non-stdlib dependency
(Python/JS extractors otherwise only use stdlib). Without it, `TerraformCodeParser`
degrades gracefully to an empty result for `.tf` files, same as `JsCodeParser` without `node`.

**Go (.go) indexing needs no external runtime.** `GoCodeParser` is a native Java parser,
deliberately *not* a `ScriptBasedCodeParser`: a `go/ast` extractor would need the Go
toolchain, and script-based parsers degrade to an *empty* result when their runtime is
missing — Go files would be silently dropped on hosts and CI without `go` installed.

## Running the Tool

```bash
java -jar target/pharos-*.jar index <path>      # Index a Java project
java -jar target/pharos-*.jar search <query>    # Keyword search
java -jar target/pharos-*.jar mcp-server        # Start MCP server (stdio JSON-RPC)
```

**Python CLI client:** `./pharos` at the project root is the primary user-facing CLI. It starts a background JVM daemon on first use and proxies all commands via the web API (avoids JVM cold-start). Always check/update `./pharos` when adding new commands or API parameters — the Java CLI and the Python client must stay in sync.

```bash
./pharos search "query"           # Uses auto type-classification by default
./pharos search "query" --trace   # Shows pipeline trace (resolved type, stage timings)
./pharos index /path/to/project   # Index via daemon
```

Config is stored in `~/.pharos/config.json`; project registry in `~/.pharos/registry.json`.

## Architecture

### Indexing Pipeline

Source files flow through four sequential stages:

1. **Parse** (`parser/`) — every parser implements `CodeParser` and declares the extensions it
   handles. `ProjectIndexManager` does one `walkFileTree` and dispatches each file to the
   **first** parser claiming its extension, so the registration order in `Main.main` matters
   (`GenericFileParser` stays last). There are four tiers:

   | Tier | Parsers | Notes |
   |---|---|---|
   | AST, in-process | `JavaCodeParser` (JavaParser + `CombinedTypeSolver` over JDK + source roots + JARs), `GoCodeParser` | Fully resolved symbols; no external runtime |
   | AST, subprocess | `PythonCodeParser`, `JsCodeParser`, `TerraformCodeParser` (all extend `ScriptBasedCodeParser`) | Bundled extractor script + a runtime on PATH; **degrade to an empty result** when it's missing |
   | Regex | `RegexCodeParser` × the 11 `LanguageProfile.ALL` entries | No runtime; no call graph |
   | Chunker | `GenericFileParser` | Markdown/YAML/JSON/etc., not code |

   **Adding a language:** pick a tier, register it in the `parsers` list in `Main.main`, bump
   `IndexVersions.CHUNKING_VERSION`, and add a build-file reader (below) if the language has one.

   **Only `resolved()` call references become graph edges** (`CallGraphBuilder`) — a parser
   that emits solely unresolved references contributes no call graph at all. That is why the
   regex tier has none.

2. **Graph** (`graph/`) — `CallGraphBuilder` turns parsed call references into a JGraphT `DefaultDirectedGraph` stored as `<index-dir>/<project>/graph.graphml`. `ModuleGraphBuilder` maintains `~/.pharos/module-graph.graphml`; `ProjectIndexManager.updateModuleGraph` detects the build system (first match wins: Maven → Gradle → Python → Node.js → CMake → Go) and every reader returns the shared `MavenPomReader.PomInfo` shape, so a new language's reader (e.g. `GoModReader` for `go.mod`) just joins that chain.

3. **Embed** (`embedding/`) — Optional DJL + ONNX Runtime step that produces 384-dim float vectors. Falls back to `NoOpEmbeddingProvider` when unavailable.

4. **Index** (`indexer/`) — `DocumentMapper` converts parsed objects to Lucene documents with `TextField` (BM25) and `KnnFloatVectorField` (vector search). `LuceneIndexer` manages per-project `FSDirectory` instances with cached `DirectoryReader`s.

### Search Layer

`SearchEngine` is the facade. It selects a strategy (`KeywordSearchStrategy` / `VectorSearchStrategy` / `HybridSearchStrategy`) and applies two post-processing steps:
- **Graph boost**: score × (1 + 0.3 × log(inDegree) / log(maxInDegree))
- **Neighborhood expansion**: appends callees of top-3 results at 0.5× score

Field boosts for keyword search: `methodName` 3×, `javadoc` 2×, `signature` 2×, `className` 1.5×, `body`/`annotations` 1×.

### CLI Wiring

`Main.java` does manual dependency injection using picocli's `CommandLine.IFactory` (`DependencyFactory`). All subcommands receive their dependencies through this factory — there is no DI framework. Adding a new subcommand requires registering it in `CodeSearchCommand` and wiring it in `DependencyFactory`.

### Cross-Project Linking

When projects are indexed, `ProjectRegistry` stores unresolved call references. `link <proj1> <proj2>` or automatic Maven coordinate matching triggers `CrossProjectLinker`, which resolves those dangling refs across project boundaries and extends call graph edges.

### Incremental Indexing

`FileStateTracker` records file modification times in `<index-dir>/<project>/file-state.json`. Re-indexing without `--full` only re-parses changed files, deletes their old Lucene documents by `filePath`, and inserts fresh ones. The call graph is always fully rebuilt (it's fast compared to parsing).

#### Selective re-embedding and the embedding cache

`PersistentEmbeddingCache` stores `SHA-256(embeddingText) → float[]` on disk, one file pair per embedding model: `<index-dir>/<project>/embed-cache.<model>.{bin,meta}` (the migrated `legacy` model keeps the unsuffixed `embed-cache.{bin,meta}`). Both full and incremental index runs check this cache before calling the provider; unchanged embedding texts are served from cache at zero cost. A cache whose meta names a different model or dims is ignored — never deleted — and overwritten on the next save; fingerprints in the pre-multi-provider format (`url:dims`) are accepted for the same model (`IndexVersions.legacyFingerprints`).

`IndexVersions.CHUNKING_VERSION` is an `int` constant in `indexer/IndexVersions.java`. **Bump it whenever chunking logic changes** (e.g. `DefaultChunker`, `chunkDocument`, `chunkMethodWithLines`). On the next incremental run, `FileStateTracker.hasOutdatedEmbeddings()` detects the mismatch, adds all tracked files to the dirty set, and re-embeds them. Single-chunk methods whose embedding text hasn't changed will be served from the persistent cache with no ONNX call.

**Workflow when changing chunking:**
1. Edit chunking logic in `DefaultChunker` (or related classes).
2. Increment `CHUNKING_VERSION` in `indexer/IndexVersions.java`.
3. Run `./pharos index <project>` — changed files re-embed via ONNX; unchanged single-chunk methods use cache.

#### Index format (`INDEX_FORMAT`) and the N-1 rule

`IndexVersions.INDEX_FORMAT` is stamped per project in `<index-dir>/<project>/index-format.json`. **Bump it when a change makes existing indexes unreadable or wrong** (document ids, Lucene schema, call-graph layout, cache layout) — not for embedding-text changes, which `CHUNKING_VERSION` covers. A release with format N must keep **reading and serving format N-1** (`IndexFormat.MIN_READABLE_FORMAT`, status `upgrade-available`) so a live system keeps answering while projects are re-indexed; only ≤ N-2 and newer formats are refused.

**Workflow when bumping `INDEX_FORMAT`:**
1. Increment `INDEX_FORMAT` and add a history entry in its javadoc.
2. Keep a working read path for the previous format (N-1).
3. Add the fixture for the new format: `mvn test -Dtest=IndexCompatTest -Dpharos.regenerateCompat=true` writes `src/test/resources/compat/format-<new N>.zip`. **Keep `compat/format-<N-1>.zip`**; older fixtures may stay (`refusesFixturesBelowMinimum` checks they are refused).
4. `IndexCompatTest` must pass: it searches every fixture whose format is ≥ N-1.

### MCP Server

`McpServerCommand` starts a JSON-RPC 2.0 server over stdio. Tools are registered in `McpToolRegistry` and map directly to `SearchEngine` / `ProjectIndexManager` operations. Configure in Claude Desktop via:
```json
{ "mcpServers": { "pharos": { "command": "java", "args": ["-jar", "codesearch.jar", "mcp-server"] } } }
```

## Releasing (maintainers only)

1. Bump the version in every pom (`X.Y.Z`): `mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`
   (root `pom.xml` plus the `<parent>` version in `app/`, `pharos-api/`, `operator/`).
2. In `CHANGELOG.md`, move `Unreleased` into a new `## [X.Y.Z] - date` section. It must start with an `**Index format:**` line (`N (unchanged)` or `N → M (...)`; `scripts/release-notes.sh X.Y.Z` fails without it). If the format changed, add a section to `UPGRADING.md`.
3. Run `bash scripts/test-setup.sh` and `mvn -B clean package`.
4. Tag and push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`. The `release.yml` workflow checks the tag against the pom version, builds, and publishes the GitHub release (jar, `.sha256`, skill bundle, `pharos`, `setup.sh`).

## Critical: FAT JAR Gotchas

- The shade plugin **must** include `ServicesResourceTransformer` for Lucene codec SPI discovery. Removing it breaks runtime with `ServiceConfigurationError`.
- Lucene `DirectoryReader` caching: readers are kept open across searches. When re-indexing, `LuceneIndexer` must reopen readers — verify this path when modifying indexing code.

## Unsupported diagnostics switches

System properties under `pharos.diag.` are for maintainers' measurements only: undocumented for
users, unsupported, and free to change or vanish between releases. They are JVM-global.

- `-Dpharos.diag.luceneDirectory=niofs` — `LuceneDirectoryFactory.DEFAULT` opens `NIOFSDirectory`
  instead of Lucene's mmap default (the Phase 2 gate's bitwise comparison of a lazy reader against
  a full copy). A test that sets it must restore it and must not run in parallel with others.

## Key Method FQN Format

Methods are identified as: `com.example.MyClass#methodName(ParamType1,ParamType2)`

This format is used as the Lucene document ID, in call graph node keys, and in all CLI commands that take a fully-qualified method name.
