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

**Java 25+ required.** The shade plugin produces a single executable JAR.

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

1. **Parse** (`parser/`) — `JavaCodeParser` uses JavaParser with `CombinedTypeSolver` (JDK + source roots + JARs) to produce `ParsedMethod` and `ParsedClass` objects with fully resolved symbol information.

2. **Graph** (`graph/`) — `CallGraphBuilder` turns parsed call references into a JGraphT `DefaultDirectedGraph` stored as `<index-dir>/<project>/graph.graphml`. `ModuleGraphBuilder` reads `pom.xml` for Maven coordinates and maintains `~/.pharos/module-graph.graphml`.

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

`PersistentEmbeddingCache` stores `SHA-256(embeddingText) → float[]` on disk at `<index-dir>/<project>/embed-cache.bin`. Both full and incremental index runs check this cache before calling the ONNX provider; unchanged embedding texts are served from cache at zero cost. The cache is invalidated automatically when the embedding model URL or dimensions change.

`IndexVersions.CHUNKING_VERSION` is an `int` constant in `indexer/IndexVersions.java`. **Bump it whenever chunking logic changes** (e.g. `DefaultChunker`, `chunkDocument`, `chunkMethodWithLines`). On the next incremental run, `FileStateTracker.hasOutdatedEmbeddings()` detects the mismatch, adds all tracked files to the dirty set, and re-embeds them. Single-chunk methods whose embedding text hasn't changed will be served from the persistent cache with no ONNX call.

**Workflow when changing chunking:**
1. Edit chunking logic in `DefaultChunker` (or related classes).
2. Increment `CHUNKING_VERSION` in `indexer/IndexVersions.java`.
3. Run `./pharos index <project>` — changed files re-embed via ONNX; unchanged single-chunk methods use cache.

### MCP Server

`McpServerCommand` starts a JSON-RPC 2.0 server over stdio. Tools are registered in `McpToolRegistry` and map directly to `SearchEngine` / `ProjectIndexManager` operations. Configure in Claude Desktop via:
```json
{ "mcpServers": { "pharos": { "command": "java", "args": ["-jar", "codesearch.jar", "mcp-server"] } } }
```

## Critical: FAT JAR Gotchas

- The shade plugin **must** include `ServicesResourceTransformer` for Lucene codec SPI discovery. Removing it breaks runtime with `ServiceConfigurationError`.
- Lucene `DirectoryReader` caching: readers are kept open across searches. When re-indexing, `LuceneIndexer` must reopen readers — verify this path when modifying indexing code.

## Key Method FQN Format

Methods are identified as: `com.example.MyClass#methodName(ParamType1,ParamType2)`

This format is used as the Lucene document ID, in call graph node keys, and in all CLI commands that take a fully-qualified method name.
