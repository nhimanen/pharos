# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes multi-language projects and gives you multiple ways to navigate them:

- **Hybrid search** — BM25 keyword + semantic vector search with automatic query classification (`auto` default picks the right pipeline per query)
- **Named pipelines** — keyword / vector / unified / hybrid (Borda or RRF) / cross-encoder reranked / doc-type diversified, all defined in `pipelines.yaml` and selectable via `--pipeline`
- **MMR class diversity** — default last-stage reranker that surfaces hits from more classes before repeating overloads of the same one
- **Cross-encoder reranking** — optional second-stage rerank for hybrid pipelines when higher precision matters more than latency
- **Multi-vector indexing** — classes split into semantic chunks (header, method groups) and stored as `LateInteractionField` for ColBERT-style rescoring
- **Multi-provider embeddings** — index against several embedding models side-by-side and A/B compare them with `pharos embed`; query-time provider can differ from index-time runtime
- **Knowledge graph** — inheritance, field access, annotations, and type references stored as typed edges in ArcadeDB alongside the call graph
- **Call graph exploration** — who calls what, multi-hop traversal with bodies, transitive impact analysis
- **Smart snippets** — Lucene Highlighter + vector chunk positioning, both combined for precise source location
- **Zero-result advisor** — fuzzy matches, token breakdown, and filter notes when queries return nothing
- **Module dependency graph** — Maven, Gradle, npm, Python, and CMake projects in one unified view
- **Project skeleton** — public API surface as a filesystem tree, scoped to any subdirectory
- **Claude skill + MCP server** — teaches Claude Code to use `pharos` for code navigation instead of grep

## Requirements

- Java 25+
- Python 3.8+ (for the `pharos` CLI client)
- Maven (to build)
- Node.js 14+ (optional — enables JavaScript/TypeScript indexing)

## Build & install

```bash
./setup.sh            # build JAR, install to ~/.pharos/bin/, restart daemon
./setup.sh --skip-build   # reinstall without rebuilding
./setup.sh --restart-only # just restart the running daemon
```

`setup.sh` also copies the Claude skill to `~/.claude/skills/pharos/` automatically.

## Supported languages

| Language | Parser | Call graph | Knowledge graph |
|---|---|---|---|
| Java | JavaParser (full symbol resolution) | ✓ resolved FQNs | ✓ fields, inheritance, annotations, type refs |
| Python | AST via `python3` subprocess | unresolved | ✓ inheritance, decorators, instance fields |
| JavaScript / TypeScript | AST via `node` subprocess | unresolved | ✓ extends, implements, decorators, class fields |
| Kotlin, Scala, Rust, Go, Swift, C#, and more | Regex-based | partial | — |
| Markdown, YAML, shell scripts | Generic chunker | — | — |

## Module graph support

The module dependency graph is populated automatically during indexing from whichever build file is present:

| Build system | File | Module key |
|---|---|---|
| Maven | `pom.xml` | `groupId:artifactId` |
| Gradle | `settings.gradle` + `build.gradle` | `group:name` |
| Python | `pyproject.toml` / `setup.py` | `python:package` |
| Node.js | `package.json` | `npm:name` or `scope:name` |
| CMake | `CMakeLists.txt` | `cmake:projectname` |

## Usage

```bash
# Index a project
pharos index /path/to/project                    # incremental
pharos index /path/to/project --force --no-embed # clean re-index, skip embedding
pharos index /path/to/workspace --project-threads 4  # parallel workspace indexing

# Search — auto-classifies the query and picks the best pipeline
pharos search "connection pool initialization"         # natural language → hybrid
pharos search "ConnectionPool"                         # identifier → keyword automatically
pharos search "validate token" --type unified          # single-pass BM25 + vector boost
pharos search "JWT expiry" --trace                     # show resolved type + timings
pharos search "authenticate" --snippet-lines 20        # control snippet size
pharos search "rate limit" --pipeline hybrid-reranked  # named pipeline (overrides --type)
pharos search "rate limit" --oversample 3              # fetch 3× candidates before rerank
pharos pipelines                                       # list available pipelines

# Get full method or class body
pharos method "com.pharos.indexer.LuceneIndexer#index(ParsedMethod)"
pharos class  "com.pharos.search.SearchEngine"
pharos class  "com.pharos.search.SearchEngine" --context   # + fields, constructors, callers

# Call graph
pharos callers "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos callees "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos trace   "com.pharos.search.SearchEngine#search(SearchRequest,boolean)" --direction callees --depth 2
pharos impact  "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"   # all transitive callers
pharos path    "com.pharos.Main#main(String[])" "com.pharos.parser.JavaCodeParser#parseProject(Path,String)"

# Knowledge graph
pharos usages "com.pharos.search.SearchEngine"                 # all usage kinds
pharos usages "com.pharos.search.SearchEngine" --kind subclasses
pharos usages "com.pharos.indexer.LuceneIndexer#indexer" --kind field_readers
pharos usages "Cacheable" --kind annotated

# Project structure as a filesystem tree
pharos skeleton myproject
pharos skeleton myproject --path src/main/java/com/example/api --depth 1

# Module dependencies
pharos modules
pharos deps com.pharos:pharos --transitive
pharos mod-path com.example:module-a com.example:module-b
pharos boundary myproject

# Web UI (graph browser, port 7171)
pharos web

# List indexed projects
pharos projects
```

### Pipelines

Pipelines are defined in `src/main/resources/pipelines.yaml` and selected with `--pipeline ID` (overrides `--type`). The five legacy `--type` values still work and map onto the same pipelines.

| Pipeline | When to use |
|---|---|
| `auto` (default) | FST-backed intent classifier dispatches to a child pipeline based on query shape |
| `keyword` | Exact name or FQN lookup; highest BM25 precision |
| `vector` | Semantic / concept search when you don't know the exact name |
| `unified` | Single BM25 pass with vector similarity as a multiplicative boost |
| `hybrid` | Borda-count fusion of keyword + vector with agreement bonus |
| `hybrid-rrf` | Reciprocal Rank Fusion (k=60) — the auto default for hybrid intents |
| `hybrid-reranked` | `hybrid` + cross-encoder rerank for higher precision (requires cross-encoder) |
| `hybrid-ce-merge` | Cross-encoder scores all candidates and acts as the merge step |
| `hybrid-diverse` | `hybrid` + doc-type diversity rerank (balances method/class/chunk hits) |
| `hybrid-reranked-diverse` | Cross-encoder rerank followed by doc-type diversity |

MMR class diversity runs as the default last-stage reranker on every pipeline except `keyword`.

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency:

```bash
pharos daemon status
pharos daemon stop
pharos daemon logs
```

Environment overrides honored on every JVM launch (daemon and `index`):

| Var | Purpose |
|---|---|
| `PHAROS_PORT` | Daemon HTTP port (default 7171) |
| `PHAROS_HEAP` | Passed as `-Xmx<value>` (e.g. `PHAROS_HEAP=16g pharos index ...`) |

## Claude Code integration

### Skill

`setup.sh` installs the skill to `~/.claude/skills/pharos/` automatically. To install manually:

```bash
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md
```

Once installed, Claude Code calls `pharos search`, `pharos trace`, `pharos usages`, `pharos impact`, etc. when navigating indexed projects instead of falling back to grep.

### MCP server (alternative)

```bash
claude mcp add -s user pharos -- java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar mcp-server
```

All MCP tools return structured JSON. Key tools:

| Tool | Description |
|---|---|
| `search_code` | BM25/vector/hybrid/unified search with auto-classification, snippets, and zero-result hints |
| `get_method` | Full method body by FQN |
| `get_methods` | Batch method/class lookup in one Lucene pass |
| `get_class` | Class body; add `context: true` for fields + constructors + public methods + callers |
| `get_callers` / `get_callees` | Direct call graph edges |
| `trace_call_chain` | Multi-hop BFS with bodies, direction, body truncation |
| `find_transitive_callers` | All unique transitive callers (impact analysis), flat deduplicated set |
| `find_usages` | Knowledge-graph usages: callers, subclasses, field readers/writers, annotations, type refs |
| `find_call_path` | Shortest call chain between two methods |
| `list_projects` | Indexed projects with stats |
| `get_module_deps` | Direct/transitive module deps |
| `find_module_path` | Shortest dependency path between modules |
| `get_module_boundary` | Entry/exit points for a module |

## Embedding configuration

Vector search is **disabled by default**. To enable it, add at least one provider to `embeddingProviders[]` in `~/.pharos/config.json`. The list is the new shape — legacy single-key configs (`embeddingModelUrl` at the top level) are migrated on load and not written back.

### Recommended: Jina v2 Code via DJL (local, no API key)

```json
{
  "embeddingProviders": [
    {
      "modelId": "jina-v2-code",
      "type": "djl",
      "url": "hf://jinaai/jina-embeddings-v2-base-code",
      "dimensions": 768,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "jina-v2-code"
}
```

On first index run, DJL downloads the ONNX model from HuggingFace Hub and caches it in `~/.djl.ai/pharos/`. Subsequent runs load from cache. This is the recommended setup for code search — 768-dim, 8 192-token context window.

### Alternative: MiniLM via DJL model zoo

```json
{
  "embeddingProviders": [
    {
      "modelId": "minilm",
      "type": "djl",
      "url": "ai.djl.huggingface.onnxruntime:sentence-transformers/all-MiniLM-L6-v2",
      "dimensions": 384,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "minilm"
}
```

Lighter model (384-dim), good for general text, lower RAM use.

### Alternative: OpenAI-compatible HTTP server

If you run a local embedding server (e.g. llama.cpp, Ollama, text-embeddings-inference), set `type: openai`:

```json
{
  "embeddingProviders": [
    {
      "modelId": "qwen3-emb",
      "type": "openai",
      "url": "http://localhost:8083/v1",
      "dimensions": 768,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "qwen3-emb"
}
```

### Multiple providers side-by-side

`embeddingProviders` is a list. Pharos can index against several models in parallel and store one vector field per model (`vec.<modelId>`) on each Lucene document. `searchEmbeddingModel` selects which one is queried; the others stay on disk for cheap A/B comparison via `pharos embed --model=<other>`.

### Split index/search runtimes

For setups where the index-time runtime differs from the query-time runtime (e.g. embed at index time via a fast remote `openai` server, embed queries offline via local DJL), add `searchEmbeddingProvider`:

```json
{
  "embeddingProviders": [
    { "modelId": "qwen3-emb", "type": "openai", "url": "http://prod-emb:8083/v1", ... }
  ],
  "searchEmbeddingProvider": {
    "modelId": "qwen3-emb",
    "type": "djl",
    "url": "hf://Qwen/Qwen3-Embedding-0.6B",
    "dimensions": 1024,
    "maxTokens": 512
  }
}
```

The `modelId` must match one of the `embeddingProviders` entries (that's how pharos routes the query to the right `vec.<modelId>` field). You are on the hook for ensuring both runtimes produce vectors in the same space — typically by serving the same underlying weights under both runtimes.

### Embedding cache & re-embedding

Embedding vectors are cached on disk at `<index-dir>/<project>/embed-cache.bin`, keyed by `SHA-256(embeddingText)`. Three commands manage it:

```bash
pharos backfill-embedding-cache myproject              # seed cache from existing HNSW vectors (no ONNX)
pharos backfill-embedding-cache --all                  # all projects

pharos embed --model=qwen3-emb myproject               # add a model's vectors to an existing index
pharos embed --model=qwen3-emb --all                   # all projects
pharos embed --model=qwen3-emb myproject --force       # overwrite existing vectors

pharos invalidate-embeddings myproject                 # docs only (default scope)
pharos invalidate-embeddings myproject --scope java    # Java files only
pharos invalidate-embeddings myproject --scope all     # everything
pharos invalidate-embeddings --all                     # docs in every project
```

Use `embed` when adding a new model without re-parsing. Use `invalidate-embeddings` after changing chunking logic to force selective re-embedding on the next incremental index run.

### Skipping embeddings

```bash
pharos index /path/to/project --no-embed     # skip embedding this run
```

Keyword search (`--type keyword`) and call graph features work without embeddings. Only `vector`, `hybrid`, and `unified` search types require them.

## How it works

1. **Parse** — JavaParser (with full symbol resolution), Python AST, Node.js AST, and regex-based parsers extract methods, classes, call references, field declarations, inheritance, annotations, and type references

2. **Knowledge graph** — call references plus structural relationships (inheritance, field access, annotations, type refs) stored as typed ArcadeDB edges alongside the call graph; enables `find_usages`, subclass hierarchy, and field impact analysis

3. **Embed** — optional semantic vectors via DJL + ONNX; documents split into logical chunks (method = 1 chunk, class = header + method-group chunks) and stored as `LateInteractionField` for multi-vector rescoring; falls back gracefully if unavailable

4. **Index** — Lucene stores all fields with per-field BM25; `KnnFloatVectorField` (mean of chunk vectors) for HNSW retrieval; `LateInteractionField` for late-interaction rescoring

5. **Search** — pipelines defined in `pipelines.yaml` compose retrievers (keyword / vector / unified), a merger (Borda / RRF / cross-encoder-merge), and rerankers (cross-encoder, doc-type diversity, MMR class diversity). The FST-backed `auto` router classifies query intent and dispatches to a child pipeline; `--pipeline` overrides it explicitly

Config and indexes are stored in `~/.pharos/`.
