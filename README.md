# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes multi-language projects and gives you multiple ways to navigate them:

- **Hybrid search** — BM25 keyword + semantic vector search with automatic query classification (`auto` default picks the right strategy per query)
- **Multi-vector indexing** — classes split into semantic chunks (header, method groups) and stored as `LateInteractionField` for ColBERT-style rescoring
- **Knowledge graph** — inheritance, field access, annotations, and type references stored as typed edges in ArcadeDB alongside the call graph
- **Call graph exploration** — who calls what, multi-hop traversal with bodies, transitive impact analysis
- **Smart snippets** — Lucene Highlighter + vector chunk positioning, both combined for precise source location
- **Zero-result advisor** — fuzzy matches, token breakdown, and filter notes when queries return nothing
- **Module dependency graph** — Maven, Gradle, npm, Python, and CMake projects in one unified view
- **Project skeleton** — public API surface as a filesystem tree, scoped to any subdirectory
- **Claude skill + MCP server** — teaches Claude Code to use `pharos` for code navigation instead of grep

## Requirements

- Java 21+
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

# Search — auto-classifies the query and picks the best strategy
pharos search "connection pool initialization"         # natural language → hybrid
pharos search "ConnectionPool"                         # identifier → keyword automatically
pharos search "validate token" --type unified          # single-pass BM25 + vector boost
pharos search "JWT expiry" --trace                     # show resolved type + timings
pharos search "authenticate" --snippet-lines 20        # control snippet size

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

### Search types

| Type | When to use |
|---|---|
| `auto` (default) | Let pharos decide: identifiers → keyword, natural language → hybrid |
| `keyword` | Exact name or FQN lookup; highest BM25 precision |
| `vector` | Semantic / concept search when you don't know the exact name |
| `hybrid` | Natural language query; fuses keyword and vector results |
| `unified` | Single BM25 pass with vector similarity as a multiplicative boost |

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency (port 7171, `PHAROS_PORT` to override):

```bash
pharos daemon status
pharos daemon stop
pharos daemon logs
```

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

## How it works

1. **Parse** — JavaParser (with full symbol resolution), Python AST, Node.js AST, and regex-based parsers extract methods, classes, call references, field declarations, inheritance, annotations, and type references

2. **Knowledge graph** — call references plus structural relationships (inheritance, field access, annotations, type refs) stored as typed ArcadeDB edges alongside the call graph; enables `find_usages`, subclass hierarchy, and field impact analysis

3. **Embed** — optional semantic vectors via DJL + ONNX; documents split into logical chunks (method = 1 chunk, class = header + method-group chunks) and stored as `LateInteractionField` for multi-vector rescoring; falls back gracefully if unavailable

4. **Index** — Lucene stores all fields with per-field BM25; `KnnFloatVectorField` (mean of chunk vectors) for HNSW retrieval; `LateInteractionField` for late-interaction rescoring

5. **Search** — auto-classifier picks keyword vs hybrid; query-type-adaptive candidate pool sizes; rVSM camelCase expansion; unified single-pass BM25+vector mode; Highlighter-based snippet positioning

Config and indexes are stored in `~/.pharos/`.
