# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes multi-language projects and gives you three ways to navigate them:

- **Keyword search** across methods, classes, and documentation with per-field BM25 tuning and query modifiers (`project:query`, `in:java`)
- **Call graph exploration** — who calls what, down to method level — powered by ArcadeDB and visualized in a browser UI
- **Module dependency graph** — Maven, Gradle, npm, Python, and CMake projects in one unified view
- **Project skeleton** — public API surface as a filesystem tree, scoped to any subdirectory
- **Claude skill** — teaches Claude Code to use `pharos` for code navigation instead of grep

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

| Language | Parser | Notes |
|---|---|---|
| Java | JavaParser (full symbol resolution) | Call graph edges, type-resolved FQNs |
| Python | AST via `python3` subprocess | Classes, functions, call refs |
| JavaScript / TypeScript | AST via `node` subprocess | Classes, functions, arrow fns, interfaces |
| Kotlin, Scala, Rust, Go, Swift, C#, and more | Regex-based | Classes and methods; no call graph |
| Markdown, YAML, shell scripts | Generic chunker | Full-text search |

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

# Search with query modifiers
pharos search "lucene:searching"           # boost lucene project results
pharos search "parse tokens in:java"       # boost Java file results
pharos search "parse tokens"              # regular cross-project search

# Get full method body
pharos method "com.pharos.indexer.LuceneIndexer#index(ParsedMethod)"

# Call graph
pharos callers "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos callees "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos path    "com.pharos.Main#main(String[])" "com.pharos.parser.JavaCodeParser#parseProject(Path,String)"

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

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency (port 7171, `PHAROS_PORT` to override):

```bash
pharos daemon status
pharos daemon stop
pharos daemon logs
```

## Claude Code integration

### Skill (recommended)

`setup.sh` installs the skill to `~/.claude/skills/pharos/` automatically. To install manually:

```bash
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md
```

Once installed Claude Code will call `pharos search`, `pharos callers`, `pharos skeleton`, etc. when navigating indexed projects.

### MCP server (alternative)

```bash
claude mcp add -s user pharos -- java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar mcp-server
```

| Tool | Description |
|---|---|
| `search_code` | BM25/vector/hybrid search |
| `get_method` | Full method body by FQN |
| `get_callers` / `get_callees` | Call graph edges |
| `find_call_path` | Shortest call chain between two methods |
| `list_projects` | Indexed projects with stats |
| `get_module_deps` | Direct/transitive deps |
| `find_module_path` | Shortest dependency path |
| `get_module_boundary` | Entry/exit points for a module |

## How it works

1. **Parse** — JavaParser (with full symbol resolution), Python AST, Node.js AST, and regex-based parsers for other languages extract methods, classes, and call references
2. **Graph** — call references become a directed graph (ArcadeDB embedded graph database); build files become a module dependency graph
3. **Embed** — optional semantic vectors via DJL + ONNX (falls back gracefully if unavailable; skip with `--no-embed`)
4. **Index** — Lucene stores everything with per-field BM25 for keyword search and KNN for vector search; graph in-degree boosts results for heavily-called methods

Config and indexes are stored in `~/.pharos/`.

## TODO
- query support to web UI with hit highlighting. When clicking result adjust the graph to reflect the hit
- for bm25, use lucene boosts to bring up better results to the top k
