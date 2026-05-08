# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes Java projects and gives you three ways to navigate them:

- **Keyword and semantic search** across methods, classes, and javadoc
- **Call graph exploration** — who calls what, down to method level — visualized in a browser UI
- **Claude skill** — teaches Claude Code to use `pharos` for code navigation instead of grep

## Requirements

- Java 21+
- Python 3.8+ (for the `pharos` CLI client)
- Maven (to build)

## Build & install

```bash
mvn clean package -DskipTests
cp pharos ~/.local/bin/pharos && chmod +x ~/.local/bin/pharos
```

The `pharos` script auto-discovers the JAR from `target/` (dev) or `~/.pharos/bin/pharos.jar` (installed).

To install the JAR to a stable location (survives `mvn clean`):
```bash
mkdir -p ~/.pharos/bin && cp target/pharos-*.jar ~/.pharos/bin/pharos.jar
```

## Usage

```bash
# Index a project (incremental by default)
pharos index /path/to/project

# Search
pharos search "parse method signature"
pharos search "LuceneIndexer" --type keyword

# Get full method body
pharos method "com.pharos.indexer.LuceneIndexer#index(ParsedMethod)"

# Call graph
pharos callers "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos callees "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos path  "com.pharos.Main#main(String[])" "com.pharos.parser.JavaCodeParser#parseProject(Path,String)"

# Web UI (graph browser, port 7171)
pharos web

# List indexed projects
pharos projects
```

Index multiple projects and link them for cross-project call resolution:

```bash
pharos index /path/to/project-a
pharos index /path/to/project-b
pharos link project-a project-b   # (via java -jar for now; web API coming)
```

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency on every command (port 7171, `PHAROS_PORT` to override):

```bash
pharos daemon status
pharos daemon stop
pharos daemon logs
```

## Claude Code integration

### Skill (recommended)

Install the bundled skill so Claude Code automatically uses `pharos` for code navigation instead of grep or file reads:

```bash
# User-level (all projects)
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md

# Or project-level only
mkdir -p /path/to/your-project/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md /path/to/your-project/.claude/skills/pharos/SKILL.md
```

Once installed Claude Code will call `pharos search`, `pharos callers`, etc. when navigating indexed projects.

### MCP server (alternative)

If you prefer tool-call integration over CLI, register the MCP server:

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
| `get_module_deps` | Direct/transitive Maven deps |
| `find_module_path` | Shortest Maven dependency path |
| `get_module_boundary` | Entry/exit points for a module |

## How it works

1. **Parse** — JavaParser resolves symbols across source roots and JARs, producing fully-qualified method and call reference data
2. **Graph** — call references become a directed graph (GraphML); Maven coordinates become a module dependency graph
3. **Embed** — optional semantic vectors via DJL + ONNX (falls back gracefully if unavailable)
4. **Index** — Lucene stores everything with BM25 for keyword search and KNN for vector search; graph centrality boosts results for heavily-called methods

Config and indexes are stored in `~/.pharos/`.
