# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes Java projects and gives you three ways to navigate them:

- **Keyword and semantic search** across methods, classes, and javadoc
- **Call graph exploration** — who calls what, down to method level — visualized in a browser UI
- **MCP server** — exposes search and graph tools to AI assistants via JSON-RPC

The graph UI lets you drill down three levels: project dependencies → class dependencies → method call graph. External libraries are shown distinctly and can be hidden.

## Requirements

- Java 21+
- Maven (to build)

## Build

```bash
mvn clean package -DskipTests
```

Produces `target/codesearch-*.jar` (fat JAR, no other dependencies needed).

## Usage

```bash
# Index a project
java -jar target/codesearch-*.jar index /path/to/project

# Search
java -jar target/codesearch-*.jar search "parse method signature"

# Open the graph browser (default port 7070)
java -jar target/codesearch-*.jar web

# Start MCP server (stdio JSON-RPC)
java -jar target/codesearch-*.jar mcp-server
```

Index multiple projects and link them to resolve cross-project call edges:

```bash
java -jar target/codesearch-*.jar index /path/to/project-a
java -jar target/codesearch-*.jar index /path/to/project-b
java -jar target/codesearch-*.jar link project-a project-b
```

Config and indexes are stored in `~/.codesearch/`.

## MCP server setup

### Claude Code (user-level, recommended)

Build the fat JAR and copy it to a stable location so it won't be overwritten by `mvn clean`:

```bash
mvn clean package -DskipTests
mkdir -p ~/.pharos/bin
cp target/pharos-*.jar ~/.pharos/bin/pharos.jar
```

Register it with the Claude Code CLI at user scope (available in all projects):

```bash
claude mcp add -s user pharos -- java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar mcp-server
```

Verify it's connected:

```bash
claude mcp list
# pharos: java -jar ... mcp-server - ✓ Connected
```

Claude Code starts the process once per session and keeps it alive for all tool calls. The JVM startup cost (~1-2s) is paid once per session.

### Claude Code skill

A Claude Code skill is bundled at `.claude/skills/pharos/SKILL.md`. It teaches Claude when and how to use the `pharos` CLI for code navigation.

**To activate for all your projects (user-level):**
```bash
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md
```

**To activate for a single project only**, copy it into that project:
```bash
mkdir -p /path/to/your-project/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md /path/to/your-project/.claude/skills/pharos/SKILL.md
```

Once installed, Claude Code will automatically use `pharos` CLI commands for code search and navigation instead of grep or file reads, when the project is indexed.

### Available MCP tools

| Tool | Description |
|---|---|
| `search_code` | BM25/vector/hybrid search across indexed methods and classes |
| `get_method` | Fetch full method body by FQN |
| `get_callers` | Find all methods that call a given method |
| `get_callees` | Find all methods called by a given method |
| `find_call_path` | Shortest call chain between two methods |
| `list_projects` | List indexed projects with stats |
| `list_modules` | All modules in the Maven dependency graph |
| `get_module_deps` | Direct/transitive deps and dependents of a module |
| `find_module_path` | Shortest Maven dependency path between two modules |
| `get_module_boundary` | Entry points and external call exit points for a module |

## How it works

1. **Parse** — JavaParser resolves symbols across source roots and JARs, producing fully-qualified method and call reference data
2. **Graph** — call references become a directed graph (GraphML); Maven coordinates become a module dependency graph
3. **Embed** — optional semantic vectors via DJL + ONNX (falls back gracefully if unavailable)
4. **Index** — Lucene stores everything with BM25 for keyword search and KNN for vector search; graph centrality boosts results for heavily-called methods
