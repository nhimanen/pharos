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

Add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "pharos": {
      "command": "java",
      "args": ["-jar", "/path/to/codesearch.jar", "mcp-server"]
    }
  }
}
```

## How it works

1. **Parse** — JavaParser resolves symbols across source roots and JARs, producing fully-qualified method and call reference data
2. **Graph** — call references become a directed graph (GraphML); Maven coordinates become a module dependency graph
3. **Embed** — optional semantic vectors via DJL + ONNX (falls back gracefully if unavailable)
4. **Index** — Lucene stores everything with BM25 for keyword search and KNN for vector search; graph centrality boosts results for heavily-called methods
