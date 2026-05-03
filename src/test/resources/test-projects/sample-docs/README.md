# Code Search

A fast code search tool with BM25 and vector search capabilities.

## Installation

Install the tool by building from source:

```bash
mvn clean package
java -jar target/codesearch.jar --help
```

See [Getting Started](docs/getting-started.md) for a full walkthrough.

## Quick Start

Index a project and search:

```bash
java -jar codesearch.jar index /path/to/my-project
java -jar codesearch.jar search "parse method"
```

## Features

- Keyword search with BM25 ranking
- Vector semantic search with embeddings
- Hybrid search combining both approaches
- Call graph analysis and traversal
- Multi-language support: Java, Python, and more

## Configuration

The tool reads configuration from `~/.codesearch/config.json`.
See [Configuration Guide](docs/configuration.md) for all options.

## Contributing

Pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
