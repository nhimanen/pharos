# Getting Started

This guide walks you through indexing your first project.

## Prerequisites

You need Java 21 or later installed.

```bash
java -version
```

## Indexing Your Project

Run the index command pointing at your project root:

```bash
java -jar codesearch.jar index /path/to/project --project my-project
```

### Java Projects

For Java projects, the parser resolves symbols using JavaParser.
All methods and classes are extracted with their call references.

### Python Projects

For Python projects, the extractor uses the `ast` module via subprocess.
Functions and classes are extracted with docstrings.

## Running a Search

After indexing, run keyword or semantic search:

```bash
java -jar codesearch.jar search "authentication handler"
java -jar codesearch.jar search "user login" --type vector
```

### Search Types

- **keyword**: BM25 text matching (fast, good for exact terms)
- **vector**: Semantic embedding search (finds conceptually related code)
- **hybrid**: Combines both via Reciprocal Rank Fusion (default)

## Next Steps

See [Configuration Guide](configuration.md) for advanced options.
See [README](../README.md) to go back to the overview.
