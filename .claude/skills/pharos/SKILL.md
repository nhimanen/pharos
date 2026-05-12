---
name: pharos
description: Search indexed codebases using the pharos CLI — BM25/vector/hybrid search and call graph traversal
trigger: Use when asked to find, explore, or navigate code in an indexed project
---

You have access to the `pharos` command line tool for code search and navigation.
Use it instead of grep or file reads when the current project is indexed.

pharos runs a persistent background daemon — the first call may take a few seconds to start it; all subsequent calls are fast (~200ms).

Start with `pharos projects` to confirm the project is indexed before using other commands.

## Command Reference

| Goal | Command |
|---|---|
| Find code by concept or description | `pharos search "<query>"` |
| Find code by exact method or class name | `pharos search "<name>" --type keyword` |
| Restrict search to methods or classes | `pharos search "<query>" --doc-type method\|class` |
| Restrict search to one project | `pharos search "<query>" --project <name>` |
| Control result count | `pharos search "<query>" --limit 20` |
| Get full method implementation | `pharos method "<fqn>"` |
| What does a method call / depend on | `pharos callees "<fqn>" [--limit N]` |
| Who calls a method (change impact) | `pharos callers "<fqn>" [--limit N]` |
| Trace execution between two methods | `pharos path "<from_fqn>" "<to_fqn>"` |
| List indexed projects | `pharos projects [--limit N]` |
| List Maven modules | `pharos modules [--filter indexed\|external] [--limit N]` |
| Module dependency tree | `pharos deps <module> [--transitive] [--limit N]` |
| Dependency path between modules | `pharos mod-path <from> <to>` |
| Module public API surface | `pharos boundary <project> [--limit N]` |
| Project structure as file tree | `pharos skeleton <project> [--path <dir>] [--depth N] [--limit N]` |
| Index a project | `pharos index <path> [--force] [--full] [--no-embed] [--single] [--depth N] [--project-threads N]` |
| Daemon lifecycle | `pharos daemon status\|start\|stop\|restart\|logs` |

## FQN Format

`com.example.MyClass#methodName(ParamType1,ParamType2)`

Use FQNs from `pharos search` results directly in `method`, `callers`, `callees`.

## Limits

All listing commands accept `--limit N` (default 0 = no limit). `search` defaults to `--limit 10`.
Use `--limit 0` to remove the cap, or a positive number to cap output.

## Indexing Options

```
pharos index <path>                  # incremental re-index (only changed files)
pharos index <path> --full           # re-parse all files
pharos index <path> --force          # delete existing index then re-index
pharos index <path> --no-embed       # skip vector embedding (faster, no semantic search)
pharos index <path> --single         # treat as one project even if sub-projects exist
pharos index <path> --depth 2        # workspace discovery depth (default: 3)
pharos index <path> --project-threads 4  # index N projects in parallel
```

## Skeleton Options

```
pharos skeleton <project>                        # full public API as filesystem tree
pharos skeleton <project> --path src/main/java/com/example   # scope to a subdirectory
pharos skeleton <project> --path src/main/java/com/example --depth 1  # direct children only
pharos skeleton <project> --limit 20             # cap at 20 classes shown
pharos skeleton <project> --method-limit 200     # cap at 200 total methods fetched
```

## Workflows

**"Where is X implemented?"**
```
pharos search "X"
pharos method "com.example.MyClass#x()"
pharos callees "com.example.MyClass#x()"
```

**"Who uses this API / impact of changing a method?"**
```
pharos callers "com.example.MyClass#x()"
pharos path "com.example.Entry#main(String[])" "com.example.MyClass#x()"
```

**"Find an exact class or method by name"**
```
pharos search "ClassName" --type keyword --doc-type class
pharos search "methodName" --type keyword --doc-type method
```

**"How are two modules connected?"**
```
pharos mod-path com.example:module-a com.example:module-b
pharos boundary my-project
```

**"What is the public API of a package?"**
```
pharos skeleton my-project --path src/main/java/com/example/api --depth 1
```

**"Re-index after major changes (clean slate)"**
```
pharos index /path/to/project --force --no-embed
```

**"Show all callers but cap output"**
```
pharos callers "com.example.MyClass#x()" --limit 20
```

Run `pharos <command> --help` for full options on any command.
