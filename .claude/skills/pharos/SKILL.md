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

### Search & Retrieval

| Goal | Command |
|---|---|
| Find code by concept or description | `pharos search "<query>"` |
| Find code by exact method or class name | `pharos search "<name>" --type keyword` |
| Single-pass BM25 + vector combined ranking | `pharos search "<query>" --type unified` |
| Restrict to methods or classes | `pharos search "<query>" --doc-type method\|class` |
| Restrict to one project | `pharos search "<query>" --project <name>` |
| Control result count | `pharos search "<query>" --limit 20` |
| Show pipeline trace (resolved type + timings) | `pharos search "<query>" --trace` |
| Control snippet size per result | `pharos search "<query>" --snippet-lines 20` |

### Method & Class Lookup

| Goal | Command |
|---|---|
| Get full method implementation | `pharos method "<fqn>"` |
| Get full class body | `pharos class "<qualifiedName>"` |
| Get class with fields, constructors, public methods + callers | `pharos class "<qualifiedName>" --context` |

### Call Graph Navigation

| Goal | Command |
|---|---|
| Who calls a method | `pharos callers "<fqn>" [--limit N]` |
| What a method calls | `pharos callees "<fqn>" [--limit N]` |
| Multi-hop call chain with bodies | `pharos trace "<fqn>" [--depth 2] [--direction callees\|callers\|both] [--max-body 500]` |
| All transitive callers — impact analysis | `pharos impact "<fqn>" [--depth 5] [--max 2000]` |
| Shortest path between two methods | `pharos path "<from_fqn>" "<to_fqn>"` |

### Knowledge Graph

| Goal | Command |
|---|---|
| All usages (callers, subclasses, field access, annotations, type refs) | `pharos usages "<fqn>"` |
| Specific usage kind | `pharos usages "<fqn>" --kind callers\|subclasses\|field_readers\|field_writers\|annotated\|type_refs` |

### Module Graph

| Goal | Command |
|---|---|
| List indexed projects | `pharos projects [--limit N]` |
| List Maven modules | `pharos modules [--filter indexed\|external] [--limit N]` |
| Module dependency tree | `pharos deps <module> [--transitive] [--limit N]` |
| Dependency path between modules | `pharos mod-path <from> <to>` |
| Module public API surface | `pharos boundary <project> [--limit N]` |

### Indexing & Navigation

| Goal | Command |
|---|---|
| Project structure as file tree | `pharos skeleton <project> [--path <dir>] [--depth N] [--limit N]` |
| Index a project | `pharos index <path> [--force] [--full] [--no-embed] [--single] [--depth N] [--project-threads N]` |
| Daemon lifecycle | `pharos daemon status\|start\|stop\|restart\|logs` |

## FQN Format

`com.example.MyClass#methodName(ParamType1,ParamType2)`

Use FQNs from `pharos search` results directly in `method`, `class`, `callers`, `callees`, `trace`, `impact`, `usages`.

## Search Types

`pharos search` defaults to `--type auto` which classifies the query and picks the best strategy:
- **auto** (default) — classifies query: identifier → keyword, natural language → hybrid
- **keyword** — BM25 only; best for exact method/class names and FQN-like queries
- **vector** — semantic embedding search; best when you know the concept but not the name
- **hybrid** — keyword + vector fused; good general-purpose for natural language
- **unified** — single BM25 pass with vector similarity as a score boost; no separate merge step

## Limits

All listing commands accept `--limit N` (default 0 = no limit). `search` defaults to `--limit 10`.
Use `--limit 0` to remove the cap, or a positive number to cap output.

## Indexing Options

```
pharos index <path>                      # incremental re-index (only changed files)
pharos index <path> --full               # re-parse all files
pharos index <path> --force              # delete existing index then re-index
pharos index <path> --no-embed           # skip vector embedding (faster, no semantic search)
pharos index <path> --single             # treat as one project even if sub-projects exist
pharos index <path> --depth 2            # workspace discovery depth (default: 3)
pharos index <path> --project-threads 4  # index N projects in parallel
```

## Skeleton Options

```
pharos skeleton <project>                                              # full public API as filesystem tree
pharos skeleton <project> --path src/main/java/com/example            # scope to a subdirectory
pharos skeleton <project> --path src/main/java/com/example --depth 1  # direct children only
pharos skeleton <project> --limit 20                                   # cap at 20 classes shown
pharos skeleton <project> --method-limit 200                           # cap at 200 total methods fetched
```

## Workflows

**"Where is X implemented?"**
```
pharos search "X"
pharos method "com.example.MyClass#x()"
pharos callees "com.example.MyClass#x()"
```

**"Understand a class before modifying it"**
```
pharos class "com.example.MyService" --context
```
Returns: body, declared fields, constructors (shows injected deps), public methods + direct callers.

**"Who uses this API / impact of changing a method?"**
```
pharos callers "com.example.MyClass#x()"          # direct callers
pharos impact "com.example.MyClass#x()"           # all transitive callers, no bodies
pharos trace "com.example.MyClass#x()" --direction callers --depth 3  # callers with bodies
```

**"Trace an execution path / understand dependencies"**
```
pharos trace "com.example.PaymentService#process(Order)" --direction callees --depth 2
pharos path "com.example.Entry#main(String[])" "com.example.MyClass#x()"
```

**"Find all implementations of an interface / subclasses"**
```
pharos usages "com.example.Repository" --kind subclasses
```

**"Find everything annotated with @Transactional"**
```
pharos usages "Transactional" --kind annotated
```

**"Where is this field read or written?"**
```
pharos usages "com.example.PaymentService#connectionPool" --kind field_readers
pharos usages "com.example.PaymentService#connectionPool" --kind field_writers
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

**"Debug why search returned unexpected results"**
```
pharos search "query" --trace
```
Shows resolved type (what `auto` chose), stage timings, and pipeline used.

Run `pharos <command> --help` for full options on any command.
