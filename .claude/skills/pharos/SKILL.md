---
name: pharos
description: Search indexed codebases using the pharos CLI — BM25/vector/hybrid search and call graph traversal
trigger: Use when asked to find, explore, or navigate code in an indexed project
---

You have access to the `pharos` command line tool for code search and navigation.
Use it instead of grep or file reads when the current project is indexed.

Start with `pharos projects` to confirm the project is indexed before using other commands.

## Command Reference

| Goal | Command |
|---|---|
| Find code by concept or description | `pharos search "<query>"` |
| Find code by exact method or class name | `pharos search "<name>" --type keyword` |
| Get full method implementation | `pharos method "<fqn>"` |
| What does a method call / depend on | `pharos callees "<fqn>"` |
| Who calls a method (change impact) | `pharos callers "<fqn>"` |
| Trace execution between two methods | `pharos path "<from_fqn>" "<to_fqn>"` |
| List indexed projects | `pharos projects` |
| List Maven modules | `pharos modules` |
| Module dependency tree | `pharos deps <module> [--transitive]` |
| Dependency path between modules | `pharos mod-path <from> <to>` |
| Module public API surface | `pharos boundary <project>` |

## FQN Format

`com.example.MyClass#methodName(ParamType1,ParamType2)`

Use FQNs from `pharos search` results directly in `method`, `callers`, `callees`.

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

Run `pharos <command> --help` for full options on any command.
