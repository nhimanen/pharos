# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes multi-language projects and gives you multiple ways to navigate them:

- **Hybrid search** — BM25 keyword + semantic vector search with automatic query classification (`auto` default picks the right pipeline per query)
- **Named pipelines** — keyword / vector / unified / hybrid (Borda or RRF) / cross-encoder reranked / doc-type diversified, all defined in `pipelines.yaml` and selectable via `--pipeline`
- **Search filters** — restrict a search to a build module, a directory, a file extension or code vs. docs
- **MMR class diversity** — default last-stage reranker that surfaces hits from more classes before repeating overloads of the same one
- **Cross-encoder reranking** — optional second-stage rerank for hybrid pipelines when higher precision matters more than latency
- **Multi-vector indexing** — classes split into semantic chunks (header, method groups) and stored as `LateInteractionField` for ColBERT-style rescoring
- **Multi-provider embeddings** — opt-in; index against several embedding models side-by-side and A/B compare them with `pharos embed`
- **Knowledge graph** — inheritance, field access, annotations, and type references stored as typed edges in ArcadeDB alongside the call graph
- **Call graph exploration** — who calls what, multi-hop traversal with bodies, transitive impact analysis
- **Smart snippets** — Lucene Highlighter + vector chunk positioning, both combined for precise source location
- **Zero-result advisor** — fuzzy matches, token breakdown, and filter notes when queries return nothing
- **Module dependency graph** — Maven, Gradle, npm, Python, CMake and Go projects in one unified view
- **Project skeleton** — public API surface as a filesystem tree, scoped to any subdirectory
- **Claude skill + MCP server** — teaches Claude Code to use `pharos` for code navigation instead of grep
- **Multi-node mode** — writers index and publish, readers serve, a router spreads the load; sharding and live resharding; a Kubernetes operator ([docs/cluster.md](docs/cluster.md), [docs/operator.md](docs/operator.md))

## Requirements

- Java 25+
- Python 3.8+ (for the `pharos` CLI client)
- Maven (only to build from source)
- Node.js 14+ (optional — enables JavaScript/TypeScript indexing)
- `python3` with `python-hcl2` (optional — enables Terraform indexing: `pip install python-hcl2`)

## Install

### From a release (no Maven needed)

Each GitHub release ships the jar, its SHA-256, the Claude skill bundle, `pharos` and `setup.sh`:

```bash
./setup.sh --release latest     # or --release v1.12.0
```

It downloads the release, verifies the checksum, installs it, and restarts the daemon. Before installing
it warns about projects the new release would no longer serve and asks for confirmation (`--yes` skips
the prompt).

### From source

```bash
./setup.sh                 # build the jar, install it, restart the daemon
./setup.sh --skip-build    # reinstall the jar already in target/
./setup.sh --restart-only  # just restart the running daemon
```

`setup.sh` installs:

- the jar as `~/.pharos/bin/pharos.jar`;
- the `pharos` client as `~/.pharos/bin/pharos-cli.py`, linked from `~/.local/bin/pharos` (put
  `~/.local/bin` on your `PATH`);
- the Claude skill in `~/.claude/skills/pharos/`.

| Option | Meaning |
|---|---|
| `--release latest\|vX.Y.Z` | install a published release |
| `--skip-build` | install without rebuilding |
| `--restart-only` | only restart a running daemon |
| `--upgrade-indexes` | re-index projects whose index format is behind (after the install, with `--release`) |
| `--yes`, `-y` | do not ask for confirmation |

Environment: `PHAROS_PORT` (daemon port, default 7171), `PHAROS_HEAP` (`-Xmx` of the daemon),
`PHAROS_ARITHMETIC` (see [CPU-deterministic readers](docs/cluster.md#cpu-deterministic-readers)),
`PHAROS_RELEASE_BASE_URL` (release download base), `PHAROS_NO_DAEMON=1` (do not start the daemon).

Exit codes: 2 the upgrade warning was declined or needs `--yes`, 3 checksum mismatch; with
`--upgrade-indexes`, the exit code of `upgrade-indexes` (below).

`pharos --version` prints the installed jar's version and the client's location.

## Upgrading

Install the new release with `./setup.sh --release latest`, then read [UPGRADING.md](UPGRADING.md) for
anything to do by hand. [CHANGELOG.md](CHANGELOG.md) lists every change.

### Index formats

Every project index carries a format number (`index-format.json`). The current format is **2**.

Under the **N-1 rule**, a release serves its own format and the one before it: after an upgrade that
changes the format, existing indexes keep answering (status `upgrade-available`) and are rewritten by
their next `pharos index` run. Indexes older than N-1, or written by a newer release, are refused until
they are re-indexed.

```bash
pharos index-status            # each project's status: ok, upgrade-available, outdated, newer, missing, invalid
pharos index-status --json     # machine-readable (also role, graphDamage, readerCache)
pharos upgrade-indexes         # full re-index of every project that is not current (asks first)
pharos upgrade-indexes --dry-run
pharos upgrade-indexes --check # change nothing; exit 3 if this release would refuse an index
./setup.sh --upgrade-indexes   # the same as upgrade-indexes, with the installed jar
```

`upgrade-indexes` takes project names (default: all that need it), `--yes`, `--no-embed`, `--dry-run`
and `--check`. Embeddings come from the embedding cache, so a re-index mostly repeats the parse.
Projects stay searchable while they are re-indexed.

`upgrade-indexes` exit codes: **0** nothing to upgrade, all upgraded, or aborted at the prompt; **1** a
project was skipped (unknown, missing index, root gone) or failed; **2** confirmation needed but no
terminal (re-run with `--yes`); **3** `--check` found an index this release refuses (with an
object-store cluster: any shard published in the bucket); **5** refused on a cluster reader or a
work-queue writer (see [docs/cluster.md](docs/cluster.md#index-formats-and-upgrades)).

### Damaged call graphs

Releases before 1.10.0 could, rarely, write a call graph that opens without an error but decodes wrongly
(callers, callees, usages and call paths are then wrong). Pharos detects such graphs when it opens
them: it logs a WARN with the repair, `pharos index-status` prints a `warning:` line (`graphDamage` in
`--json`), and the project's next index run is a full one, which rebuilds the graph.
`pharos upgrade-indexes` re-indexes damaged projects too, and a cluster writer refuses to publish one.
The cross-project graph is rebuilt by the next `pharos link`; a damaged module graph is rebuilt empty
and refills as projects are indexed. See UPGRADING.md, "Upgrading to 1.10.0".

## Supported languages

| Language | Parser | Call graph | Knowledge graph |
|---|---|---|---|
| Java | JavaParser (full symbol resolution) | ✓ resolved FQNs | ✓ fields, inheritance, annotations, type refs |
| Go | Native Java parser (no toolchain needed) | ✓ resolved FQNs | ✓ struct fields, embedding, param/result type refs |
| Python | AST via `python3` subprocess | unresolved | ✓ inheritance, decorators, instance fields |
| JavaScript / TypeScript | AST via `node` subprocess | unresolved | ✓ extends, implements, decorators, class fields |
| Terraform | HCL via `python3` + `python-hcl2` | unresolved | — |
| Kotlin, Scala, Rust, Swift, C#, F#, Haskell, Elixir, Clojure, Ruby, VB.NET | Regex-based | — | — |
| Markdown, YAML, shell scripts | Generic chunker | — | — |

Go needs no `go` toolchain on PATH: `GoCodeParser` is pure Java. Methods are bound by
their **receiver** (`func (s *Server) Handle(...)`), and because Go has no function
overloading a method's identity is exactly `(receiver type, name)` — so parameter types
are omitted from the FQN and call resolution is exact rather than heuristic. Resolved
edges come from receiver calls, same-package functions, imported project packages, and
`v := &T{}` / `v := NewT()` local inference; anything else is left unresolved rather
than guessed.

## Module graph support

The module dependency graph is populated automatically during indexing from whichever build file is present:

| Build system | File | Module key |
|---|---|---|
| Maven | `pom.xml` | `groupId:artifactId` |
| Gradle | `settings.gradle` + `build.gradle` | `group:name` |
| Python | `pyproject.toml` / `setup.py` | `python:package` |
| Node.js | `package.json` | `npm:name` or `scope:name` |
| CMake | `CMakeLists.txt` | `cmake:projectname` |
| Go | `go.mod` / `go.work` | `go:module/path` |

## Usage

`pharos` is the Python client: it starts a background daemon on first use and sends every command to it.
`pharos <command> --help` shows each command's options.

```bash
# Index a project (runs in its own JVM; the daemon keeps serving)
pharos index /path/to/project                    # incremental
pharos index /path/to/project --full             # re-parse every file
pharos index /path/to/project --force --no-embed # delete the index first, skip embedding
pharos index /path/to/workspace --project-threads 4  # each sub-project, 4 at a time
pharos index ~/git/yeast:my-yeast                # path:name overrides the project name

# Search — auto-classifies the query and picks the best pipeline
pharos search "connection pool initialization"         # natural language → hybrid
pharos search "ConnectionPool"                         # identifier → keyword automatically
pharos search "validate token" --type unified          # single-pass BM25 + vector boost
pharos search "JWT expiry" --trace                     # show resolved type + timings
pharos search "authenticate" --snippet-lines 20        # control snippet size
pharos search "rate limit" --pipeline hybrid-reranked  # named pipeline (overrides --type)
pharos search "rate limit" --oversample 3              # fetch 3× candidates before rerank
pharos search "parser" --module core --language java   # filters, see below
pharos pipelines                                       # list available pipelines

# Get full method or class body
pharos method "com.pharos.indexer.LuceneIndexer#index(ParsedMethod)"
pharos class  "com.pharos.search.SearchEngine"
pharos class  "com.pharos.search.SearchEngine" --context   # + fields, constructors, callers

# Call graph
pharos callers "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos callees "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos impact  "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"   # all transitive callers
pharos path    "com.pharos.Main#main(String[])" "com.pharos.parser.JavaCodeParser#parseProject(Path,String)"

# Project structure as a filesystem tree
pharos skeleton myproject
pharos skeleton myproject --path src/main/java/com/example/api --depth 1

# Module dependencies
pharos modules
pharos deps com.pharos:pharos --transitive
pharos mod-path com.example:module-a com.example:module-b
pharos boundary myproject

# Web UI (graph browser and Cluster page, on the daemon's port 7171)
pharos web

# List indexed projects, index status
pharos projects
pharos index-status
```

`pharos search` exit codes: 0 results (or none), 1 error, 4 a project's index format is not served by
this release, 6 `--project` names a project without vectors of the search model (vector, hybrid or
unified search).

### Search filters

`--module` (the module's directory relative to the project root, the nearest directory with a build
file — not the Maven artifactId; `_` selects files outside any module), `--path-prefix` (a directory
relative to the project root, whole path segments), `--language` (the file extension,
case-insensitive) and `--kind` (`code` or `doc`). Filters only select documents: scores are those of the
unfiltered query. They need index format 2; a filtered search over an older project is refused with the
fix. The web API (`/api/search`: `module`, `pathPrefix`, `language`, `kind`) and MCP `search_code`
(`module`, `path_prefix`, `language`, `kind`) take the same filters.

### Commands only in the Java CLI

Some commands have no `pharos` client wrapper yet. Run them with the installed jar:

```bash
java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar <command> ...
```

| Command | What it does |
|---|---|
| `trace <fqn> [--depth 2] [--direction callees\|callers\|both] [--max-body 500]` | multi-hop call chain with bodies |
| `usages <fqn> [--kind all\|callers\|subclasses\|field_readers\|field_writers\|annotated\|type_refs]` | knowledge-graph usages |
| `link <project1> <project2>` | link two projects for cross-project call paths |
| `stats [--project <name>]` | index statistics |
| `mine <project> [--write]` | mine vocabulary and synonym candidates |
| `remove-index <name>` / `--all` | remove a project's index and registration |
| `mcp-server` | the MCP server (see below) |

The same operations are available as MCP tools (`trace_call_chain`, `find_usages`) and on the web API.

### Pipelines and reranking

Pipelines are defined in `src/main/resources/pipelines.yaml` and selected with `--pipeline ID` (overrides `--type`). The five `--type` values map onto the same pipelines.

| Pipeline | When to use |
|---|---|
| `auto` (default) | FST-backed intent classifier dispatches to a child pipeline based on query shape |
| `keyword` | Exact name or FQN lookup; highest BM25 precision |
| `vector` | Semantic / concept search when you don't know the exact name |
| `unified` | Single BM25 pass with vector similarity as a multiplicative boost |
| `hybrid` | Borda-count fusion of keyword + vector with agreement bonus |
| `hybrid-rrf` | Reciprocal Rank Fusion (k=60) — the auto default for hybrid intents |
| `hybrid-reranked` | `hybrid` + cross-encoder rerank for higher precision (requires cross-encoder) |
| `hybrid-ce-merge` | Cross-encoder scores all candidates and acts as the merge step |
| `hybrid-diverse` | `hybrid` + doc-type diversity rerank (balances method/class/chunk hits) |
| `hybrid-reranked-diverse` | Cross-encoder rerank followed by doc-type diversity |

MMR class diversity runs as the default last-stage reranker on every pipeline except `keyword`.

**Cross-encoder reranking.** `hybrid-reranked` and `hybrid-ce-merge` use a cross-encoder
(`Xenova/ms-marco-MiniLM-L-6-v2`, downloaded and loaded on the first rerank). It fuses the first-stage rank with the
cross-encoder's rank (equal-weight RRF, k=60), so its scores are on the RRF scale, not probabilities.
`auto` does not dispatch to the reranked pipeline: in an A/B it did not beat plain hybrid on both
embedding models. Set `"crossEncoderEnabled": false` in `~/.pharos/config.json` to never load the model
(the reranked pipelines then run without the cross-encoder step, as they do when the model cannot be
loaded).

**Stable tie order.** Keyword search ranks equal scores by document id, type and line range, not by
Lucene's internal doc id, so two builds of the same source (a re-index, a resharded cluster project)
return the same page, scores included.

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency:

```bash
pharos daemon status
pharos daemon start
pharos daemon stop
pharos daemon restart
pharos daemon logs
```

Environment overrides honored on every JVM launch (daemon and `index`):

| Var | Purpose |
|---|---|
| `PHAROS_PORT` | Daemon HTTP port (default 7171) |
| `PHAROS_HEAP` | Passed as `-Xmx<value>` (e.g. `PHAROS_HEAP=16g pharos index ...`); default 75 % of RAM, 2–24 GB |
| `PHAROS_ARITHMETIC` | `deterministic` or `native` vector arithmetic; only matters on cluster readers |

## Embeddings

Vector search is **off by default**; nothing is downloaded until you opt in. The recommended model is
**granite-embedding-english-r2**:

```bash
pharos embeddings enable           # writes the granite-r2 provider to ~/.pharos/config.json
pharos daemon restart
pharos embeddings status           # prints the index command for every project without vectors
pharos index --full <root path>    # per project (index takes the directory, not the name)
```

`enable` only edits the config (expect a 600 MB download and a CPU-heavy first embed later). Keyword
search and the call graph work without embeddings. Other models, several providers side by side,
remote embedding servers, fallbacks and the embedding cache: [docs/embeddings.md](docs/embeddings.md).

## File size limits

Non-code files (Markdown, text, JSON, YAML, …) are indexed as a single document holding the whole file, so large data files (word lists, tokenizer vocabularies, cached JSON) are skipped rather than loaded into memory. Code is split into per-method documents and has a much higher safety cap. Both are set in `~/.pharos/config.json`, in KB; `0` disables a limit:

```json
{
  "maxDocumentFileSizeKb": 10240,
  "maxSourceFileSizeKb": 102400
}
```

A file that grows past its limit is removed from the index on the next incremental run.

## Claude Code integration

### Skill

`setup.sh` installs the skill to `~/.claude/skills/pharos/` automatically. To install manually:

```bash
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md
```

Once installed, Claude Code calls `pharos search`, `pharos callers`, `pharos impact`, `pharos class --context`, etc. when navigating indexed projects instead of falling back to grep.

### MCP server (alternative)

```bash
claude mcp add -s user pharos -- java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar mcp-server
```

All MCP tools return structured JSON:

| Tool | Description |
|---|---|
| `search_code` | BM25/vector/hybrid/unified search with auto-classification, filters, snippets, and zero-result hints |
| `get_method` | Full method body by FQN |
| `get_methods` | Batch method/class lookup in one Lucene pass |
| `get_class` | Class body; add `context: true` for fields + constructors + public methods + callers |
| `get_callers` / `get_callees` | Direct call graph edges |
| `trace_call_chain` | Multi-hop BFS with bodies, direction, body truncation |
| `find_transitive_callers` | All unique transitive callers (impact analysis), flat deduplicated set |
| `find_usages` | Knowledge-graph usages: callers, subclasses, field readers/writers, annotations, type refs |
| `find_call_path` | Shortest call chain between two methods |
| `list_projects` | Indexed projects with stats |
| `list_modules` | Modules in the dependency graph |
| `get_module_deps` | Direct/transitive module deps |
| `find_module_path` | Shortest dependency path between modules |
| `get_module_boundary` | Entry/exit points for a module |

## Multi-node and Kubernetes

Without a `cluster` section in `config.json`, pharos is the single-node tool described above. With one,
nodes take roles:

- **writers** index projects and publish each run as an immutable generation, either to an S3-compatible
  bucket (`object-store`) or from their own disk (`peer-pull`, experimental, no bucket);
- **readers** sync the generations and answer queries, fanning a query out to the readers holding the
  other shards;
- **`pharos router`** spreads requests over the healthy readers.

Work-queue writers share the projects under leases and take over a dead writer's shards; large projects
can be split into shards and resharded live with `pharos reshard`; `/cluster/status`, an event stream
and the web UI's **Cluster** page show the cluster and its scaling proposals.

| Guide | Covers |
|---|---|
| [docs/cluster.md](docs/cluster.md) | roles, backends, configs for each topology, readers, writers, fan-out, the router, status, scaling proposals, sharding, upgrades, draining, TLS, security |
| [docs/operator.md](docs/operator.md) | the Kubernetes operator: install, the `PharosCluster` resource, MinIO or S3, git projects, autoscaling, upgrades, teardown |
| [docs/docker.md](docs/docker.md) | the container image: entrypoint, `PHAROS_ROLE`, volumes, the model cache |

## How it works

1. **Parse** — JavaParser (with full symbol resolution), a native Go parser, Python AST, Node.js AST, and regex-based parsers extract methods, classes, call references, field declarations, inheritance, annotations, and type references

2. **Knowledge graph** — call references plus structural relationships (inheritance, field access, annotations, type refs) stored as typed edges in an embedded ArcadeDB graph alongside the call graph; enables `find_usages`, subclass hierarchy, and field impact analysis

3. **Embed** — optional semantic vectors via DJL + ONNX; documents split into logical chunks (method = 1 chunk, class = header + method-group chunks) and stored as `LateInteractionField` for multi-vector rescoring; falls back gracefully if unavailable

4. **Index** — Lucene stores all fields with per-field BM25; `KnnFloatVectorField` (mean of chunk vectors) for HNSW retrieval; `LateInteractionField` for late-interaction rescoring

5. **Search** — pipelines defined in `pipelines.yaml` compose retrievers (keyword / vector / unified), a merger (Borda / RRF / cross-encoder-merge), and rerankers (cross-encoder, doc-type diversity, MMR class diversity). The FST-backed `auto` router classifies query intent and dispatches to a child pipeline; `--pipeline` overrides it explicitly

Config and indexes are stored in `~/.pharos/` (`config.json`, `registry.json`, `indexes/`).
