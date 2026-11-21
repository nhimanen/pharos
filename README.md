# Pharos

The Pharos of Alexandria was one of the Seven Wonders of the Ancient World — a lighthouse that guided sailors safely through fog and storm into port. Pharos does the same for large codebases: when you're lost in thousands of methods and tangled dependencies, it illuminates the structure and guides you from confusion to clarity.

## What it does

Pharos indexes multi-language projects and gives you multiple ways to navigate them:

- **Hybrid search** — BM25 keyword + semantic vector search with automatic query classification (`auto` default picks the right pipeline per query)
- **Named pipelines** — keyword / vector / unified / hybrid (Borda or RRF) / cross-encoder reranked / doc-type diversified, all defined in `pipelines.yaml` and selectable via `--pipeline`
- **MMR class diversity** — default last-stage reranker that surfaces hits from more classes before repeating overloads of the same one
- **Cross-encoder reranking** — optional second-stage rerank for hybrid pipelines when higher precision matters more than latency
- **Multi-vector indexing** — classes split into semantic chunks (header, method groups) and stored as `LateInteractionField` for ColBERT-style rescoring
- **Multi-provider embeddings** — index against several embedding models side-by-side and A/B compare them with `pharos embed`; query-time provider can differ from index-time runtime
- **Knowledge graph** — inheritance, field access, annotations, and type references stored as typed edges in ArcadeDB alongside the call graph
- **Call graph exploration** — who calls what, multi-hop traversal with bodies, transitive impact analysis
- **Smart snippets** — Lucene Highlighter + vector chunk positioning, both combined for precise source location
- **Zero-result advisor** — fuzzy matches, token breakdown, and filter notes when queries return nothing
- **Module dependency graph** — Maven, Gradle, npm, Python, and CMake projects in one unified view
- **Project skeleton** — public API surface as a filesystem tree, scoped to any subdirectory
- **Claude skill + MCP server** — teaches Claude Code to use `pharos` for code navigation instead of grep

## Requirements

- Java 25+
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

## Install from a release

Tagged releases (GitHub Releases) ship the jar, its SHA-256, the Claude skill bundle, `pharos` and `setup.sh`:

```bash
./setup.sh --release latest     # or --release v1.4.0; downloads, verifies the checksum, installs
```

## Upgrading

Upgrades may change the on-disk index format. A release serves its own format and the previous one,
so existing indexes keep working; re-index when convenient with `./setup.sh --upgrade-indexes`
(or `pharos upgrade-indexes`). Check status with `pharos index-status`. See [UPGRADING.md](UPGRADING.md)
and [CHANGELOG.md](CHANGELOG.md).

`pharos upgrade-indexes` exit codes: **0** nothing to upgrade, all upgraded, or aborted at the prompt;
**1** a project was skipped (unknown, missing index, root gone) or failed; **2** confirmation needed but
no terminal (re-run with `--yes`); **5** refused on a cluster reader node (upgrade and re-publish on the
writer). `./setup.sh --upgrade-indexes` exits with the same code.

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

```bash
# Index a project
pharos index /path/to/project                    # incremental
pharos index /path/to/project --force --no-embed # clean re-index, skip embedding
pharos index /path/to/workspace --project-threads 4  # parallel workspace indexing

# Search — auto-classifies the query and picks the best pipeline
pharos search "connection pool initialization"         # natural language → hybrid
pharos search "ConnectionPool"                         # identifier → keyword automatically
pharos search "validate token" --type unified          # single-pass BM25 + vector boost
pharos search "JWT expiry" --trace                     # show resolved type + timings
pharos search "authenticate" --snippet-lines 20        # control snippet size
pharos search "rate limit" --pipeline hybrid-reranked  # named pipeline (overrides --type)
pharos search "rate limit" --oversample 3              # fetch 3× candidates before rerank
pharos pipelines                                       # list available pipelines

# Get full method or class body
pharos method "com.pharos.indexer.LuceneIndexer#index(ParsedMethod)"
pharos class  "com.pharos.search.SearchEngine"
pharos class  "com.pharos.search.SearchEngine" --context   # + fields, constructors, callers

# Call graph
pharos callers "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos callees "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"
pharos trace   "com.pharos.search.SearchEngine#search(SearchRequest,boolean)" --direction callees --depth 2
pharos impact  "com.pharos.search.SearchEngine#search(SearchRequest,boolean)"   # all transitive callers
pharos path    "com.pharos.Main#main(String[])" "com.pharos.parser.JavaCodeParser#parseProject(Path,String)"

# Knowledge graph
pharos usages "com.pharos.search.SearchEngine"                 # all usage kinds
pharos usages "com.pharos.search.SearchEngine" --kind subclasses
pharos usages "com.pharos.indexer.LuceneIndexer#indexer" --kind field_readers
pharos usages "Cacheable" --kind annotated

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

### Pipelines

Pipelines are defined in `src/main/resources/pipelines.yaml` and selected with `--pipeline ID` (overrides `--type`). The five legacy `--type` values still work and map onto the same pipelines.

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

### Daemon

`pharos` keeps a background JVM running to avoid cold-start latency:

```bash
pharos daemon status
pharos daemon stop
pharos daemon logs
```

Environment overrides honored on every JVM launch (daemon and `index`):

| Var | Purpose |
|---|---|
| `PHAROS_PORT` | Daemon HTTP port (default 7171) |
| `PHAROS_HEAP` | Passed as `-Xmx<value>` (e.g. `PHAROS_HEAP=16g pharos index ...`) |

## Claude Code integration

### Skill

`setup.sh` installs the skill to `~/.claude/skills/pharos/` automatically. To install manually:

```bash
mkdir -p ~/.claude/skills/pharos
cp .claude/skills/pharos/SKILL.md ~/.claude/skills/pharos/SKILL.md
```

Once installed, Claude Code calls `pharos search`, `pharos trace`, `pharos usages`, `pharos impact`, etc. when navigating indexed projects instead of falling back to grep.

### MCP server (alternative)

```bash
claude mcp add -s user pharos -- java --enable-native-access=ALL-UNNAMED -jar ~/.pharos/bin/pharos.jar mcp-server
```

All MCP tools return structured JSON. Key tools:

| Tool | Description |
|---|---|
| `search_code` | BM25/vector/hybrid/unified search with auto-classification, snippets, and zero-result hints |
| `get_method` | Full method body by FQN |
| `get_methods` | Batch method/class lookup in one Lucene pass |
| `get_class` | Class body; add `context: true` for fields + constructors + public methods + callers |
| `get_callers` / `get_callees` | Direct call graph edges |
| `trace_call_chain` | Multi-hop BFS with bodies, direction, body truncation |
| `find_transitive_callers` | All unique transitive callers (impact analysis), flat deduplicated set |
| `find_usages` | Knowledge-graph usages: callers, subclasses, field readers/writers, annotations, type refs |
| `find_call_path` | Shortest call chain between two methods |
| `list_projects` | Indexed projects with stats |
| `get_module_deps` | Direct/transitive module deps |
| `find_module_path` | Shortest dependency path between modules |
| `get_module_boundary` | Entry/exit points for a module |

## Embedding configuration

Vector search is **disabled by default**. To enable it, add at least one provider to `embeddingProviders[]` in `~/.pharos/config.json`. The list is the new shape — legacy single-key configs (`embeddingModelUrl` at the top level) are migrated on load and not written back.

### Recommended: Jina v2 Code via DJL (local, no API key)

```json
{
  "embeddingProviders": [
    {
      "modelId": "jina-v2-code",
      "type": "djl",
      "url": "hf://jinaai/jina-embeddings-v2-base-code",
      "dimensions": 768,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "jina-v2-code"
}
```

On first index run, DJL downloads the ONNX model from HuggingFace Hub and caches it in `~/.djl.ai/pharos/`. Subsequent runs load from cache. This is the recommended setup for code search — 768-dim, 8 192-token context window.

### Alternative: MiniLM via DJL model zoo

```json
{
  "embeddingProviders": [
    {
      "modelId": "minilm",
      "type": "djl",
      "url": "ai.djl.huggingface.onnxruntime:sentence-transformers/all-MiniLM-L6-v2",
      "dimensions": 384,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "minilm"
}
```

Lighter model (384-dim), good for general text, lower RAM use.

### Alternative: OpenAI-compatible HTTP server

If you run a local embedding server (e.g. llama.cpp, Ollama, text-embeddings-inference), set `type: openai`:

```json
{
  "embeddingProviders": [
    {
      "modelId": "qwen3-emb",
      "type": "openai",
      "url": "http://localhost:8083/v1",
      "dimensions": 768,
      "maxTokens": 512
    }
  ],
  "searchEmbeddingModel": "qwen3-emb"
}
```

### Multiple providers side-by-side

`embeddingProviders` is a list. Pharos can index against several models in parallel and store one vector field per model (`vec.<modelId>`) on each Lucene document. `searchEmbeddingModel` selects which one is queried; the others stay on disk for cheap A/B comparison via `pharos embed --model=<other>`. A vector, hybrid or unified search over several projects leaves a project without vectors of the search model out of its vector part, with the warning `project <name> has no vectors for model <id>; vector results exclude it` (its keyword part still runs, and unified scores its documents by BM25 alone, as any document without vectors); a search scoped to that one project fails with a message naming the `pharos embed` command to run.

### Split index/search runtimes

For setups where the index-time runtime differs from the query-time runtime (e.g. embed at index time via a fast remote `openai` server, embed queries offline via local DJL), add `searchEmbeddingProvider`:

```json
{
  "embeddingProviders": [
    { "modelId": "qwen3-emb", "type": "openai", "url": "http://prod-emb:8083/v1", ... }
  ],
  "searchEmbeddingProvider": {
    "modelId": "qwen3-emb",
    "type": "djl",
    "url": "hf://Qwen/Qwen3-Embedding-0.6B",
    "dimensions": 1024,
    "maxTokens": 512
  }
}
```

The `modelId` must match one of the `embeddingProviders` entries (that's how pharos routes the query to the right `vec.<modelId>` field). You are on the hook for ensuring both runtimes produce vectors in the same space — typically by serving the same underlying weights under both runtimes.

### Fallback: remote-with-local-backup

An `embeddingProviders` entry can list `fallbacks` — alternate runtimes for the *same* logical model, tried in order at index time when the primary runtime is unreachable. Handy when the primary is a not-always-up remote box:

```json
{
  "embeddingProviders": [
    {
      "modelId": "jina-code-v2",
      "type": "openai",
      "url": "http://192.168.1.100:8083/v1",
      "model": "jinaai/jina-embeddings-v2-base-code",
      "dimensions": 768,
      "fallbacks": [
        { "type": "djl", "url": "hf://jinaai/jina-embeddings-v2-base-code" }
      ]
    }
  ],
  "searchEmbeddingProvider": {
    "modelId": "jina-code-v2",
    "type": "djl",
    "url": "hf://jinaai/jina-embeddings-v2-base-code",
    "dimensions": 768
  }
}
```

A `fallbacks` entry inherits the parent's `modelId`/`dimensions` when omitted; if given explicitly they must match the parent's — mismatched dimensions would corrupt the shared `vec.jina-code-v2` field, and a mismatched `modelId` would defeat the whole point. As with `searchEmbeddingProvider`, you're on the hook for verifying the fallback runtime produces vectors close enough to the primary's to be usable interchangeably in the same vector space.

`pharos index` and `pharos embed` each spawn a fresh JVM per invocation, so index-time fallback resolution re-probes the primary's reachability on every run — no daemon restart needed to pick up a remote server coming back online. The Lucene field and embedding cache key are always derived from the parent config, so vectors written via the fallback one day and the primary the next land in the same `vec.<modelId>` field/cache namespace — no forced re-embed when the runtime flips.

### Embedding cache & re-embedding

Embedding vectors are cached on disk at `<index-dir>/<project>/embed-cache.bin`, keyed by `SHA-256(embeddingText)`. Three commands manage it:

```bash
pharos backfill-embedding-cache myproject              # seed cache from existing HNSW vectors (no ONNX)
pharos backfill-embedding-cache --all                  # all projects

pharos embed --model=qwen3-emb myproject               # add a model's vectors to an existing index
pharos embed --model=qwen3-emb --all                   # all projects
pharos embed --model=qwen3-emb myproject --force       # overwrite existing vectors

pharos invalidate-embeddings myproject                 # docs only (default scope)
pharos invalidate-embeddings myproject --scope java    # Java files only
pharos invalidate-embeddings myproject --scope all     # everything
pharos invalidate-embeddings --all                     # docs in every project
```

Use `embed` when adding a new model without re-parsing. Use `invalidate-embeddings` after changing chunking logic to force selective re-embedding on the next incremental index run.

### Skipping embeddings

```bash
pharos index /path/to/project --no-embed     # skip embedding this run
```

Keyword search (`--type keyword`) and call graph features work without embeddings. Only `vector`, `hybrid`, and `unified` search types require them.

## File size limits

Non-code files (Markdown, text, JSON, YAML, …) are indexed as a single document holding the whole file, so large data files (word lists, tokenizer vocabularies, cached JSON) are skipped rather than loaded into memory. Code is split into per-method documents and has a much higher safety cap. Both are set in `~/.pharos/config.json`, in KB; `0` disables a limit:

```json
{
  "maxDocumentFileSizeKb": 10240,
  "maxSourceFileSizeKb": 102400
}
```

A file that grows past its limit is removed from the index on the next incremental run.

## Multi-node (experimental)

Pharos can run as one **writer** and any number of **readers** that share an S3-compatible object store (AWS S3, MinIO, …). The writer indexes as usual and, after each index run, publishes the project as a new immutable generation. Readers poll the store every `pollIntervalSeconds`, download new generations into `cacheDir`, and serve search and call-graph queries from them. Writers and readers never talk to each other. Without a `cluster` section, pharos is the single-node tool described above (`role: all`, `backend: local`).

Writer node, in `~/.pharos/config.json` (this example lists every key; a writer ignores `cacheDir` and `pollIntervalSeconds`, which only readers use):

```json
{"cluster": {
  "role": "writer",
  "backend": "object-store",
  "nodeId": "node-a",
  "pollIntervalSeconds": 5,
  "cacheDir": "/var/cache/pharos",
  "retentionGenerations": 4,
  "retentionMinutes": 120,
  "objectStore": {
    "endpoint": "http://localhost:9000",
    "region": "eu-west-1",
    "bucket": "pharos-bucket",
    "prefix": "indexes",
    "pathStyle": false,
    "accessKeyEnv": "PHAROS_ACCESS_KEY",
    "secretKeyEnv": "PHAROS_SECRET_KEY"
  }}}
```

> **Note on `pathStyle`:** the examples show `"pathStyle": false` (virtual-hosted-style URLs, as AWS S3 expects) next to a `localhost:9000` endpoint only to show every key. A MinIO endpoint such as `http://localhost:9000` needs `"pathStyle": true` (the default when the key is omitted).

Reader node (same `objectStore` section; `cacheDir` is where generations are kept):

```json
{"cluster": {
  "role": "reader",
  "backend": "object-store",
  "nodeId": "node-b",
  "pollIntervalSeconds": 5,
  "cacheDir": "/var/cache/pharos",
  "objectStore": {
    "endpoint": "http://localhost:9000",
    "region": "eu-west-1",
    "bucket": "pharos-bucket",
    "prefix": "indexes",
    "pathStyle": false,
    "accessKeyEnv": "PHAROS_ACCESS_KEY",
    "secretKeyEnv": "PHAROS_SECRET_KEY"
  }}}
```

- Credentials come from the environment variables named by `accessKeyEnv` / `secretKeyEnv` (set both or neither; a named variable that is unset or empty is an error, not a silent fallback), or, when neither is configured, the AWS default credential chain. Use `"pathStyle": true` for MinIO.
- `nodeId` defaults to the host name. `pollIntervalSeconds` must be positive, `retentionMinutes` non-negative, `retentionGenerations` at least 2, and `prefix` must not start with `/` or contain `.`/`..` segments.
- `objectStore.readTimeoutSeconds` (default 10, 1–3600) bounds the store's connect time and each attempt of a reader's ranged read; a whole ranged read, retries included, gets three times that (30 s). A query whose block can't be fetched in that time fails with an error naming the shard and object key, instead of stalling while the bucket is unreachable. That wait starts when the query asks for the block, so it includes time queued behind other fetches (`maxConcurrentFetches` at once): under a burst of cold misses a block can time out although the bucket answers each fetch quickly. Raise `maxConcurrentFetches` or `readTimeoutSeconds` if timeouts appear only under load. Other calls (whole-object downloads, the writer's uploads, catalog reads) wait up to `readTimeoutSeconds` or 30 s, whichever is longer, between reads of a response.
- `retireGraceSeconds` (default 120) is how long a reader keeps a shard's directory after it stops serving it, so queries and shard calls already reading it finish. It must be at least `2 x pollIntervalSeconds` and at least the fan-out timeout (`fanoutTimeoutMillis`, default 5000; `connectTimeoutMillis`, default 1000, bounds the connect). Both rules are checked at startup and the message names the fields.
- Readers also serve `/internal/*` (term statistics, shard search, snippets, callees, graph lookups) to other readers, guarded by the shared `cluster.token` sent as `X-Pharos-Cluster-Token`; a reader without a token does not start, and a missing or wrong token gets 401. Single-node (`role: all`) has no `/internal/*` routes. A reader's web server accepts request bodies up to 4 MiB on every route, not only `/internal/*`.
- **Fan-out.** Any reader takes any query. A reader holding every shard a query needs searches locally, exactly as a single node. Otherwise it coordinates: it classifies and embeds the query once, sums BM25 term statistics over all the query's shards, and merges candidates in two rounds where a single index would pick a global pool first (vector first pass, unified recall: the global top candidates are chosen, then rescored on their shards), so rankings equal a single node's over the same shards; graph boost, fusion and reranking run once over the merged lists. Each query pins one reader and generation per shard (a dead reader is retried once on another replica of the same generation) and has one budget, `cluster.queryTimeoutMillis` (default 20000, or `fanoutTimeoutMillis` if larger; at least `fanoutTimeoutMillis` and below 60000), shared by all its rounds. A shard no reader answers for is left out with a warning, `shard <name> unavailable: <reason>; results exclude it` (when that leaves nothing, because no shard of the call answered and the reader holds none of them, the web API answers HTTP 503 `{"error": "no shard answered (…)", "warnings": […]}`, which `pharos router` retries once on another reader, and MCP a retryable tool error, instead of an empty result); a shard without vectors of the coordinator's search model is left out of the vector part with `project <name> has no vectors for model <id>; vector results exclude it`, as on a single node (a search scoped to that one project fails, also as on a single node: HTTP 409 on the web API, a tool error on MCP), so the answer does not depend on which reader serves it. Synonym expansions are scored with every expanded term's corpus-wide statistics, including terms a shard does not contain. A query ships the statistics of at most 5 000 terms (synonym expansions included); terms beyond that are scored with each shard's own statistics, with the warning `query expands to N terms; …`. A query naming a project (`project:query`, or the bare name) boosts that project's results on any reader, whether or not it holds the project. Coordinator and shards must share the same `synonyms.txt` (a reader with other synonyms refuses with 409 and is skipped). The shard catalog is re-read in the background every 5 s; past 60 s old an unscoped query carries `routing with a catalog N s old`, and past `cluster.maxCatalogAgeSeconds` (default 600), or before a first good read, unscoped queries and graph tools answer from the reader's own shards only, with a warning naming the catalog's age and the shards not searched (a reader holding every shard keeps answering completely, as before fan-out). Known approximations: ties between shards break by shard order, and vector and unified retrieval across several shards approximate a single index (each shard's HNSW search and unified candidate pool are its own).
- **Trust model.** Write access to the bucket is cluster control: whoever can write `catalog/readers/*.json` can set a reader's `advertiseUrl` and so redirect the token-bearing shard calls of every coordinator to a host of their choosing (`advertiseUrl` must be `http://` or `https://`). Restrict bucket write access to cluster nodes. The token travels in clear text on `/internal/*` unless TLS fronts the readers (use `https://` advertise URLs); the endpoints cap request size (4 MiB) and collection sizes (10 000 ids/terms, 4096-dim vectors) and a reader without its shard service answers 503 so coordinators fail over.
- **Request distribution.** Each reader heartbeats into `catalog/readers/<node>.json` (every `heartbeatSeconds`, default 10; a reader whose heartbeat is older than `readerTtlSeconds`, default 30, is dropped) with the shards it holds and its `state` (`warming`, `ready`, `degraded` with `degradedShards`, or `draining`); `GET /cluster/readers` on a reader lists them. Put readers behind any load balancer that routes on `/health` (200 ok or degraded; 503 warming or draining), or run `pharos router --port 7080` (`./pharos router`): a stateless router that lists the live readers from the bucket, polls each one's `/health` every 2 s and round-robins `/api/*` and `/health` over those that answered 200. It retries an idempotent request (any `GET`, and `POST /api/search`) once on another reader after a connect error or a 5xx, answers 503 when no reader is healthy, and never forwards `/internal/*` (404; nor any path outside `/api/*` and `/health`, or with percent-encoding, dot segments or `;`). It adds `X-Forwarded-For`, drops hop-by-hop headers (and those a `Connection` header names), answers 400 for a request URI it cannot forward, 502 for a reader answer over 64 MiB, and 504 when a forwarded exchange (response headers and body read together) takes longer than 120 s; such a request is not resent. It needs the cluster config with `"backend": "object-store"`, the `objectStore` section and `cluster.token`, but no index, and never sends the token on.
- **Draining.** On SIGTERM a serving reader drains before it stops: it rewrites its heartbeat with `draining` (coordinators stop sending it shard calls) and answers `/health` with HTTP 503 `{"status": "draining"}` (the router drops it within one 2 s poll), keeps serving (requests still arriving are answered), and stops once it has been draining for at least 4 s (a router poll plus its probe) and one `heartbeatSeconds` plus 2 s (a coordinator's tick plus its bucket latency), and no request has been in flight for the last second; at most after `cluster.drainSeconds` (default 20, 0–3600). It then removes its heartbeat. The drain runs from the signal handler before any JVM shutdown hook, so ONNX Runtime's own hook cannot free the model under a query still being served; every long-running pharos process that embeds (`web`, `mcp-server`, `index`, `embed`, `upgrade-indexes`) stops in that order (drain, then running model calls, then exit 143/130), and a second signal exits at once. Give the process manager a stop timeout of at least `drainSeconds` + 15 s (the longest wait for running model calls) plus a margin for the exit itself: with the default `drainSeconds` of 20, Kubernetes `terminationGracePeriodSeconds` must be above 35, e.g. 45. A shorter timeout sends SIGKILL in the middle of the drain or the model wait.
- The writer keeps the newest `retentionGenerations` generations, and anything younger than `retentionMinutes`; older ones are garbage-collected after a publish.
- Readers serve a generation **lazily** by default: only the manifest, the graph and the small Lucene files (`segments_N`, `.si`, `.fnm`, `.cfe`, any file up to `eagerMaxBytes`, and a `.cfs` compound file of at most one block) are downloaded; every other file is read in blocks of `blockSizeBytes` through a local block cache in `<cacheDir>/.blocks`, capped at `cacheMaxBytes` (least recently used blocks are evicted). Before a new generation is swapped in, the blocks of the `prefetchExtensions` files (term dictionaries, vector graphs) are prefetched, waiting at most `prefetchTimeoutSeconds`. `"mode": "full"` copies every file instead. The optional `cluster.reader` section, with its defaults:

  ```json
  "reader": {"mode": "lazy", "cacheMaxBytes": 8589934592, "blockSizeBytes": 8388608, "eagerMaxBytes": 1048576,
             "prefetchExtensions": ["tip", "tmd", "tim", "vex", "vem"], "prefetchTimeoutSeconds": 30,
             "maxConcurrentFetches": 16, "privateCacheMaxBytes": 0}
  ```

  `blockSizeBytes` must be a power of two between 64 KiB and 64 MiB, and `cacheMaxBytes` at least four blocks.

  The block cache belongs to one process: the reader that syncs takes `<cacheDir>/.blocks`. Any other pharos process on the same `cacheDir` (an MCP server next to the daemon, a one-shot `pharos search`) gets its own private cache in `<cacheDir>/.blocks-private/`, which costs up to `privateCacheMaxBytes` more disk per such process (default 0 = `min(cacheMaxBytes, max(4 blocks, cacheMaxBytes / 8))`, i.e. 1 GiB for the defaults) and is deleted when the process exits. The process holds a file lock inside its private cache for its lifetime, and the syncing reader deletes the private caches whose lock is free (their process is gone), whatever their PID says — so this also works for containers in separate PID namespaces sharing a `cacheDir` volume.

  **Disk for a lazy reader's `cacheDir`:** the eager files of two generations per shard (the served one and the previous one) + the compact graph of each + `cacheMaxBytes` + (`maxConcurrentFetches` + prefetch threads) × `blockSizeBytes` for fetches in flight (prefetch threads = `min(maxConcurrentFetches, max(2, maxConcurrentFetches / 4))`, 4 by default) + `privateCacheMaxBytes` per extra process on the same `cacheDir`. With the defaults and no extra process: eager files + graphs + 8 GiB + 20 × 8 MiB. Plan for a whole served generation on top if the reader may be held back by a format refusal (see below).

  **Retention and lagging lazy readers.** A lazy reader reads its served generation's objects from the bucket on every cache miss, so it depends on the writer's GC keeping them: `retentionMinutes` must exceed the longest time a reader may lag behind the newest generation. Readers lag legitimately when the format gate refuses a newer generation during a rolling N-1 upgrade (writers upgraded first), when syncs keep failing, and after a restart following downtime. When the format gate refuses a newer generation, the reader starts downloading the rest of its served generation in the background (logged, smallest files first, stopping with a WARN when the disk would keep less than the block cache's remaining room plus 64 MiB), so it becomes self-sufficient before GC deletes the objects. If a miss still finds an object of the served generation gone, the reader logs one ERROR per generation (`generation N of shard S was garbage-collected on the bucket; this reader is behind retention …`) and `/health` answers HTTP 200 `{"status": "degraded", "degradedShards": ["S"], "reason": …}`: the node stays routable for its other shards, its heartbeat lists the shard under `degradedShards` (with `state` `degraded`), and coordinators route that shard to other readers only (a query whose shard is degraded on every reader serving it names it in its warnings; a search of the shard on the degraded reader itself carries `shard S is degraded on this reader: …`). It answers `ok` again once a newer generation is served. Readers that must outlive retention (long maintenance windows, readers upgraded last) should use `"mode": "full"`.

  Switching a reader from `lazy` to `full` leaves `<cacheDir>/.blocks` behind; the full-mode reader deletes it when it starts syncing, unless another process on the same `cacheDir` still holds the block cache.

  A process that began on a private cache moves to the shared one when its lock frees up (a follower that takes over syncing); readers it opened earlier keep using the private cache until the process exits.

  Operators see the cache of the process they ask: `pharos index-status --json` and `/health` carry a `readerCache` object on every reader (not on role `all`), `{mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate, bytesFetched, evictions, leasesOpen, shared}` (`shared` is false on a private cache; `source` is `process`, or `daemon` when `./pharos index-status --json` substituted the running daemon's figures; a `mode: "full"` reader reports zeros; `hitRate` is 1.0 while there have been no lookups, and the text line then says `hit rate n/a`). `pharos index-status` prints one line, `reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions`, taken from the running daemon when there is one (a one-shot command has served no queries). A hit rate that stays low while `evictions` climb means `cacheMaxBytes` is smaller than the working set.
- If publishing fails at the end of an index run (store unreachable, bad credentials), the local index is kept and `pharos index` exits with code **3**. Retry the upload without re-indexing:

  ```bash
  pharos publish myproject      # writer only; prints "published myproject generation N"
  ```

  `pharos publish` refuses (exit code **4**) an index it would not serve at the current format: a broken `callgraph.current` pointer, or an index format other than this build's (including an `upgrade-available` one). Re-index with `pharos index --full` first.
- Every manifest carries the shard's index format stamp (`index-format.json`); a manifest published before the stamp shipped counts as format 0. A reader only materialises generations whose format it can serve (its own, or N-1). Otherwise it keeps serving the generation it has and logs a WARN naming the shard, both formats and the fix (upgrade the reader, or re-index and re-publish on the writer). Served generation dirs carry the stamp, so `pharos index-status` works on readers.

- Readers refuse `index`, `link`, `embed`, `mine`, `remove-index`, `backfill-embedding-cache` and `upgrade-indexes` (also `./setup.sh --upgrade-indexes`): readers receive upgraded indexes by syncing, so upgrade and re-publish on the writer. `index-status --json` reports the node's `role`. They start syncing when the daemon (`pharos web` / `mcp-server`) starts, and serve class bodies and snippets from the index, never from source files on the reader machine.
- `pharos web` on a reader binds its port at once and loads its search models (embedder, cross-encoder) in parallel with the first sync pass. Until both are done, `/health` answers HTTP 503 `{"status": "warming", "ready": false}` (a served lazy generation that lost objects on the bucket answers HTTP 200 `{"status": "degraded", "degradedShards": […]}` instead, see above; a draining reader answers HTTP 503 `{"status": "draining"}`) (load balancers and the router hold traffic on it; the log line `Reader … serving N shard(s)` appears only then), and searches answered meanwhile carry the notice in the `X-Pharos-Warnings` header (the same JSON array that lists skipped incompatible projects; `./pharos` prints it on stderr). Only one process per `cacheDir` syncs (it holds `<cacheDir>/.sync.lock`); another daemon on the same `cacheDir` (e.g. `mcp-server` next to `web`) serves the generations the lock holder downloads, and takes over syncing when that process exits. Only `pharos web` joins reader membership (heartbeat, `/internal/*`, the shutdown drain): `pharos mcp-server` on a reader never advertises itself, so it cannot overwrite, drain or delete the heartbeat of a `web` daemon with the same `nodeId`, and it answers from the shards this reader holds only (it does not coordinate fan-out). One-shot commands on a reader serve the generations already on disk. A model that fails to load is logged at ERROR and does not hold readiness: the reader becomes ready, and vector/hybrid searches carry a `vector search unavailable` warning; a load still running after 5 minutes is reported the same way.
- Not yet on readers: the module tools (MCP `list_modules`, `get_module_deps`, `find_module_path`, `get_module_boundary` and the web UI's module views) answer "not available on reader nodes yet", and the cross-project graph is not replicated, so call paths that span projects are only found on the writer.

## How it works

1. **Parse** — JavaParser (with full symbol resolution), Python AST, Node.js AST, and regex-based parsers extract methods, classes, call references, field declarations, inheritance, annotations, and type references

2. **Knowledge graph** — call references plus structural relationships (inheritance, field access, annotations, type refs) stored as typed ArcadeDB edges alongside the call graph; enables `find_usages`, subclass hierarchy, and field impact analysis

3. **Embed** — optional semantic vectors via DJL + ONNX; documents split into logical chunks (method = 1 chunk, class = header + method-group chunks) and stored as `LateInteractionField` for multi-vector rescoring; falls back gracefully if unavailable

4. **Index** — Lucene stores all fields with per-field BM25; `KnnFloatVectorField` (mean of chunk vectors) for HNSW retrieval; `LateInteractionField` for late-interaction rescoring

5. **Search** — pipelines defined in `pipelines.yaml` compose retrievers (keyword / vector / unified), a merger (Borda / RRF / cross-encoder-merge), and rerankers (cross-encoder, doc-type diversity, MMR class diversity). The FST-backed `auto` router classifies query intent and dispatches to a child pipeline; `--pipeline` overrides it explicitly

Config and indexes are stored in `~/.pharos/`.
