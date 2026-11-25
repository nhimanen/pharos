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
no terminal (re-run with `--yes`); **3** (`--check`, which changes nothing) an index has a format this release
refuses (older than N-1, or newer; with an object-store backend: any shard published in the bucket); **5**
refused on a cluster reader node (upgrade and re-publish on the
writer) or on a work-queue writer (it upgrades by itself: restart it on the new release, see
"Work-queue writers"). `./setup.sh --upgrade-indexes` exits with the same code.

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

Vector search is **off by default** for everyone, new installs included: nothing is downloaded or embedded until you opt in. The recommended model is **granite-embedding-english-r2**. Turn it on with:

```bash
pharos embeddings enable                 # writes the granite-r2 provider to ~/.pharos/config.json
pharos embeddings enable --model jina-v2-code   # or the jina provider
pharos embeddings status                 # configured model, fingerprint, which projects have vectors
pharos embeddings disable                # back to keyword-only
```

`enable` only edits `config.json` and prints the cost first (about a 600 MB model download on first use, and a CPU-heavy first embed); it never embeds by itself. Afterwards restart the daemon (`pharos daemon restart`) and re-index each project with `pharos index --full <root path>` — `index` takes the project's directory, not its name; `pharos embeddings status` prints the exact command for every project without vectors (`pharos index --full --single --project <name> <root path>`). `pharos embed` is not the opt-in path: it only adds a model next to vectors an index already has, and fails on a keyword-only index. `enable` refuses to replace a different, already configured provider unless you pass `--force`; asking for the model that is already configured (same URL, ignoring an `@<sha>` pin) reports "already configured". Existing jina users keep jina; to switch: `pharos embeddings enable --model granite-r2 --force`, then `pharos index --full <root path>` per project (caches do not carry over between models). Legacy single-key configs (`embeddingModelUrl` at the top level) are migrated on load and not written back.

### Recommended: granite-embedding-english-r2 via DJL (local, no API key)

What `pharos embeddings enable` writes:

```json
{
  "embeddingProviders": [
    {
      "modelId": "granite-r2",
      "type": "djl",
      "url": "hf://onnx-community/granite-embedding-english-r2-ONNX@2a49b9c076aa627b14bc528b36b67462808ccc23",
      "dimensions": 768,
      "maxTokens": 512,
      "pooling": "cls"
    }
  ],
  "searchEmbeddingModel": "granite-r2"
}
```

On first index run, DJL downloads the ONNX model from HuggingFace Hub and caches it in `~/.djl.ai/pharos/` (the revision is part of the directory name). `hf://owner/name@<sha>` pins any HuggingFace model to a commit (7-40 hex chars); without `@<sha>` it follows `main`. In an A/B on 99 queries over the pharos and lucene projects, granite-r2 beat jina on the `auto` pipeline's main path (hybrid-rrf MRR@10 +0.056, 95% CI [+0.002, +0.112]) and on hybrid (+0.068, [+0.008, +0.129]); with the cross-encoder reranker it was a tie (-0.009). Details: `docs/superpowers/gates/2026-11-16-embedding-ab.md`. Trade-offs: peak RSS is higher (about 2.8 GB vs 2.3 GB) and CPU embedding is much slower (10.9 h vs about 30 min for the lucene project on one CPU, which is why a full re-embed is a deliberate step).

### Jina v2 Code via DJL (existing installs keep this)

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

768-dim, 8 192-token context window, mean pooling (the default).

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
      "modelId": "jina-v2-code",
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
    "modelId": "jina-v2-code",
    "type": "djl",
    "url": "hf://jinaai/jina-embeddings-v2-base-code",
    "dimensions": 768
  }
}
```

A `fallbacks` entry inherits the parent's `modelId`/`dimensions` when omitted; if given explicitly they must match the parent's — mismatched dimensions would corrupt the shared `vec.jina-v2-code` field, and a mismatched `modelId` would defeat the whole point. As with `searchEmbeddingProvider`, you're on the hook for verifying the fallback runtime produces vectors close enough to the primary's to be usable interchangeably in the same vector space.

`pharos index` and `pharos embed` each spawn a fresh JVM per invocation, so index-time fallback resolution re-probes the primary's reachability on every run — no daemon restart needed to pick up a remote server coming back online. The Lucene field and embedding cache key are always derived from the parent config, so vectors written via the fallback one day and the primary the next land in the same `vec.<modelId>` field/cache namespace — no forced re-embed when the runtime flips.

### Embedding cache & re-embedding

Embedding vectors are cached on disk at `<index-dir>/<project>/embed-cache.bin`, keyed by `SHA-256(embeddingText)`. Three commands manage it:

```bash
pharos backfill-embedding-cache myproject              # seed cache from existing HNSW vectors (no ONNX)
pharos backfill-embedding-cache --all                  # all projects

pharos embed --model=qwen3-emb myproject               # add a 2nd model's vectors next to existing ones
pharos embed --model=qwen3-emb --all                   # all projects
pharos embed --model=qwen3-emb myproject --force       # overwrite existing vectors

pharos invalidate-embeddings myproject                 # docs only (default scope)
pharos invalidate-embeddings myproject --scope java    # Java files only
pharos invalidate-embeddings myproject --scope all     # everything
pharos invalidate-embeddings --all                     # docs in every project
```

Use `embed` when adding a second model to an index that already has vectors, without re-parsing; it embeds single-chunk documents only (multi-chunk ones need `pharos index --full <root path>`). On a keyword-only index it fails: use `pharos index --full <root path>`. Use `invalidate-embeddings` after changing chunking logic to force selective re-embedding on the next incremental index run.

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
- **Token from the environment.** Instead of `cluster.token`, set `"tokenEnv": "PHAROS_CLUSTER_TOKEN"` to read the token from that environment variable (as Kubernetes passes a Secret), so it never sits in `config.json`. The two are mutually exclusive. Surrounding whitespace (such as the trailing newline of a Secret made with `--from-file`) is stripped; a named variable that is unset or empty, or whose value has whitespace or control characters inside it, is an error at startup.
- **Check a config file:** `pharos config validate <file>` (`./pharos config validate <file>`) reads the file as `config.json` and runs the validation a node runs at startup (`tokenEnv` is read from the current environment). It prints `ok` and exits 0, or prints the problem to stderr and exits 2. It reads only that file, so it works when the node's own config is broken, and its messages never echo a value from the file.
- `nodeId` defaults to the host name. `pollIntervalSeconds` must be positive, `retentionMinutes` non-negative, `retentionGenerations` at least 2, and `prefix` must not start with `/` or contain `.`/`..` segments.
- `objectStore.readTimeoutSeconds` (default 10, 1–3600) bounds the store's connect time and each attempt of a reader's ranged read; a whole ranged read, retries included, gets three times that (30 s). A query whose block can't be fetched in that time fails with an error naming the shard and object key, instead of stalling while the bucket is unreachable. That wait starts when the query asks for the block, so it includes time queued behind other fetches (`maxConcurrentFetches` at once): under a burst of cold misses a block can time out although the bucket answers each fetch quickly. Raise `maxConcurrentFetches` or `readTimeoutSeconds` if timeouts appear only under load. Other calls (whole-object downloads, the writer's uploads, catalog reads) wait up to `readTimeoutSeconds` or 30 s, whichever is longer, between reads of a response.
- `objectStore.callAttemptTimeoutSeconds` (default 20, 1–3600) and `objectStore.callTimeoutSeconds` (default 60, from `callAttemptTimeoutSeconds` to 3600) bound every other call per attempt and in total, retries included: heartbeats, leases, catalog, manifest and `LATEST` reads, listings, deletes, and a whole-object download until its response headers arrive (its body, which can be gigabytes, is bounded only by the 30 s idle-read timeout). Without them one call on a saturated bucket could take minutes (three 30 s socket timeouts). A healthy bucket answers these calls in well under a second; raise them only if `timed out` WARNs appear while the bucket is merely slow. An upload is one PutObject of a whole file (Lucene segments and embedding caches can be gigabytes), so it gets both bounds plus its size at `objectStore.uploadMinBytesPerSecond` (default 1048576, i.e. 1 MiB/s, at least 1024) per attempt, and three times that in total: a 5 GiB segment may take 20 s + 85 min per attempt. Lower the rate if uploads to a slow bucket fail with timeouts; a hung upload still ends.
- `retireGraceSeconds` (default 120) is how long a reader keeps a shard's directory after it stops serving it, so queries and shard calls already reading it finish. It must be at least `2 x pollIntervalSeconds` and at least the fan-out timeout (`fanoutTimeoutMillis`, default 5000; `connectTimeoutMillis`, default 1000, bounds the connect). Both rules are checked at startup and the message names the fields.
- Readers also serve `/internal/*` (term statistics, shard search, snippets, callees, graph lookups) to other readers, guarded by the shared `cluster.token` sent as `X-Pharos-Cluster-Token`; a reader without a token does not start, and a missing or wrong token gets 401. Single-node (`role: all`) has no `/internal/*` routes. A reader's web server accepts request bodies up to 4 MiB on every route, not only `/internal/*`.
- **Fan-out.** Any reader takes any query. A reader holding every shard a query needs searches locally, exactly as a single node. Otherwise it coordinates: it classifies and embeds the query once, sums BM25 term statistics over all the query's shards, and merges candidates in two rounds where a single index would pick a global pool first (vector first pass, unified recall: the global top candidates are chosen, then rescored on their shards), so rankings equal a single node's over the same shards; graph boost, fusion and reranking run once over the merged lists. Each query pins one reader and generation per shard (a dead reader is retried once on another replica of the same generation) and has one budget, `cluster.queryTimeoutMillis` (default 20000, or `fanoutTimeoutMillis` if larger; at least `fanoutTimeoutMillis` and below 60000), shared by all its rounds. A shard no reader answers for is left out with a warning, `shard <name> unavailable: <reason>; results exclude it` (when that leaves nothing, because no shard of the call answered and the reader holds none of them, the web API answers HTTP 503 `{"error": "no shard answered (…)", "warnings": […]}`, which `pharos router` retries once on another reader, and MCP a retryable tool error, instead of an empty result); a shard without vectors of the coordinator's search model is left out of the vector part with `project <name> has no vectors for model <id>; vector results exclude it`, as on a single node (a search scoped to that one project fails, also as on a single node: HTTP 409 on the web API, a tool error on MCP), so the answer does not depend on which reader serves it. Synonym expansions are scored with every expanded term's corpus-wide statistics, including terms a shard does not contain. A query ships the statistics of at most 5 000 terms (synonym expansions included); terms beyond that are scored with each shard's own statistics, with the warning `query expands to N terms; …`. A query naming a project (`project:query`, or the bare name) boosts that project's results on any reader, whether or not it holds the project. Coordinator and shards must share the same `synonyms.txt` (a reader with other synonyms refuses with 409 and is skipped). Readers fetch a published `synonyms.txt` on their next poll, so after a change, for up to one `pollIntervalSeconds`, some readers still use the old one. Each node keeps the synonyms a change replaced usable for `cluster.reader.previousGenerationGraceSeconds` (below): a reader answers a coordinator still on them, and a coordinator whose readers have not all synced the new ones analyses the query with the previous ones (the trace notes it). Only a reader more than one change behind, or behind for longer than the grace, is refused and left out with a `shard <name> unavailable: …` warning. The shard catalog is re-read in the background every 5 s (a timer, so an idle reader's catalog stays fresh; a query that still finds it stale waits up to 500 ms for the read in flight). Its age counts only while reads are failing: then, past 60 s old, an unscoped query carries `routing with a catalog N s old`, and past `cluster.maxCatalogAgeSeconds` (default 600), or before a first good read, unscoped queries and graph tools answer from the reader's own shards only, with a warning naming the catalog's age and the shards not searched (a reader holding every shard keeps answering completely, as before fan-out). Known approximations: ties between shards break by shard order, and vector and unified retrieval across several shards approximate a single index (each shard's HNSW search and unified candidate pool are its own).
- **Previous generations.** After a reader activates a shard's new generation it keeps the previous one open and answerable for `cluster.reader.previousGenerationGraceSeconds` (default max(30, 3 × 5 + 2 × `heartbeatSeconds`) seconds; 0 turns it off): a query planned against the previous generation (one already running, a coordinator whose catalog predates a reshard flip, a split project whose readers activate a run at different moments) is answered from it instead of losing the shard. Heartbeats list it as `retained`, the writer's GC keeps a generation for the grace after it was superseded, and a reader holds up to two generations of a changed shard (disk, open files, memory) meanwhile.
- **Trust model.** Write access to the bucket is cluster control: whoever can write `catalog/readers/*.json` can set a reader's `advertiseUrl` and so redirect the token-bearing shard calls of every coordinator to a host of their choosing (`advertiseUrl` must be `http://` or `https://`). Restrict bucket write access to cluster nodes. The token travels in clear text on `/internal/*` unless TLS fronts the readers (use `https://` advertise URLs); the endpoints cap request size (4 MiB) and collection sizes (10 000 ids/terms, 4096-dim vectors) and a reader without its shard service answers 503 so coordinators fail over.
- **Request distribution.** Each reader heartbeats into `catalog/readers/<node>.json` (every `heartbeatSeconds`, default 10; a reader whose heartbeat is older than `readerTtlSeconds`, default 30, is dropped) with the shards it holds and its `state` (`warming`, `ready`, `degraded` with `degradedShards`, or `draining`); `GET /cluster/readers` on a reader lists them (with `advertiseUrl`, the internal topology, so it requires `X-Pharos-Cluster-Token` like `/internal/*`: 401 without it). Put readers behind any load balancer that routes on `/health` (200 ok or degraded; 503 warming or draining), or run `pharos router --port 7080` (`./pharos router`): a stateless router that lists the live readers from the bucket, polls each one's `/health` every 2 s and round-robins `/api/*` and `/health` over those that answered 200. It retries an idempotent request (any `GET`, and `POST /api/search`) once on another reader after a connect error or a 5xx, answers 503 when no reader is healthy, and never forwards `/internal/*` (404; nor any path outside `/api/*` and `/health`, or with percent-encoding, dot segments or `;`). It adds `X-Forwarded-For`, drops hop-by-hop headers (and those a `Connection` header names), answers 400 for a request URI it cannot forward, 502 for a reader answer over 64 MiB, and 504 when a forwarded exchange (response headers and body read together) takes longer than 120 s; such a request is not resent. It needs the cluster config with `"backend": "object-store"`, the `objectStore` section and `cluster.token`, but no index, and never sends the token on.
- **Draining.** On SIGTERM a serving reader drains before it stops: it rewrites its heartbeat with `draining` (coordinators stop sending it shard calls) and answers `/health` with HTTP 503 `{"status": "draining"}` (the router drops it within one 2 s poll), keeps serving (requests still arriving are answered), and stops once it has been draining for at least 4 s (a router poll plus its probe) and one `heartbeatSeconds` plus 2 s (a coordinator's tick plus its bucket latency), and no request has been in flight for the last second; at most after `cluster.drainSeconds` (default 20, 0–3600), the draining announce included. It then removes its heartbeat. On a slow or hung bucket, or in peer mode a slow or frozen writer, the draining heartbeat and the delete (peer mode: the withdraw) each give up after 5 s with a `Stopping: gave up …` WARN (the heartbeat then expires after `readerTtlSeconds`), so the drain stays within its bound. The drain runs from the signal handler before any JVM shutdown hook, so ONNX Runtime's own hook cannot free the model under a query still being served; every long-running pharos process that embeds (`web`, `mcp-server`, `index`, `embed`, `upgrade-indexes`) stops in that order (drain, then running model calls, then exit 143/130), and a second signal exits at once. Give the process manager a stop timeout of at least `drainSeconds` + 15 s (the longest wait for running model calls) plus a margin for the exit itself: with the default `drainSeconds` of 20, Kubernetes `terminationGracePeriodSeconds` must be above 35, e.g. 45. A shorter timeout sends SIGKILL in the middle of the drain or the model wait.
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
- On a hand-run writer (`"writer": {"workQueue": false}`): if publishing fails at the end of an index run (store unreachable, bad credentials), the local index is kept and `pharos index` exits with code **3**. Retry the upload without re-indexing (a work-queue writer retries by itself on its next poll):

  ```bash
  pharos publish myproject      # writer only; prints "published myproject generation N"
  ```

  `pharos publish` refuses (exit code **4**) an index it would not serve at the current format: a broken `callgraph.current` pointer, or an index format other than this build's (including an `upgrade-available` one). Re-index with `pharos index --full` first.
- Every manifest carries the shard's index format stamp (`index-format.json`); a manifest published before the stamp shipped counts as format 0. A reader only materialises generations whose format it can serve (its own, or N-1). Otherwise it keeps serving the generation it has and logs a WARN naming the shard, both formats and the fix (upgrade the reader, or re-index and re-publish on the writer: a work-queue writer does that by itself once it runs the new release). Served generation dirs carry the stamp, so `pharos index-status` works on readers.

- Readers refuse `index`, `link`, `embed`, `mine`, `remove-index`, `backfill-embedding-cache` and `upgrade-indexes` (also `./setup.sh --upgrade-indexes`): readers receive upgraded indexes by syncing, so upgrade and re-publish on the writer (a work-queue writer re-indexes and re-publishes every shard by itself once restarted on the new release). `index-status --json` reports the node's `role`. They start syncing when the daemon (`pharos web` / `mcp-server`) starts, and serve class bodies and snippets from the index, never from source files on the reader machine.
- `pharos web` on a reader binds its port at once and loads its search models (embedder, cross-encoder) in parallel with the first sync pass. Until both are done, `/health` answers HTTP 503 `{"status": "warming", "ready": false}` (a served lazy generation that lost objects on the bucket answers HTTP 200 `{"status": "degraded", "degradedShards": […]}` instead, see above; a draining reader answers HTTP 503 `{"status": "draining"}`) (load balancers and the router hold traffic on it; the log line `Reader … serving N shard(s)` appears only then), and searches answered meanwhile carry the notice in the `X-Pharos-Warnings` header (the same JSON array that lists skipped incompatible projects; `./pharos` prints it on stderr). Only one process per `cacheDir` syncs (it holds `<cacheDir>/.sync.lock`); another daemon on the same `cacheDir` (e.g. `mcp-server` next to `web`) serves the generations the lock holder downloads, and takes over syncing when that process exits. Only `pharos web` joins reader membership (heartbeat, `/internal/*`, the shutdown drain): `pharos mcp-server` on a reader never advertises itself, so it cannot overwrite, drain or delete the heartbeat of a `web` daemon with the same `nodeId`, and it answers from the shards this reader holds only (it does not coordinate fan-out). One-shot commands on a reader serve the generations already on disk. A model that fails to load is logged at ERROR and does not hold readiness: the reader becomes ready, and vector/hybrid searches carry a `vector search unavailable` warning; a load still running after 5 minutes is reported the same way.
- Not yet on readers: the module tools (MCP `list_modules`, `get_module_deps`, `find_module_path`, `get_module_boundary` and the web UI's module views) answer "not available on reader nodes yet", and the cross-project graph is not replicated, so call paths that span projects are only found on the writer.

### Without a bucket: peer-pull (experimental)

With `"backend": "peer-pull"` there is no bucket: each writer keeps its published generations in a local **peer store**, and its `pharos web` serves them on `/replication/*` (every route requires `cluster.token`). Readers find the writers through `cluster.peers.discovery`, pull manifests and files from them, and fetch whole files from other readers that already hold them.

#### Setup

Writer (one per group of shards; repeat with another `nodeId` and other projects for more writers):

```json
{"indexDir": "/data/pharos/indexes",
 "cluster": {
  "role": "writer",
  "backend": "peer-pull",
  "nodeId": "writer-1",
  "token": "<shared secret>",
  "advertiseUrl": "http://writer-1.internal:7171",
  "peers": {
    "storeDir": "/data/pharos/peer-store",
    "discovery": "seeds:http://writer-1.internal:7171,http://writer-2.internal:7171",
    "allowHosts": ["writer-1.internal", "writer-2.internal", "reader-1.internal", "reader-2.internal"]
  }}}
```

Reader:

```json
{"cluster": {
  "role": "reader",
  "backend": "peer-pull",
  "nodeId": "reader-1",
  "token": "<shared secret>",
  "advertiseUrl": "http://reader-1.internal:7171",
  "cacheDir": "/var/cache/pharos",
  "peers": {
    "discovery": "seeds:http://writer-1.internal:7171,http://writer-2.internal:7171",
    "allowHosts": ["writer-1.internal", "writer-2.internal", "reader-1.internal", "reader-2.internal"]
  }}}
```

- **Writers must run `pharos web`** (`java -jar pharos.jar web --port 7171`, long-lived, role writer). It is what serves `/replication/*`, reader membership and the status state. Index and publish next to it as on a hand-run writer: `pharos index <dir>` publishes at the end of the run, and `pharos publish <project>` retries a failed publish; the running `pharos web` hands each new generation to waiting readers within about a second.
- **No takeover.** Peer mode has no leases: each writer owns the shards in its own registry, and `cluster.writer.workQueue` is forced off (setting it to `true` is a config error). When a writer is down, its shards keep serving the generations readers hold, but nothing publishes them until it is back. A Phase 4 work-queue cluster moved to peer mode loses writer failover.
- **`cluster.peers.storeDir`** (default `~/.pharos/peer-store`) holds the published generations: the last `retentionGenerations` (3) of each shard plus those younger than `retentionMinutes` (60). Lucene files are hard-linked from the index dir, so **put `storeDir` on the same file system as `indexDir`**; otherwise every file is copied, disk use doubles, and the writer logs a WARN at startup. The writer's disk guard (`cluster.writer.disk`, `/health` `writerDisk`) watches the store too.
- **Readers** need `discovery`, `token` and an `advertiseUrl` that coordinators and other readers can reach. `pharos router` reads reader membership from the writers too: run it with a reader's config.
- **Discovery.**
  - `dns:<name>[:port]`: every A/AAAA record of the name is a writer, re-resolved on every refresh (`refreshSeconds`, default 15). A Kubernetes headless service is the usual example. Give the fully qualified name (for example `pharos-writers.<namespace>.svc.cluster.local`): a name without a dot is tried in the `/etc/resolv.conf` search domains first, and a name no DNS query answers falls back to the JVM resolver, which caches answers (30 s by default). Readers connect to `http(s)://<address>:<port>`, so certificates must carry IP SANs (see TLS).
  - `seeds:<url>,<url>`: the seeds, plus the writers they list at `/replication/peers` (gossip, followed transitively). Every writer a reader polls receives the cluster token, so gossip is limited: with `cluster.tls.truststore` every seed must be `https://` and only `https://` gossip entries are followed; without TLS a gossiped URL is polled only if its host is in `cluster.peers.allowHosts` (default: the seeds' hosts). Other hosts are ignored, with a WARN once per host.
- **`cluster.peers.allowHosts`** (host names or IPs, matched as written, ignoring case) is used three ways: (1) the gossip rule above; (2) the hosts of peer readers a reader may fetch from without TLS (besides the discovered writers' hosts); (3) on writers, the reader hosts whose heartbeats may arrive from another address (NAT, proxies; see the trust model). **Peer fetch is therefore off unless reader hosts are listed**: with `dns:` discovery the default list is empty, so reader pods are never used as peers unless every reader host is in `allowHosts`, which is impractical with pod IPs. Every file then comes from the writer, which is correct, only slower for the writer.
- **TLS** (`cluster.tls`, peer-pull only):
  - On a writer, `keystore` (with `keystoreType`, `PKCS12` (default) or `JKS`, and `passwordEnv`, the name of the environment variable holding the password; the password never goes into `config.json`) makes `pharos web` listen with TLS only on its port. It also listens with plain HTTP on `127.0.0.1:<localPort>` (`cluster.tls.localPort`, default web port + 1), loopback only, serving everything except `/replication/*` (404): `./pharos` on the writer uses that port by itself. Keep that port free; scripts on the writer host can use it too.
  - Readers may set only `truststore` (with `passwordEnv`): their calls to writers then use `https://`, and every seed must be `https://`. A reader keystore is a config error: readers, coordinators and `pharos router` speak plain HTTP to readers. A writer that sets `discovery` (it polls the other writers for `/cluster/status`) needs the `truststore` and `https://` seeds too, and so does `pharos router`'s config.
  - Certificates: with `dns:` discovery readers connect to the writers' IP addresses, so each writer certificate needs **IP subject alternative names** for its addresses (pod IPs); otherwise use `seeds:` with fully qualified host names that the certificates carry.
  - With a truststore, readers fetch from peers only over `https://`, and readers serve plain HTTP, so **peer fetch is off in a TLS cluster**: every file comes from the writers. Do not put a TLS-terminating proxy in front of readers to get it back: the reader would then have to advertise an `https://` URL, and coordinators and `pharos router` have no truststore, so their shard calls and `/health` polls to it would fail.
- **Long-polls and `cluster.peers.maxLongPolls`.** Readers long-poll each shard's `latest` at the writer that owns it (`longPollSeconds`, default 30), so a writer holds one waiting request per reader per shard it owns: size `maxLongPolls` (default 4096) to at least **readers × shards per writer** (8 readers × 40 shards = 320). A waiting long-poll holds no thread, but holds a connection, so the writer's open-files limit (`ulimit -n`) must be above `maxLongPolls` plus transfers. Past the cap, a long-poll gets 503 with `Retry-After` and the reader retries every 2–2.5 s, so the writer answers a steady stream of refused requests, and shards that never get a slot learn of new publishes only at the next refresh (`refreshSeconds`, 15 s). `/health` on the writer shows `peers.transfers.longPolls` (waiting now); raise the cap when it sits at the cap.

#### How it runs

**Peer readers.** A `pharos web` reader with a `cluster.advertiseUrl` also serves the files of the generations it holds in full to other readers (`/replication/<shard>/files/<key>`, token required, at most `cluster.peers.maxTransfersPerReader` (default 4) at once; more get 503 with `Retry-After`). Its heartbeat lists those generations (`generationsHeld`), and a writer's `latest` answer names up to 4 readers holding that generation. A reader then tries at most 2 of them before the writer. Peers serve **whole files only** (a `Range` request gets 416), and every file a peer sends is checked against the manifest (exact length and checksum). Ranges, that is a lazy reader's cold blocks and file heads, always come from the writer, so while the writer is down a lazy reader's cold reads fail (its cached data keeps serving). On any mismatch or failure the peer is skipped for 2 minutes and the writer serves the file. The token goes only to a trusted peer (see `allowHosts` and TLS above). `/health` reports the counters under `peers.peerFetch` (files and bytes from peers and from the writer).

Every publish reaches a peer-mode writer from another process (`pharos index` or `pharos publish` next to `pharos web`); the writer re-reads `LATEST` of the shards readers wait on every 0.5 s, so a new generation reaches waiting readers within about a second. `/health` on a peer-mode writer reports its transfer counters under `peers.transfers` (`maxTransfers`, `transfersInFlight`, `peakTransfersInFlight`, `transfersServed`, `transfersRefused`, `bytesServed`, `longPolls`).

A writer missing for 3 refreshes is dropped, and the generations readers hold of its shards keep serving. When two writers claim one shard, the reader syncs from the one with the higher `LATEST`; on a tie, the lowest node id wins. A writer that misses a refresh keeps its claim until it is dropped. `/health` reports each such conflict under `peers.conflicts` as `shard X claimed by A and B`.

**`/health` and addresses.** Like `/cluster/status`, a peer-mode reader's `/health` has a public view and a token view. Without `X-Pharos-Cluster-Token` (and through `pharos router`, which never forwards the token) `peers` names nodes only: no `discovery` spec, no writer `url` or `error`, no peer reader URLs (`peerFetch.suspectCount` instead of `peerFetch.suspects`), and `unavailable` reasons by node id. With the token (compared in constant time) it shows all of them. The status code and readiness are the same in both views.

#### Trust model

- **The cluster token is the membership credential.** There are no bucket credentials in peer mode: any client holding `cluster.token` can read every published file and join as a reader.
- **A heartbeat alone never makes a host trusted.** A writer admits a reader's heartbeat only when the host of its `advertiseUrl` resolves to the address the heartbeat came from (a name is resolved and the IPs compared; one that does not resolve is refused), or is in the writer's `cluster.peers.allowHosts` (a reader behind NAT or a proxy). Anything else gets 403, the writer logs one WARN per host, and the reader logs one WARN per writer. So a token holder cannot redirect coordinators' shard calls (which carry the token and users' queries) or the router's traffic to somebody else's address, though it can still join from its own.
- **The token is visible on the network.** Coordinator-to-reader traffic (`/internal/*`) and peer fetches are always plain HTTP, and reader-to-writer traffic is too without `cluster.tls`; all of them carry the token. `cluster.tls` protects only reader-to-writer pulls. Run a peer-mode cluster on a network only its nodes can reach: a Kubernetes `NetworkPolicy` that admits only the pharos pods (and the router) to the cluster ports, or a private network or VPC.

### Work-queue writers

A `role: writer` node runs the **work queue** by default (`cluster.writer.workQueue`, default `true` since the Phase 4 gate): `pharos web` on the writer takes shards from the catalog, polls each shard's source, indexes its changes and publishes them, and any number of writers share the shards. Add `"writer": {"workQueue": false}` to the writer example above to keep the hand-run writer (`pharos index` / `pharos publish` under an implicit lease); a work-queue writer refuses hand runs (`index`, `publish`, `remove-index`, `embed`: exit 2; `upgrade-indexes`: exit 5).

- **Upgrades.** On a work-queue cluster, upgrading is restarting the writers on the new release: a held shard is pending, and gets a full run, when its local index needs one (another index format, a broken graph pointer), its files were embedded under another `CHUNKING_VERSION` or model, or its published `LATEST` generation has an older index format. So every shard is re-indexed and re-published without a source change (embeddings from its cache). Don't run `upgrade-indexes` there (it is refused).
- **One work loop per node.** `pharos web` takes `<workDir>/.work-loop.lock` before it starts the loop. A second pharos daemon with the same config on the same host (for example one `./pharos` starts on demand) logs a WARN, `another process is the writer for node <id> …`, runs no loop and serves everything else.

- **Sources.** A shard is workable once its catalog entry names a source: index it once on a hand-run writer with `pharos index <dir> --single --project <name> --source-git <url> --ref <branch>` (or `--source-path <dir>`, a directory every writer can read; `pharos publish <project>` takes the same flags), or only register the source, with no path: `pharos index --project <name> --source-git <url> --ref <branch>`. That indexes nothing and publishes nothing, so it runs on any writer config, a work-queue writer's included, with or without writers running; running it again with another url or ref replaces the source (on every shard of a split project, too). `--no-embed` is recorded with the source (`"embed": false`), and work-queue writers then index that shard without embeddings too. A shard without a source stays with the writer that has its path (pinned). `pharos remove-index --from-catalog <name>` removes a project from the catalog in one write (a split project's every shard, its staged and retired versions and an open reshard with it): writers release it and readers stop serving it; its generations and writer state stay in the bucket, and registering it again resumes from them.
- **Git credentials.** Writers run git with their own environment (credential helper, ssh agent, `GIT_ASKPASS`). For credentials per project, `cluster.writer.gitCredentials` maps a project to a directory (`{"mypal": "/etc/pharos/git/mypal"}`) holding the keys of a Kubernetes `ssh-auth` or `basic-auth` Secret: `ssh-privatekey` (and optionally `known_hosts`; without it a host key is accepted on first contact and pinned in `<workDir>/.ssh-known-hosts`), or `username` and `password` (a token works as the password). Only that project's clone, fetch and `ls-remote` use them; git reads the files when it needs them, and the config holds paths only. A URL must not carry credentials.
- **Change detection.** Every `pollSeconds` (default 60, at a fixed rate, also while another shard is being indexed) a writer checks the sources of the shards it holds, read-only (`git ls-remote` of the source URL, or a digest of a path source's tree; `ls-remote` is killed after `lsRemoteTimeoutSeconds`, default 30, and an unreachable remote stalls only its own shard), and writes its heartbeat as soon as one has an unpublished change, so the backlog shows it within one poll. It indexes pending shards one at a time, oldest change first; a change pushed during a shard's own run stays pending after that run publishes.
- **Leases.** One writer holds a shard's lease (`shards/<shard>/lease`, renewed every `leaseTtlSeconds` / 3, default TTL 30 s, expiry judged by the bucket's clock). When a writer dies its leases expire after the TTL, and another writer takes them on its next poll, restores the shard's writer state (embedding cache, file state) from the bucket and resumes from the last published generation, so unchanged code is not re-embedded. Writers share the shards (`ceil(shards / writers)` each) and release a surplus shard only after its publish or while it has nothing unpublished, never within `minHoldSeconds` (120) of taking it. A writer's checkout lives in `workDir/<shard>` (default `~/.pharos/work`): give every writer the same `workDir` path (as containers do), otherwise each takeover re-parses the shard in full (embeddings still come from the restored cache).
- **Stopping.** On SIGTERM a writer starts no new run, lets the run in progress finish for up to three quarters of `cluster.drainSeconds`, then aborts it at its next safe point (also between the files of a parse; a run past its commit publishes), releases its leases (another writer takes them at once) and removes its heartbeat. It is done within `drainSeconds` + 10 s even when the bucket hangs: each of those bucket calls has a deadline, and one that passes it is logged (`Stopping: gave up …`) and left to expire after the TTL. With the model-call wait (up to 15 s) a writer needs a stop timeout above `drainSeconds` + 25 s plus the exit, e.g. 50 s for the default 20. A writer whose lease was taken by another writer (or is gone) aborts that shard's run at once, since its publish could only fail; a renewal that merely failed (an I/O error or a timeout on a slow bucket) lets the run commit, its publish re-checks the lease, and the writer reclaims its untouched lease and publishes on its next poll.
- **Disk guard** (`cluster.writer.disk`): a writer whose free space is below `max(minFreeBytes, minFreePercent % of the disk)` (defaults 5 GiB and 0 %, so 5 GiB; raise `minFreeBytes`, or set a percentage, to keep more headroom) takes no new shard, refuses or aborts (at a safe point, never half committed) a run that would cross it, hands its pending shards to other writers and reports `low` in its heartbeat (`/cluster/status` health `writer-low-disk`) and in a WARN, once per low episode; it is back once free space exceeds the threshold plus `hysteresisBytes` (1 GiB).
- **Synonyms.** Each publish merges the writer's mined rules for that shard into the cluster's `catalog/synonyms.txt` (a conditional read-modify-write that replaces only that shard's section and keeps every other shard's), so all readers serve the same file whichever writer published last. Hand-written rules come from the publishing writer's `synonyms.txt`: keep them the same on every writer.
- A writer serves no search: `pharos web` on a writer does not load the search models (its index runs load the embedding models they use). Writers heartbeat into `catalog/writers/<node>.json` every `heartbeatSeconds` (which must stay below `leaseTtlSeconds`) with their leases, backlog and current job; `/cluster/status` and the Cluster page show them.

### Resharding a project

A project is split into shards by its **shard spec** (`cluster.sharding`; object-store clusters only): `[project]` (the default, one shard), `[project, module]`, `[project, ring(path, shards=8)]`, `[language]` and so on. Changing it live is one command:

```bash
pharos reshard lucene --spec "[project, ring(path, shards=4)]"   # request (a new spec version)
pharos reshard lucene --status                                   # versions and progress
pharos reshard lucene --cancel                                   # abandon an open reshard
```

The writer holding the project runs it under a per-project lease (`projects/<p>/reshard-lease`): it writes the new version as *staged*, builds it from its local index (a ring change re-indexes only the files whose shard changes, counted at the parser; embeddings come from the cache, so nothing unchanged is embedded again) and publishes the new version's shards without moving their `LATEST`, all at one new index run; a shard that neither gains nor loses files gets a metadata-only manifest at that run listing its unchanged Lucene files (nothing uploaded, nothing downloaded again). A shard that gains or loses files is rewritten whole: the moved files' documents are expunged before the commit, so its segments (HNSW vector graphs included) are new, uploaded in full and downloaded in full by every reader serving it; the writer needs about twice the project's Lucene size free on disk while it builds (the disk guard checks this before it starts). Readers keep serving the current version and prepare the new one; once every live reader has it (or after `cluster.writer.reshardSyncTimeoutSeconds`, default 600), one catalog CAS flips routing. Each query reads one version: right after the flip it reads the previous one until its readers serve the new one, so a file that moved shards is never seen twice or missed. The retired version is deleted after `retentionMinutes`. An interrupted reshard resumes on the next writer that holds the project; a cancelled one is removed after the retention window. Index runs never add shards: a new module under `[project, module]` fails the project's runs with "reshard the project to add it" until `pharos reshard <project> --spec "<the same spec>"` adds its shard. Resharding back to `[project]` is not supported; `[project, ring(path, shards=1)]` serves a project from one shard. Finish a rolling upgrade first: while a reader of an older release is live, `pharos reshard` is refused and an open reshard holds (UPGRADING.md).

A reshard shows in `/cluster/status` (`projects[].reshard`: phase `requested` → `building` → `pinned` → `flipped`, or `abandoned`; shards and moved files done of total; the writer holding the reshard lease; `specs[]` lists every spec version as active, staged, retired…), as `reshard` events on `/cluster/events` (at each phase and each quarter of the shards or files) and on the Cluster page. When a shard grows past half of a reader's cache, the `reshard` proposal names the command to run, e.g. `pharos reshard lucene --spec "[project, ring(path, shards=4)]"` (a ring of `ceil(largest shard size / that threshold)` shards, at least 2, since the ring splits each shard; a ringed project of N shards gets at least N + 1). Applying it stays manual.

### Scaling proposals

`GET /cluster/status` (and `/cluster/events`, and the optional `cluster.status.webhookUrl`) lists the scaling proposals in force. Pharos applies none of them; an operator or a person does. Each scaling proposal carries a signed `delta`, the node count its `action` names: `+N` to start N readers or writers, `-N` to stop N. `reshard` and `writer-disk` proposals carry no `delta`. The rules and their keys under `cluster.status`, with defaults:

| Proposal | Raised when | Keys |
|---|---|---|
| `add-readers` | the highest warm p95 or the QPS per reader is above target for the window | `targets.warmP95Millis` (500), `targets.qpsPerReader` (50), `windows.addReadersSeconds` (300) |
| `remove-readers` | more than one reader, QPS per reader below `removeReadersLoad` of the target, warm p95 within target | `removeReadersLoad` (0.2, above 0 and below 1), `windows.removeReadersSeconds` (1800) |
| `add-writers` | at least `writers.backlogShards` shards have an unpublished change older than `writers.oldestPendingSeconds`, and there are fewer live writers than workable shards | `writers.backlogShards` (1), `writers.oldestPendingSeconds` (300), `windows.addWritersSeconds` (0) |
| `remove-writers` | more than one live writer, none indexing and nothing pending, for `writers.idleSeconds` | `writers.idleSeconds` (unset: `windows.removeWritersSeconds`, 1800; both set to different values is an error) |
| `reshard` | a shard is above half of the smallest reader cache cap | `windows.reshardSeconds` (0) |
| `writer-disk` | a writer reports low disk | `windows.writerDiskSeconds` (0) |

`targets.backlog` (600) sets when a pending shard is reported as `shard-lagging` in `health`. A proposal clears after its condition has been false for `windows.clearGraceSeconds` (60).

```json
"status": {"removeReadersLoad": 0.2,
           "writers": {"backlogShards": 1, "oldestPendingSeconds": 300, "idleSeconds": 1800}}
```

### Cluster page

The web UI (`pharos web`, `./pharos web`) has a **Cluster** button next to *Module Graph*; `http://<node>:7171/#cluster` opens it directly. It is drawn only from `GET /cluster/status` (the public view, without reader addresses) on the node you opened, so any node shows the same cluster:

- **Layout:** writers, the bucket and readers, with lines for publish and sync. Each writer lists the shards whose lease it holds (a lease holder without a heartbeat is drawn dashed), each reader the shards it serves. Shard chips are coloured by state (current, lagging, lease-expired, unpublished, staged); a yellow dot marks a shard with an unpublished source change, a dashed chip a shard degraded on that reader, a blue dashed outline a shard of a reshard's staged version not served yet (new, or rewritten), and a faded chip a retired version's shard. Writer boxes show their current job and free disk, and low-disk writers are outlined. Large clusters scroll inside the panel, and above 12 boxes per column, nodes with the same shards and state are merged into one box.
- **Indexing backlog:** shards with unpublished changes, oldest first, with source against published revision and how long the change has waited; each writer's job with its phase and a progress bar.
- **Scale:** live writer and reader counts and the proposals in force, with reason, action and metrics.
- **Resharding:** each project's reshard, from version to version, its phase, progress bars for shards and files, the reshard lease holder and what the flip waits for.
- **Health**, **Nodes** (role, shards, docs, size, free disk, reader cache of cap, hit rate, QPS, warm p95, heartbeat age) and **Sharding** (the spec versions and their state, split projects with their staged and retired versions, and one shard table per spec version — active, staged, retired — with project, owner, generation and readers).

The page follows `/cluster/events` and refetches the status on every event; when the stream is unavailable (a refused stream, more than 64 clients on the node, a proxy that drops it) it polls every 10 s and says so. On a single node (`role: all`) it shows one node holding every project.

## Container image

`docker/pharos/Dockerfile` builds the image the Kubernetes operator deploys (Temurin 25 JRE, the fat jar, python3 with `python-hcl2`, node, git; runs as uid 10001). Build from the repository root: `docker build -f docker/pharos/Dockerfile -t pharos:dev .`. Release tags publish `ghcr.io/<owner>/pharos:<version>`.

Entrypoint contract:

| Setting | Meaning |
|---|---|
| `PHAROS_ROLE` | `writer`, `reader` or `all` run `pharos web` (port 7070); `router` runs `pharos router` (port 7080). The node's cluster role comes from `cluster.role` in the config. |
| `/etc/pharos/config.json` | The config, mounted from a ConfigMap and linked to `$PHAROS_HOME/config.json`. Override the path with `PHAROS_CONFIG`. |
| `PHAROS_HOME` | State and model cache, `/data/pharos` (put a PVC on `/data`). |
| `PHAROS_PORT`, `JAVA_OPTS` | Optional port and JVM options. |
| arguments | One pharos command to run instead of the server, e.g. `index --project lucene --source-git <url> --ref main` (the operator's registration Job); the container exits with its code. |

Pods must set `securityContext` `runAsUser: 10001` and `fsGroup: 10001` when a PVC is mounted on `/data` (the operator sets these). With `PHAROS_ROLE` writer, reader or router a missing config makes the container exit 78, and an invalid one fails `pharos config validate` before serving.

Secrets reach the process only as env vars named by the config (`cluster.tokenEnv`, the object-store key envs); the entrypoint never writes them. It ends in `exec java`, so SIGTERM reaches the JVM. There is no `HEALTHCHECK`; use Kubernetes probes on `/health`. `scripts/image-smoke.sh` builds the image, runs one node and searches once; it needs a Docker daemon.

### Kubernetes operator: MinIO images and keeping the bucket

With `storage.minio`, the operator deploys MinIO and creates the bucket with `mc`. MinIO no longer publishes images: `quay.io/minio/*` and `docker.io/minio/*` cannot be pulled anymore. The defaults are therefore a community build of the upstream MinIO sources (AGPL), pinned to release tags: `docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z` and `docker.io/pgsty/mc:RELEASE.2026-11-12T00-00-00Z`. To use your own build or a mirror, set the env vars on the operator Deployment (`deploy/operator.yaml`); there is no field in the `PharosCluster` spec for it:

```yaml
env:
  - { name: MINIO_IMAGE, value: "registry.example.com/mirror/minio:RELEASE.2026-08-04T00-00-00Z" }
  - { name: MC_IMAGE, value: "registry.example.com/mirror/mc:RELEASE.2026-11-12T00-00-00Z" }
```

A changed image applies to the MinIO StatefulSet at the next reconcile. The bucket Job picks it up the next time it is created. Deleting a `PharosCluster` keeps its data: the MinIO PVC and its credentials Secret have no owner reference, so a cluster applied again under the same name adopts them. Writer and reader volume claims also outlive their StatefulSets. A full teardown deletes them by label: `kubectl delete pvc,secret -l pharos.dev/cluster=<name>`.

## How it works

1. **Parse** — JavaParser (with full symbol resolution), Python AST, Node.js AST, and regex-based parsers extract methods, classes, call references, field declarations, inheritance, annotations, and type references

2. **Knowledge graph** — call references plus structural relationships (inheritance, field access, annotations, type refs) stored as typed ArcadeDB edges alongside the call graph; enables `find_usages`, subclass hierarchy, and field impact analysis

3. **Embed** — optional semantic vectors via DJL + ONNX; documents split into logical chunks (method = 1 chunk, class = header + method-group chunks) and stored as `LateInteractionField` for multi-vector rescoring; falls back gracefully if unavailable

4. **Index** — Lucene stores all fields with per-field BM25; `KnnFloatVectorField` (mean of chunk vectors) for HNSW retrieval; `LateInteractionField` for late-interaction rescoring

5. **Search** — pipelines defined in `pipelines.yaml` compose retrievers (keyword / vector / unified), a merger (Borda / RRF / cross-encoder-merge), and rerankers (cross-encoder, doc-type diversity, MMR class diversity). The FST-backed `auto` router classifies query intent and dispatches to a child pipeline; `--pipeline` overrides it explicitly

Config and indexes are stored in `~/.pharos/`.
