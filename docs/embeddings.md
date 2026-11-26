# Embeddings

Vector search is **off by default** for everyone, new installs included: nothing is downloaded or
embedded until you opt in. Keyword search, the call graph and the knowledge graph work without
embeddings; only the `vector`, `hybrid` and `unified` search types need them.

## Turning it on

```bash
pharos embeddings enable                        # writes the granite-r2 provider to ~/.pharos/config.json
pharos embeddings enable --model jina-v2-code   # or the jina provider
pharos embeddings status                        # configured model, and which projects have its vectors
pharos embeddings disable                       # back to keyword-only (vectors already indexed stay)
```

`enable` only edits `config.json` and prints the cost first (about a 600 MB model download on first use,
and a CPU-heavy first embed); it never downloads or embeds by itself. Afterwards:

```bash
pharos daemon restart
pharos embeddings status               # prints the exact index command for every project without vectors
pharos index --full <root path>        # per project; `index` takes the project's directory, not its name
```

The printed command has the form `pharos index --full --single --project <name> <root path>`.

- `enable` refuses to replace a different, already configured provider unless you pass `--force`.
  Asking for the model that is already configured (same URL, ignoring an `@<sha>` pin) reports
  "already configured".
- Existing jina installs keep jina. To switch: `pharos embeddings enable --model granite-r2 --force`,
  then `pharos index --full <root path>` per project. Embedding caches are per model and do not carry
  over, so the first embed covers everything.
- `pharos embed` is not the opt-in path: it only adds a second model next to vectors an index already
  has, and fails on a keyword-only index.
- Legacy single-key configs (`embeddingModelUrl` at the top level) are migrated on load and not written
  back.
- `pharos embeddings disable` and `status` still run when `config.json` fails validation; every other
  command prints the error and how to fix the file.

## Choosing a model

**granite-embedding-english-r2** (`granite-r2`) is the recommended model and the `enable` default. In an
A/B over 99 queries on two Java code bases it beat jina on the `auto` pipeline's main path
(hybrid-rrf MRR@10 +0.056, 95 % CI [+0.002, +0.112]) and on `hybrid` (+0.068, [+0.008, +0.129]); with
the cross-encoder reranker the two tied (−0.009). The trade-offs: peak memory is higher (about 2.8 GB
against 2.3 GB), and CPU embedding is much slower (10.9 h against about 30 min for the Lucene source
tree on one CPU), which is why a full re-embed is a deliberate step.

### granite-r2 (what `pharos embeddings enable` writes)

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

On first use DJL downloads the ONNX model from Hugging Face and caches it in `~/.djl.ai/pharos/` (the
revision is part of the directory name). `hf://owner/name@<sha>` pins any Hugging Face model to a commit
(7–40 hex characters); without `@<sha>` it follows `main`. Repositories with external weights
(`model.onnx_data`) are supported.

### jina-v2-code

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

768 dimensions, mean pooling (the default).

### MiniLM via the DJL model zoo

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

A lighter model (384 dimensions), good for general text, lower memory use.

### An OpenAI-compatible HTTP server

For a local embedding server (llama.cpp, Ollama, text-embeddings-inference, …), use `type: openai`:

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

### Provider options

| Key | Default | Meaning |
|---|---|---|
| `modelId` | — | the id the vector field is named after (`vec.<modelId>`) |
| `type` | — | `djl` (local ONNX) or `openai` (HTTP) |
| `url` | — | `hf://owner/name[@sha]`, a DJL model-zoo id, or the server's base URL |
| `dimensions` | — | vector length, at most 1024; sent as `dimensions` to `openai` servers |
| `maxTokens` | 512 | `djl`: tokenizer truncation |
| `pooling` | `mean` | `mean` or `cls` |
| `queryPrefix`, `documentPrefix` | `""` | prepended to query and document texts |
| `model` | — | `openai`: the model name sent in the request |
| `apiKeyEnv` | — | `openai`: the environment variable holding the API key (unset: no `Authorization` header) |
| `batchSize` | 0 | texts per call; 0 means 8 (`djl`) or 64 (`openai`) |
| `timeoutMillis` | 60000 | `openai`: per request |
| `embeddingThreads` | 0 | concurrent batches; 0 means 1 (`djl`) or 4 (`openai`) |
| `fallbacks` | `[]` | see [Fallback runtimes](#fallback-runtimes) |

## Several providers side by side

`embeddingProviders` is a list. Pharos can index against several models and store one vector field per
model (`vec.<modelId>`) on each document. `searchEmbeddingModel` selects the one queried; the others stay
on disk for A/B comparison. Add a second model's vectors to an existing index with `pharos embed`:

```bash
pharos embed --model=qwen3-emb myproject           # add a second model's vectors, no re-parse
pharos embed --model=qwen3-emb --all               # every project
pharos embed --model=qwen3-emb myproject --force   # overwrite existing vectors
```

`embed` handles single-chunk documents only; multi-chunk ones need `pharos index --full <root path>`.

A vector, hybrid or unified search over several projects leaves a project without vectors of the search
model out of its vector part, with the warning
`project <name> has no vectors for model <id>; vector results exclude it` (its keyword part still runs).
A search scoped to that one project fails with a message naming the `pharos embed` command to run
(`./pharos search` exits 6).

## Split index and search runtimes

When the runtime at index time differs from the one at query time (embed at index time on a fast remote
`openai` server, embed queries locally with DJL), add `searchEmbeddingProvider`:

```json
{
  "embeddingProviders": [
    { "modelId": "qwen3-emb", "type": "openai", "url": "http://prod-emb:8083/v1", "dimensions": 1024, "maxTokens": 512 }
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

The `modelId` must match one of the `embeddingProviders` entries: that is how the query reaches the
right `vec.<modelId>` field. Both runtimes must produce vectors in the same space, typically by serving
the same weights.

## Fallback runtimes

An `embeddingProviders` entry can list `fallbacks`: other runtimes for the *same* model, tried in order
at index time when the primary is unreachable:

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

A fallback inherits the parent's `modelId` and `dimensions` when omitted; if given they must match.
`pharos index` and `pharos embed` each run in a fresh JVM, so the primary's reachability is probed again
on every run. The vector field and the embedding cache always follow the parent's config, so vectors
written through the fallback and through the primary land in the same field and cache.

## The embedding cache

Vectors are cached on disk per model, keyed by `SHA-256(embedding text)`
(`<index-dir>/<project>/embed-cache.<model>.{bin,meta}`). Full and incremental index runs check it first,
so unchanged texts are never embedded again.

```bash
pharos backfill-embedding-cache myproject            # seed the cache from existing vectors (no model calls)
pharos backfill-embedding-cache --all

pharos invalidate-embeddings myproject               # mark docs (.md, .yml, ...) for re-embedding (default)
pharos invalidate-embeddings myproject --scope java  # Java files; also go, all
pharos invalidate-embeddings --all                   # docs in every project
```

`invalidate-embeddings` makes the next incremental `pharos index` re-embed the selected files, using the
cache where texts are unchanged.

A broken model is loud: it logs a WARN and aborts the embedding pass (the cache is saved), and a search
whose query cannot be embedded warns and falls back to keyword-only results.

## Skipping embeddings for one run

```bash
pharos index /path/to/project --no-embed
```

`--no-embed` never loads the model.
