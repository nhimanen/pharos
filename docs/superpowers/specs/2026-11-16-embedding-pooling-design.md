# Configurable Pooling, Prefixes and Model Switch: Design

Status: approved direction (2026-11-16, "move forward the embedding model change") · Evidence: the embedding benchmark of 2026-11-15/16 (`../pharos-emb/.bench`, results in the conversation)

## Intent

pharos can only run embedding models that use mean pooling, need no query/document prefixes, and ship a single-file `onnx/model.onnx`. On a 200-query code-search benchmark, **granite-embedding-english-r2** (IBM, Apache-2.0, ModernBERT, 768 dims) beats the current default jina-embeddings-v2-base-code on vector search: MRR@10 0.518 vs 0.454, paired 95% CI for the difference [+0.017, +0.111]. That holds only when it runs as its model card specifies (CLS pooling). Run the way pharos runs models today, it scores 0.312. On simulated hybrid search the two are tied: +0.016, CI [−0.023, +0.054].

The goal is to make pharos able to run such models correctly, then decide the default based on pharos's **real** pipelines rather than the simulation.

## Goals

- Per-provider `pooling`: `mean` (default, today's behaviour) or `cls`.
- Per-provider `queryPrefix` and `documentPrefix` (default empty), applied to queries and documents respectively.
- The model downloader also fetches `onnx/model.onnx_data` when the repo has it (external-data ONNX).
- Embedding-cache and file-state fingerprints include pooling and prefixes. Existing jina fingerprints must stay byte-identical, so no cache is invalidated by upgrading.
- A real A/B comparison of jina against granite-r2 through pharos's own vector, hybrid and hybrid-reranked pipelines. The default changes only if granite-r2 wins there (decision gate below).

## Non-goals

- Last-token pooling (Qwen3-style decoders) and models with a dense head (EmbeddingGemma): not justified by the benchmark.
- Quantized (int8/fp16) variants.
- Changing the default within this branch without passing the gate.

## Design

- **Query vs document:** `EmbeddingProvider` gains `default float[] embedQuery(String q)` and `default float[] embedDocument(String d)`, both delegating to `embed()`. Batch document paths (`embedBatch`, `embedChunked`) are document-side.
  - Query call sites switch to `embedQuery`: `VectorSearchStrategy:58`, `UnifiedRetrievalStage:123`, and the vector snippet resolver at `SearchEngine:~286` (it embeds the query).
  - The warm-up call stays as is.
  - `MultiModelEmbedder` and the indexing batch paths use document embedding.
- **DJL provider:** the translators (`SentenceTransformerTranslator`, `BatchSentenceTransformerTranslator`) take a `Pooling` enum: MEAN as today, CLS = `hidden[:, 0, :]`. `DjlEmbeddingProvider` applies the prefixes before tokenization.
- **Remote OpenAI-compatible provider:** applies the prefixes to the input text. Pooling does not apply (the server pools); setting `pooling` on an `openai` provider is a config validation error.
- **Fingerprint:** `IndexVersions.modelFingerprint(cfg)` appends `:pool=<p>:qp=<sha8>:dp=<sha8>` **only when** pooling ≠ mean or a prefix is non-empty. Existing configs keep their exact fingerprint.
- **Downloader:** after `model.onnx`, try `onnx/model.onnx_data`. A 404 is fine; any other error fails the load. The file must sit next to `model.onnx` with exactly that name, which is where ONNX Runtime resolves external data.
- **Config example** (README):
  ```json
  { "modelId": "granite-r2", "type": "djl",
    "url": "hf://onnx-community/granite-embedding-english-r2-ONNX",
    "dimensions": 768, "maxTokens": 512, "pooling": "cls" }
  ```

## Decision gate (A/B)

- **Indexes:** isolated homes (`-Dpharos.home`), one per model, each indexing **pharos and lucene** with embeddings. These two cover 99 of the 200 benchmark queries, and lucene is about 77k documents. All four projects would cost more than ten CPU-hours per model on this laptop.
- **Cost:** jina reuses the existing embedding caches by copying the cache files into its isolated home, so its run is cheap. granite-r2 is embedded from scratch, which takes several hours on CPU.
- **Benchmark:** `eval/benchmark.py`, filtered to the pharos and lucene queries, against each model's daemon (isolated port), on the pipelines `vector`, `hybrid`, `hybrid-rrf` and `hybrid-reranked`.
- **Switch the default only if** granite-r2's MRR@10 is at least as good as jina's on `hybrid-reranked` and `hybrid`, and better on `vector` (paired bootstrap CI excluding 0). Otherwise ship the pooling support with a documented opt-in config for granite-r2.
- **If the default switches:** vector dimensions are unchanged (768), but the fingerprint changes, so every user re-embeds. That goes into a release (1.5.0) with a CHANGELOG `**Index format:** 1 (unchanged)` line and an UPGRADING note on re-embedding with `pharos embed` (caches don't carry over across models). The index format does not change, because vectors live in a per-model field (`vec.<modelId>`).

## Risks

- **Community ONNX export:** `onnx-community/granite-embedding-english-r2-ONNX` is not IBM's own export. Pin the repo revision (commit SHA) in the default URL if the default switches.
- **Memory:** ModernBERT's 22 layers give a higher peak RSS (2.8 GB vs 2.3 GB in the benchmark). Indexing threads may need a lower default for this model.
