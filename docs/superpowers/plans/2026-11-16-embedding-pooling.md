# Configurable Pooling, Prefixes and the granite-r2 A/B — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let pharos run CLS-pooled and prefix-using embedding models correctly, including external-data ONNX, then decide on granite-embedding-english-r2 as the default through a real A/B test on pharos's own pipelines.

**Architecture:** The query/document distinction moves into the `EmbeddingProvider` interface (default methods). The DJL translators get a `Pooling` strategy. Prefixes are applied per side. Fingerprints change only for non-default settings. The downloader fetches `model.onnx_data`. The A/B test runs isolated pharos homes and reuses `eval/benchmark.py`.

**Tech Stack:** Java 25, DJL + ONNX Runtime, Jackson, JUnit 5, Python 3 (`eval/benchmark.py`).

**Spec:** `docs/superpowers/specs/2026-11-16-embedding-pooling-design.md`

## Global Constraints

- **Defaults:** `pooling` defaults to `mean`; `queryPrefix` and `documentPrefix` default to empty. With these defaults, behaviour and `IndexVersions.modelFingerprint(cfg)` are **byte-identical** to today.
- **Fingerprint suffix:** `:pool=<mean|cls>:qp=<first 8 hex of sha256(queryPrefix)>:dp=<…>`, appended only when the setting is non-default.
- **Remote providers:** `pooling` other than `mean` on an `openai`-type provider fails config validation, naming the provider's modelId. Prefixes do apply to remote providers.
- **Downloader:** fetch `onnx/model.onnx_data` after `onnx/model.onnx`. HTTP 404 means the model has no external data (fine); any other failure fails the model load.
- **Isolation:** tests and the A/B run never touch the real `~/.pharos` (`pharos.home`).
- **Model registry:** no new default model in Tasks 1–3. The default switches only in Task 5, and only if the Task 4 gate passes.

## Review Focus

1. **Snippet resolver and RRF paths:** a query embedded on the document side (or the reverse) silently degrades results. Every query call site must use `embedQuery`, and a test with a prefix-sensitive fake provider proves it (Task 1).
2. **Mixed-prefix configs:** changing a prefix must invalidate the embedding cache and file state (via the fingerprint) instead of silently mixing vectors (Task 1).
3. **CLS on an unexpected output layout:** an ONNX model whose first output is already pooled (2-D) instead of a `[batch, seq, hidden]` tensor must fail loudly, with a message suggesting `pooling: mean`. It must not index nonsense (Task 2).
4. **Partial download of `model.onnx_data`:** a partial or failed download must not leave a cached model directory that later loads half a model. Download to temp files and move them into place (Task 3).
5. **A/B fairness:** both models must be benchmarked on identical Lucene/BM25 content (same source commit, same chunking), so the only difference is the vectors (Task 4).

---

### Task 1: Query/document embedding API, prefixes, fingerprint, config

**Files:**
- Modify: `embedding/EmbeddingProvider.java` (`embedQuery`/`embedDocument` default methods)
- Modify: `config/EmbeddingProviderConfig.java` (`pooling`, `queryPrefix`, `documentPrefix`, plus validation), `indexer/IndexVersions.java` (fingerprint)
- Modify: query sites `search/VectorSearchStrategy.java:~58`, `search/pipeline/UnifiedRetrievalStage.java:~123`, `search/SearchEngine.java:~286`; document sites `indexer/MultiModelEmbedder.java` and the ProjectIndexManager embed runtimes, which use the batch APIs
- Modify: `embedding/OpenAiHttpEmbeddingProvider.java` and the fallback wrapper (apply prefixes)
- Test: `embedding/EmbeddingPrefixTest.java`, `indexer/FingerprintCompatTest.java`, `config/EmbeddingProviderConfigTest` (extend or create)

**Interfaces:**
- Produces:
  - `enum Pooling { MEAN, CLS }`, with JSON values `"mean"` and `"cls"`.
  - `EmbeddingProviderConfig.getPooling()`, `getQueryPrefix()`, `getDocumentPrefix()`.
  - `EmbeddingProvider.embedQuery(String)` and `embedDocument(String)`. Batch methods count as document side.
  - Providers apply prefixes internally. Callers never add prefixes themselves.

- [ ] **Step 1: Write failing tests.**
  - `FingerprintCompatTest`:
    - `defaultConfigFingerprintUnchanged`: pin today's exact fingerprint string for a jina config.
    - `cls`, `queryPrefix` and `documentPrefix` each change the fingerprint, and each in a different way.
  - `EmbeddingPrefixTest`:
    - Use a fake provider that records its inputs.
    - Run a vector search through `SearchEngine`; the query is recorded with the query prefix.
    - Run indexing through `ProjectIndexManager` on sample-app; document texts carry the document prefix.
    - The hybrid and unified pipelines also embed with the query prefix.
  - Config tests:
    - JSON round trip of the three fields.
    - `openai` + `cls` → validation error naming the modelId.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the full suite** (`mvn test`). Expected: pass, and the golden or quality tests are unchanged.
- [ ] **Step 5: Commit.** Message: `Embedding: Query/document embedding with configurable prefixes; fingerprint covers them`.

### Task 2: CLS pooling in the DJL translators

**Files:**
- Modify: `embedding/SentenceTransformerTranslator.java`, `embedding/BatchSentenceTransformerTranslator.java`, `embedding/DjlEmbeddingProvider.java` (pass `Pooling`; apply prefixes before tokenizing)
- Test: `embedding/PoolingTranslatorTest.java` (NDArray-level unit tests with a synthetic hidden-state tensor; no model download)

- [ ] **Step 1: Write failing tests.**
  - `meanPoolingMatchesToday`: masked mean on a synthetic `[2, 4, 3]` tensor with padding.
  - `clsPoolingTakesFirstToken`.
  - `twoDimensionalOutputWithClsFailsClearly`: the message names `pooling` and suggests `mean`.
  - `batchAndSingleAgree`, for both pooling modes.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests, then `mvn test`.**
- [ ] **Step 5: Commit.** Message: `Embedding: CLS pooling for DJL models`.

### Task 3: External-data ONNX download

**Files:**
- Modify: `embedding/DjlEmbeddingProvider.java` (`prepareHuggingFaceFiles`, `downloadWithRedirects`)
- Test: `embedding/HuggingFaceDownloadTest.java`. Use a local `com.sun.net.httpserver.HttpServer` serving `onnx/model.onnx`, an optional `onnx/model.onnx_data`, and `tokenizer.json`; point the base URL at it through a package-private override.

- [ ] **Step 1: Write failing tests.**
  - `fetchesOnnxDataWhenPresent`.
  - `missingOnnxDataIs404AndFine`.
  - `onnxDataServerErrorFailsLoad`: a 500 response fails the load, and the cache dir holds no partial `model.onnx_data`.
  - `interruptedDownloadLeavesNoPartialFile`: the server closes mid-body, and there is no file at the final path.
- [ ] **Step 2: Run them to verify they fail.**
- [ ] **Step 3: Implement.** Download to `<file>.part` and move it into place atomically. Write a `.complete` marker after all files are present. A cache dir without the marker is re-downloaded.
- [ ] **Step 4: Run the tests and `mvn test`. Then a one-off manual smoke test**, allowed network: load granite-r2 with `pooling: cls` in a throwaway `-Dpharos.home`, embed two strings, and print cosine similarities for a code query and its matching method versus an unrelated one. Record the output in the report.
- [ ] **Step 5: Commit.** Message: `Embedding: Fetch external ONNX data; atomic, resumable model downloads`.

### Task 4: Real A/B — jina vs granite-r2 through pharos's pipelines (decision gate)

**Files:**
- Create: `scripts/embedding-ab.sh` (orchestration), `eval/ab_compare.py` (paired comparison: reads two `benchmark.py` result JSONs, prints per-pipeline MRR@10, P@1 and Recall@10 with paired bootstrap 95% CIs, per category and per project)
- Create: `docs/superpowers/gates/2026-11-16-embedding-ab.md` (results and decision)

- [ ] **Step 1: Implement the scripts.**
  - Two isolated homes under the scratchpad (`-Dpharos.home`), each with a config holding a single provider: jina as today, or granite-r2 with `pooling: cls`.
  - Index the **same** checkout of pharos (current main) and the lucene source (the user registry's `rootPath`, read only), with embeddings.
  - Seed jina's home with copies of the user's existing `embed-cache*.bin` for those projects. They are read-only copies, never moved, so jina's run is mostly cache hits.
  - Start two daemons on isolated ports. Run `eval/benchmark.py`, filtered to pharos and lucene queries, against each, for `vector`, `hybrid`, `hybrid-rrf` and `hybrid-reranked`. Save the result JSONs.
  - Run `ab_compare.py`.
  - Run everything under `systemd-run --user --scope -p MemoryMax=10G`, with heaps capped so the two daemons fit.
  - Never touch the real `~/.pharos` (marker check). Kill processes by PID only.
- [ ] **Step 2: Run it.** This is long, hours for granite on CPU. Run it in the background and check progress through its logs.
- [ ] **Step 3: Apply the decision gate from the spec.** Write the gate doc with all numbers and the decision (`SWITCH` or `OPT-IN ONLY`).
- [ ] **Step 4: Commit** the scripts and the gate doc. Message: `Eval: real A/B of jina vs granite-r2; decision gate`.

### Task 5: Default switch or opt-in documentation (depends on the gate)

**If `SWITCH`:**
- Change the default provider config to granite-r2: `pooling: cls`, the onnx-community repo pinned to a commit SHA in the URL (support `hf://<repo>@<sha>` if the downloader doesn't yet, with a test), and dimensions 768.
- Update README and CLAUDE.md, and add a CHANGELOG `## [Unreleased]` entry: `**Index format:** 1 (unchanged)`, re-embed recommended, existing jina caches are not reused.
- Add an UPGRADING.md section on switching models: `pharos embed`, or keep jina by pinning the old provider config.
- Tests: the default config loads with `cls` and has the new fingerprint, and the existing jina config keeps working.

**If `OPT-IN ONLY`:**
- Add a README section with the granite-r2 config (`pooling: cls`) and the A/B numbers, and a CHANGELOG entry for the pooling and prefix support.

- [ ] Steps: tests first, implement, `mvn test`, commit (`Embedding: <switch default to granite-r2 | document granite-r2 opt-in>`).
