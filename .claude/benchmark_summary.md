# Pipeline Benchmark History

## How to reproduce

```bash
cd /home/nhimanen/projects/pharos
python3 eval/benchmark.py          # uses cached results (~0.2s)
rm -f eval/benchmark_cache.json && python3 eval/benchmark.py  # full re-run (~10 min)
```

Query set: 200 queries × 8 categories (A–H), 4 projects (lucene, solr, vespa, pharos).
Golden set: `eval/benchmark.py` (QUERIES list with ground truth FQNs).

---

## 2026-05-21 — commit 528c0c4  ← CURRENT

**Config:**
- All embeddings indexed (jina-embeddings-v2-base-code + chunk vectors)
- Cross-encoder enabled (ms-marco-MiniLM-L-6-v2), batched via `CrossEncoderBatchTranslator`
- `classType` field indexed (interface/abstract/enum/record/annotation/class)
- Auto-mined synonyms **disabled** — hand-curated rules only (34 lines)
- FST QueryRouter with intents: BEHAVIORAL, INTERFACE, ABSTRACT, ENUM, RECORD, ANNOTATION, IMPLEMENTATION, CONFIG, LIFECYCLE, KEYWORD, KEYWORD_TECHNICAL, HYBRID, JAVADOC

**Key changes since previous baseline:**
- Full re-index with `classType` field → `classTypeBonus()` boosts abstract/interface on INTERFACE/ABSTRACT intent
- `auto` routes CONFIG → hybrid-reranked (CE bridges vocabulary gaps)
- `unified`: BM25∪KNN union pool with adaptive BM25_BONUS per intent
- Dynamic Borda weights per intent (grid-search calibrated)
- Synonym ablation: stripping 106K auto-mined rules → **+0.005 MRR for auto**

### Overall

| Metric   | keyword | auto  | vector | unified | hybrid | h-diverse | h-reranked | h-re-div |
|----------|---------|-------|--------|---------|--------|-----------|------------|----------|
| P@1      | 0.485   | **0.510** | 0.335 | 0.450 | 0.435 | 0.450  | 0.340      | 0.350    |
| P@3      | 0.640   | **0.670** | 0.455 | 0.650 | 0.615 | 0.640  | 0.600      | 0.580    |
| P@5      | 0.685   | 0.705 | 0.510  | 0.705   | 0.690 | **0.715** | 0.665     | 0.700    |
| P@10     | 0.715   | 0.745 | 0.575  | 0.745   | **0.780** | 0.775 | 0.760  | 0.775    |
| MRR@10   | 0.570   | **0.595** | 0.415 | 0.556 | 0.543 | 0.557  | 0.483      | 0.490    |

### MRR@10 by Category

| Category              | kw    | auto  | vec   | unified | hybrid | h-div | h-re  | h-re-div | Winner        |
|-----------------------|-------|-------|-------|---------|--------|-------|-------|----------|---------------|
| A: Exact name         | 0.933 | 0.933 | 0.836 | **0.970** | 0.894 | 0.933 | 0.717 | 0.755 | **unified**   |
| B: Named-concept      | **0.673** | **0.673** | 0.431 | 0.602 | 0.591 | 0.644 | 0.493 | 0.465 | **kw/auto** |
| C: Javadoc            | 0.557 | **0.561** | 0.341 | 0.441 | 0.529 | 0.490 | 0.406 | 0.394 | **auto**    |
| D: Behavioral/intent  | 0.410 | 0.430 | 0.238 | 0.403 | **0.435** | 0.428 | 0.387 | 0.419 | **hybrid** |
| E: Error/lifecycle    | **0.588** | **0.588** | 0.406 | 0.560 | 0.567 | 0.570 | 0.435 | 0.479 | **kw/auto** |
| F: Config/tuning      | 0.463 | 0.527 | 0.358 | 0.408 | 0.473 | 0.513 | **0.588** | 0.585 | **h-reranked** |
| G: Interface/contract | 0.245 | 0.439 | 0.291 | **0.458** | 0.291 | 0.335 | 0.290 | 0.250 | **unified** |
| H: Pattern            | 0.567 | 0.567 | 0.385 | **0.575** | 0.432 | 0.436 | 0.517 | 0.502 | **unified** |

### By Project (MRR@10)

| Project | keyword | auto  | vector | unified | hybrid | h-div | h-re  | h-re-div |
|---------|---------|-------|--------|---------|--------|-------|-------|----------|
| lucene  | 0.508   | 0.515 | 0.355  | 0.488   | 0.435  | 0.446 | 0.402 | 0.411    |
| solr    | 0.488   | **0.518** | 0.273 | 0.465 | 0.436 | 0.458 | 0.330 | 0.382  |
| vespa   | **0.602** | 0.574 | 0.299 | 0.504 | 0.513 | 0.483 | 0.465 | 0.460  |
| pharos  | 0.679   | **0.727** | 0.703 | **0.789** | 0.772 | 0.751 | 0.718 | 0.720 |

### Architecture notes
- `auto`: FST QueryRouter (intent-code-search.csv) → RouterDispatcher → keyword/hybrid/hybrid-reranked child pipeline
  - CONFIG intent → hybrid-reranked (CE bridges vocabulary gaps: "configure" ↔ "set")
  - Dynamic Borda weights per intent (KEYWORD_TECHNICAL=0.9, BEHAVIORAL=0.7, etc.)
- `unified`: FST QueryRouter → BM25∪KNN union pool → LateInteractionRescorer (adaptive BM25_BONUS per intent)
  - ABSTRACT/ENUM/RECORD/ANNOTATION=0.05, BEHAVIORAL=0.15, INTERFACE=0.05, KEYWORD=0.30, CONFIG=0.80
  - classTypeBonus applied: interface 3.0×, abstract 2.5× for INTERFACE intent
- Synonym ablation finding: auto-mined synonyms hurt (−0.054 MRR). Disabled.
- Full detail: `eval/benchmark_cache.json`, `eval/synonym_ablation_results.json`

---

## 2026-05-19 — commit 6bb2908

**Config:** All embeddings indexed. Cross-encoder enabled. Auto-mined synonyms active (106K rules).

### Overall

| Metric   | keyword | auto  | vector | unified | hybrid | h-diverse | h-reranked | h-re-div |
|----------|---------|-------|--------|---------|--------|-----------|------------|----------|
| P@1      | 0.485   | 0.500 | 0.320  | 0.455   | 0.415  | 0.415     | 0.335      | 0.350    |
| P@3      | 0.645   | 0.645 | 0.435  | 0.645   | 0.605  | 0.595     | 0.555      | 0.560    |
| P@5      | 0.675   | 0.675 | 0.500  | 0.690   | 0.690  | 0.695     | 0.665      | 0.675    |
| P@10     | 0.685   | 0.710 | 0.560  | 0.735   | 0.785  | 0.770     | 0.760      | 0.770    |
| MRR@10   | 0.564   | 0.575 | 0.398  | 0.553   | 0.530  | 0.527     | 0.469      | 0.485    |
