# Pipeline Comparison — 200 Queries with Ground Truth

Pipelines: keyword, auto, vector, unified, hybrid, hybrid-diverse, hybrid-reranked, hybrid-reranked-diverse  
Queries: 200 across 8 categories, 4 projects (lucene, solr, vespa, pharos)  
Results per query: 10  

> All embeddings indexed. All 9 pipelines active. 
> `auto` routes single-token/FQN queries to keyword, NL queries to hybrid. 
> `unified` uses BM25 + late-interaction vector boost in a single pass.

## Overall Metrics (200 queries)

| Metric | keyword | auto | vector | unified | hybrid | hybrid-diverse | hybrid-reranked | hybrid-reranked-diverse |
|--------|---|---|---|---|---|---|---|---|
| P@1    | 0.485 | 0.510 | 0.335 | 0.450 | 0.435 | 0.450 | 0.340 | 0.350 |
| P@3    | 0.640 | 0.670 | 0.455 | 0.650 | 0.615 | 0.640 | 0.600 | 0.580 |
| P@5    | 0.685 | 0.705 | 0.510 | 0.705 | 0.690 | 0.715 | 0.665 | 0.700 |
| P@10   | 0.715 | 0.745 | 0.575 | 0.745 | 0.780 | 0.775 | 0.760 | 0.775 |
| MRR@10 | 0.570 | 0.595 | 0.415 | 0.556 | 0.543 | 0.557 | 0.483 | 0.490 |

## P@1 by Category

| Category | n | keyword | auto | vector | unified | hybrid | hybrid-diver | hybrid-reran | hybrid-reran | best |
|----------|---|---|---|---|---|---|---|---|---|------|
| A: Exact name (single term) | 30 | 0.93 | 0.93 | 0.77 | 0.97 | 0.83 | 0.90 | 0.57 | 0.60 | **un** |
| B: Named-concept phrase | 25 | 0.60 | 0.60 | 0.40 | 0.44 | 0.44 | 0.52 | 0.36 | 0.28 | **kw** |
| C: Javadoc / documentation | 30 | 0.50 | 0.50 | 0.23 | 0.33 | 0.43 | 0.37 | 0.27 | 0.23 | **kw** |
| D: Behavioral / intent | 40 | 0.30 | 0.33 | 0.17 | 0.30 | 0.33 | 0.33 | 0.25 | 0.33 | **au** |
| E: Error / lifecycle | 20 | 0.45 | 0.45 | 0.30 | 0.45 | 0.45 | 0.45 | 0.25 | 0.35 | **kw** |
| F: Config / tuning | 20 | 0.35 | 0.35 | 0.25 | 0.20 | 0.35 | 0.35 | 0.45 | 0.45 | **hr** |
| G: Interface / contract | 15 | 0.13 | 0.40 | 0.20 | 0.40 | 0.20 | 0.27 | 0.13 | 0.13 | **au** |
| H: Cross-cutting / pattern | 20 | 0.45 | 0.45 | 0.30 | 0.45 | 0.30 | 0.30 | 0.40 | 0.35 | **kw** |

## P@3 by Category

| Category | n | keyword | auto | vector | unified | hybrid | hybrid-diver | hybrid-reran | hybrid-reran | best |
|----------|---|---|---|---|---|---|---|---|---|------|
| A: Exact name (single term) | 30 | 0.93 | 0.93 | 0.90 | 0.97 | 0.97 | 0.97 | 0.80 | 0.87 | **un** |
| B: Named-concept phrase | 25 | 0.76 | 0.76 | 0.40 | 0.76 | 0.64 | 0.68 | 0.56 | 0.56 | **kw** |
| C: Javadoc / documentation | 30 | 0.57 | 0.57 | 0.40 | 0.50 | 0.57 | 0.60 | 0.53 | 0.47 | **hd** |
| D: Behavioral / intent | 40 | 0.50 | 0.50 | 0.25 | 0.53 | 0.55 | 0.55 | 0.53 | 0.47 | **hy** |
| E: Error / lifecycle | 20 | 0.75 | 0.75 | 0.45 | 0.65 | 0.65 | 0.65 | 0.55 | 0.55 | **kw** |
| F: Config / tuning | 20 | 0.55 | 0.70 | 0.45 | 0.55 | 0.55 | 0.65 | 0.75 | 0.75 | **hr** |
| G: Interface / contract | 15 | 0.27 | 0.47 | 0.33 | 0.47 | 0.33 | 0.40 | 0.40 | 0.27 | **au** |
| H: Cross-cutting / pattern | 20 | 0.70 | 0.70 | 0.45 | 0.75 | 0.50 | 0.50 | 0.65 | 0.65 | **un** |

## P@5 by Category

| Category | n | keyword | auto | vector | unified | hybrid | hybrid-diver | hybrid-reran | hybrid-reran | best |
|----------|---|---|---|---|---|---|---|---|---|------|
| A: Exact name (single term) | 30 | 0.93 | 0.93 | 0.93 | 0.97 | 0.97 | 0.97 | 0.90 | 0.97 | **un** |
| B: Named-concept phrase | 25 | 0.76 | 0.76 | 0.48 | 0.80 | 0.84 | 0.80 | 0.68 | 0.76 | **hy** |
| C: Javadoc / documentation | 30 | 0.63 | 0.63 | 0.43 | 0.67 | 0.63 | 0.67 | 0.60 | 0.67 | **un** |
| D: Behavioral / intent | 40 | 0.55 | 0.55 | 0.33 | 0.53 | 0.57 | 0.57 | 0.55 | 0.57 | **hy** |
| E: Error / lifecycle | 20 | 0.80 | 0.80 | 0.55 | 0.70 | 0.70 | 0.75 | 0.65 | 0.65 | **kw** |
| F: Config / tuning | 20 | 0.60 | 0.75 | 0.45 | 0.70 | 0.65 | 0.75 | 0.80 | 0.80 | **hr** |
| G: Interface / contract | 15 | 0.47 | 0.53 | 0.40 | 0.53 | 0.40 | 0.47 | 0.47 | 0.40 | **au** |
| H: Cross-cutting / pattern | 20 | 0.70 | 0.70 | 0.50 | 0.75 | 0.65 | 0.70 | 0.65 | 0.70 | **un** |

## P@10 by Category

| Category | n | keyword | auto | vector | unified | hybrid | hybrid-diver | hybrid-reran | hybrid-reran | best |
|----------|---|---|---|---|---|---|---|---|---|------|
| A: Exact name (single term) | 30 | 0.93 | 0.93 | 0.97 | 1.00 | 0.97 | 0.97 | 0.97 | 0.97 | **un** |
| B: Named-concept phrase | 25 | 0.76 | 0.76 | 0.56 | 0.84 | 0.92 | 0.92 | 0.84 | 0.92 | **hy** |
| C: Javadoc / documentation | 30 | 0.70 | 0.73 | 0.53 | 0.73 | 0.77 | 0.73 | 0.70 | 0.73 | **hy** |
| D: Behavioral / intent | 40 | 0.60 | 0.65 | 0.42 | 0.57 | 0.68 | 0.65 | 0.65 | 0.65 | **hy** |
| E: Error / lifecycle | 20 | 0.80 | 0.80 | 0.60 | 0.70 | 0.75 | 0.80 | 0.80 | 0.80 | **kw** |
| F: Config / tuning | 20 | 0.65 | 0.80 | 0.50 | 0.75 | 0.80 | 0.80 | 0.80 | 0.80 | **au** |
| G: Interface / contract | 15 | 0.53 | 0.53 | 0.47 | 0.60 | 0.53 | 0.53 | 0.60 | 0.53 | **un** |
| H: Cross-cutting / pattern | 20 | 0.70 | 0.70 | 0.50 | 0.75 | 0.75 | 0.75 | 0.70 | 0.75 | **un** |

## MRR@10 by Category

| Category | n | keyword | auto | vector | unified | hybrid | hybrid-diver | hybrid-reran | hybrid-reran | best |
|----------|---|---|---|---|---|---|---|---|---|------|
| A: Exact name (single term) | 30 | 0.933 | 0.933 | 0.836 | 0.970 | 0.894 | 0.933 | 0.717 | 0.755 | **un** |
| B: Named-concept phrase | 25 | 0.673 | 0.673 | 0.431 | 0.602 | 0.591 | 0.644 | 0.493 | 0.465 | **kw** |
| C: Javadoc / documentation | 30 | 0.557 | 0.561 | 0.341 | 0.441 | 0.529 | 0.490 | 0.406 | 0.394 | **au** |
| D: Behavioral / intent | 40 | 0.410 | 0.430 | 0.238 | 0.403 | 0.435 | 0.428 | 0.387 | 0.419 | **hy** |
| E: Error / lifecycle | 20 | 0.588 | 0.588 | 0.406 | 0.560 | 0.567 | 0.570 | 0.435 | 0.479 | **kw** |
| F: Config / tuning | 20 | 0.463 | 0.527 | 0.358 | 0.408 | 0.473 | 0.513 | 0.588 | 0.585 | **hr** |
| G: Interface / contract | 15 | 0.245 | 0.439 | 0.291 | 0.458 | 0.291 | 0.335 | 0.290 | 0.250 | **un** |
| H: Cross-cutting / pattern | 20 | 0.567 | 0.567 | 0.385 | 0.575 | 0.432 | 0.436 | 0.517 | 0.502 | **un** |

## All Metrics by Project

### Lucene (n=54)

| Metric | keyword | auto | vector | unified | hybrid | hybrid-diverse | hybrid-reranked | hybrid-reranked-diverse |
|--------|---|---|---|---|---|---|---|---|
| P@1    | 0.463 | 0.519 | 0.278 | 0.389 | 0.333 | 0.389 | 0.278 | 0.222 |
| P@3    | 0.574 | 0.630 | 0.389 | 0.556 | 0.500 | 0.537 | 0.537 | 0.500 |
| P@5    | 0.630 | 0.667 | 0.463 | 0.648 | 0.630 | 0.630 | 0.648 | 0.685 |
| P@10   | 0.648 | 0.685 | 0.519 | 0.704 | 0.704 | 0.722 | 0.704 | 0.722 |
| MRR@10 | 0.525 | 0.575 | 0.355 | 0.484 | 0.449 | 0.487 | 0.430 | 0.404 |

### Solr (n=53)

| Metric | keyword | auto | vector | unified | hybrid | hybrid-diverse | hybrid-reranked | hybrid-reranked-diverse |
|--------|---|---|---|---|---|---|---|---|
| P@1    | 0.358 | 0.396 | 0.245 | 0.321 | 0.396 | 0.396 | 0.302 | 0.358 |
| P@3    | 0.566 | 0.604 | 0.321 | 0.528 | 0.528 | 0.566 | 0.472 | 0.453 |
| P@5    | 0.623 | 0.642 | 0.415 | 0.585 | 0.585 | 0.642 | 0.528 | 0.566 |
| P@10   | 0.660 | 0.698 | 0.491 | 0.642 | 0.660 | 0.679 | 0.660 | 0.679 |
| MRR@10 | 0.470 | 0.508 | 0.314 | 0.432 | 0.476 | 0.491 | 0.401 | 0.444 |

### Vespa (n=48)

| Metric | keyword | auto | vector | unified | hybrid | hybrid-diverse | hybrid-reranked | hybrid-reranked-diverse |
|--------|---|---|---|---|---|---|---|---|
| P@1    | 0.521 | 0.458 | 0.250 | 0.438 | 0.417 | 0.375 | 0.292 | 0.271 |
| P@3    | 0.646 | 0.625 | 0.333 | 0.604 | 0.604 | 0.625 | 0.542 | 0.500 |
| P@5    | 0.667 | 0.667 | 0.375 | 0.667 | 0.688 | 0.729 | 0.604 | 0.667 |
| P@10   | 0.688 | 0.688 | 0.438 | 0.688 | 0.812 | 0.792 | 0.771 | 0.792 |
| MRR@10 | 0.590 | 0.546 | 0.310 | 0.531 | 0.531 | 0.515 | 0.439 | 0.412 |

### Pharos (n=45)

| Metric | keyword | auto | vector | unified | hybrid | hybrid-diverse | hybrid-reranked | hybrid-reranked-diverse |
|--------|---|---|---|---|---|---|---|---|
| P@1    | 0.622 | 0.689 | 0.600 | 0.689 | 0.622 | 0.667 | 0.511 | 0.578 |
| P@3    | 0.800 | 0.844 | 0.822 | 0.956 | 0.867 | 0.867 | 0.889 | 0.911 |
| P@5    | 0.844 | 0.867 | 0.822 | 0.956 | 0.889 | 0.889 | 0.911 | 0.911 |
| P@10   | 0.889 | 0.933 | 0.889 | 0.978 | 0.978 | 0.933 | 0.933 | 0.933 |
| MRR@10 | 0.720 | 0.775 | 0.719 | 0.815 | 0.748 | 0.762 | 0.689 | 0.728 |


## MRR Heatmap: Category × Pipeline

| Category | keyword | auto | vector | unified | hybrid | hybrid-div | hybrid-rer | hybrid-rer | Winner |
|----------|---|---|---|---|---|---|---|---|--------|
| A: Exact name (single term) | 0.933 | 0.933 | 0.836 | 0.970 | 0.894 | 0.933 | 0.717 | 0.755 | **unified** |
| B: Named-concept phrase | 0.673 | 0.673 | 0.431 | 0.602 | 0.591 | 0.644 | 0.493 | 0.465 | **keyword** |
| C: Javadoc / documentation | 0.557 | 0.561 | 0.341 | 0.441 | 0.529 | 0.490 | 0.406 | 0.394 | **auto** |
| D: Behavioral / intent | 0.410 | 0.430 | 0.238 | 0.403 | 0.435 | 0.428 | 0.387 | 0.419 | **hybrid** |
| E: Error / lifecycle | 0.588 | 0.588 | 0.406 | 0.560 | 0.567 | 0.570 | 0.435 | 0.479 | **keyword** |
| F: Config / tuning | 0.463 | 0.527 | 0.358 | 0.408 | 0.473 | 0.513 | 0.588 | 0.585 | **hybrid-reranked** |
| G: Interface / contract | 0.245 | 0.439 | 0.291 | 0.458 | 0.291 | 0.335 | 0.290 | 0.250 | **unified** |
| H: Cross-cutting / pattern | 0.567 | 0.567 | 0.385 | 0.575 | 0.432 | 0.436 | 0.517 | 0.502 | **unified** |

## Queries Where Reranker Improves Rank by ≥2 Positions

| # | Cat | Project | Query | kw rank | reranked rank | Δ |
|---|-----|---------|-------|---------|---------------|---|
| 179 | G | pharos | interface for search pipeline stage retrieval | 5 | 2 | +3 |
| 153 | F | solr | tune replica sync timeout | 4 | 1 | +3 |
| 65 | C | lucene | applies boost to matching documents | 8 | 5 | +3 |
| 170 | G | lucene | interface for merging index segments | 4 | 2 | +2 |
| 151 | F | solr | set number of shards for new collection | 3 | 1 | +2 |
| 96 | D | solr | find code that synchronises collection state acros | 4 | 2 | +2 |
| 56 | C | lucene | returns the number of deleted documents in a segme | 5 | 3 | +2 |

## Queries Where Reranker Hurts (rank regresses ≥2)

| # | Cat | Project | Query | kw rank | reranked rank | Δ |
|---|-----|---------|-------|---------|---------------|---|
| 154 | F | solr | set soft commit interval for near real time | 6 | >10 | -10 |
| 120 | D | pharos | find code that connects methods across project bou | 10 | >10 | -10 |
| 119 | D | pharos | find code that converts a method body to a short p | 6 | >10 | -10 |
| 73 | C | solr | validates document schema before indexing | 7 | >10 | -10 |
| 44 | B | solr | SolrJ HTTP request response | 1 | 10 | -9 |
| 72 | C | solr | parses field analysis request and returns token br | 1 | 9 | -8 |
| 139 | E | vespa | handle timeout in query execution | 1 | 8 | -7 |
| 88 | D | lucene | find code that turns a stream of tokens into a fix | 1 | 8 | -7 |
| 171 | G | solr | interface for Solr request processing | 1 | 7 | -6 |
| 133 | E | solr | rollback incomplete transaction on error | 3 | 9 | -6 |
| 49 | B | vespa | YQL query parsing select where | 1 | 7 | -6 |
| 48 | B | vespa | grouping aggregation expression result | 1 | 7 | -6 |
| 5 | A | lucene | UnifiedHighlighter | 1 | 7 | -6 |
| 113 | D | vespa | find code that validates a ranking expression befo | 1 | 6 | -5 |
| 82 | C | vespa | converts query to Vespa internal representation | 1 | 6 | -5 |
| 8 | A | lucene | KnnFloatVectorQuery | 1 | 6 | -5 |
| 190 | H | solr | event sourcing transaction log document update | 2 | 6 | -4 |
| 144 | E | pharos | handle cross-encoder scoring exception | 4 | 8 | -4 |
| 92 | D | lucene | find code that estimates the memory usage of a que | 1 | 5 | -4 |
| 41 | B | solr | JSON facet aggregation bucket | 1 | 5 | -4 |

## Hard Queries — Ground Truth Not Found by Any Pipeline

| # | Cat | Conf | Project | Query | Ground truth |
|---|-----|------|---------|-------|--------------|
| 38 | B | MED | solr | SolrCloud replica state transition | `org.apache.solr.common.cloud.ZkStateReader` |
| 40 | B | MED | solr | distributed search shard merge collector | `org.apache.solr.handler.component.SearchHandler` |
| 58 | C | LOW | lucene | normalises similarity score to probability between | `org.apache.lucene.search.similarities.SimilarityBase` |
| 66 | C | MED | solr | handles incoming update requests for SolrCloud | `org.apache.solr.update.processor.DistributedUpdateProcessor` |
| 67 | C | LOW | solr | writes index segments to disk on commit | `org.apache.solr.core.SolrCore` |
| 70 | C | LOW | solr | forwards query to appropriate shard and merges res | `org.apache.solr.handler.component.SearchHandler` |
| 79 | C | LOW | vespa | flushes memory index to disk | `com.yahoo.vespa.searchlib.index.DiskIndexFlushConfig` |
| 80 | C | MED | vespa | iterates over documents in a bucket | `com.yahoo.documentapi.VisitorParameters` |
| 86 | D | MED | lucene | find the code that prevents two writers from acces | `org.apache.lucene.store.NativeFSLockFactory` |
| 89 | D | LOW | lucene | find code that skips over non-matching documents e | `org.apache.lucene.search.DocIdSetIterator` |
| 90 | D | MED | lucene | find code that keeps the most relevant passages fo | `org.apache.lucene.search.uhighlight.PassageScorer` |
| 93 | D | HIGH | lucene | find code that makes sure deleted documents do not | `org.apache.lucene.util.Bits` |
| 94 | D | MED | lucene | find code that reorders search results by a second | `org.apache.lucene.search.Sort` |
| 95 | D | HIGH | lucene | find code that assigns unique ids to facet terms | `org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomy` |
| 98 | D | MED | solr | find code that deduplicates results by a field val | `org.apache.solr.search.CollapsingQParserPlugin` |
| 100 | D | MED | solr | find code that applies custom boosting to specific | `org.apache.solr.search.QueryElevationComponent` |
| 101 | D | MED | solr | find code that streams large result sets without l | `org.apache.solr.handler.ExportHandler` |
| 115 | D | MED | vespa | find code that loads a machine learning model for  | `ai.vespa.triton.TritonOnnxRuntime` |
| 129 | E | MED | lucene | handle out of memory error during indexing | `org.apache.lucene.index.IndexWriter` |
| 135 | E | LOW | solr | graceful shutdown of distributed search | `org.apache.solr.core.SolrCore` |
| 140 | E | LOW | vespa | container startup initialization sequence | `com.yahoo.container.jdisc.Container` |
| 149 | F | MED | lucene | tune HNSW beam width for recall accuracy tradeoff | `org.apache.lucene.util.hnsw.HnswGraphBuilder` |
| 157 | F | LOW | vespa | configure document expiry age | `com.yahoo.vespa.curator.mock.MemoryFileSystem` |
| 167 | G | HIGH | lucene | interface for collecting search results | `org.apache.lucene.search.Collector` |
| 168 | G | HIGH | lucene | interface for reading index segments | `org.apache.lucene.index.LeafReader` |
| 169 | G | MED | lucene | interface for tokenizing text | `org.apache.lucene.analysis.Tokenizer` |
| 173 | G | LOW | solr | interface for search result ranking | `org.apache.solr.search.ReRankQParserPlugin` |
| 181 | H | LOW | lucene | factory pattern dependency injection command | `org.apache.lucene.analysis.TokenizerFactory` |
| 183 | H | MED | lucene | decorator pattern similarity score modification | `org.apache.lucene.search.similarities.PerFieldSimilarityWrap` |
| 188 | H | LOW | solr | retry pattern with backoff for replication | `org.apache.solr.update.PeerSync` |
| 189 | H | LOW | solr | bulkhead pattern request isolation | `org.apache.solr.util.circuitbreaker.CircuitBreaker` |
| 192 | H | LOW | vespa | template method ranking computation | `com.yahoo.search.Searcher` |

## Per-Query Results

Rank shown for each pipeline; `—` = not in top 10.

| # | Cat | Conf | Project | Query | keyword | auto | vector | unified | hybrid | hybrid-div | hybrid-rer | hybrid-rer | GT class |
|---|-----|------|---------|-------|---|---|---|---|---|---|---|---|----------|
| 1 | A | HIGH | lucene | HnswGraphBuilder | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `HnswGraphBuilder` |
| 2 | A | HIGH | lucene | TieredMergePolicy | **1** | **1** | **1** | **1** | **1** | **1** | 2 | **1** | `TieredMergePolicy` |
| 3 | A | HIGH | lucene | BooleanQuery | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `BooleanQuery` |
| 4 | A | HIGH | lucene | DirectoryReader | **1** | **1** | 4 | **1** | 3 | 2 | **1** | **1** | `DirectoryReader` |
| 5 | A | HIGH | lucene | UnifiedHighlighter | **1** | **1** | 2 | **1** | **1** | **1** | 7 | 2 | `UnifiedHighlighter` |
| 6 | A | HIGH | lucene | IndexWriter | **1** | **1** | **1** | **1** | **1** | **1** | **1** | 2 | `IndexWriter` |
| 7 | A | HIGH | lucene | PerFieldSimilarityWrapper | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `PerFieldSimilarityWrapper` |
| 8 | A | HIGH | lucene | KnnFloatVectorQuery | **1** | **1** | 3 | **1** | 2 | **1** | 6 | 4 | `KnnFloatVectorQuery` |
| 9 | A | HIGH | solr | CollapsingQParser | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `CollapsingQParser` |
| 10 | A | HIGH | solr | SolrCloud | — | — | 3 | 10 | — | — | — | — | `ZkController` |
| 11 | A | HIGH | solr | UpdateLog | **1** | **1** | **1** | **1** | **1** | **1** | 4 | 5 | `UpdateLog` |
| 12 | A | HIGH | solr | SpellCheckComponent | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `SpellCheckComponent` |
| 13 | A | HIGH | solr | StreamExpression | **1** | **1** | — | **1** | **1** | **1** | **1** | **1** | `StreamExpression` |
| 14 | A | HIGH | solr | CaffeineCache | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `CaffeineCache` |
| 15 | A | HIGH | solr | Overseer | **1** | **1** | **1** | **1** | **1** | 2 | **1** | **1** | `Overseer` |
| 16 | A | MED | vespa | TritonOnnxEvaluator | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `TritonOnnxEvaluator` |
| 17 | A | MED | vespa | DocumentTypeManager | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `DocumentTypeManager` |
| 18 | A | MED | vespa | SearchChainDispatcher | — | — | **1** | **1** | 2 | **1** | **1** | **1** | `SearchChainDispatcherSear` |
| 19 | A | HIGH | vespa | VisitorParameters | **1** | **1** | 2 | **1** | **1** | **1** | 4 | 5 | `VisitorParameters` |
| 20 | A | HIGH | vespa | ExpressionConverter | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `ExpressionConverter` |
| 21 | A | HIGH | pharos | BordaMerger | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `BordaMerger` |
| 22 | A | HIGH | pharos | CrossEncoderProvider | **1** | **1** | 6 | **1** | 2 | **1** | 2 | 2 | `CrossEncoderProvider` |
| 23 | A | HIGH | pharos | CallGraphBuilder | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `CallGraphBuilder` |
| 24 | A | HIGH | pharos | LuceneIndexer | **1** | **1** | **1** | **1** | **1** | **1** | 5 | **1** | `LuceneIndexer` |
| 25 | A | HIGH | pharos | SearchEngine | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `SearchEngine` |
| 26 | A | HIGH | pharos | FileStateTracker | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `FileStateTracker` |
| 27 | A | HIGH | pharos | DocumentMapper | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `DocumentMapper` |
| 28 | A | HIGH | pharos | McpToolRegistry | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `McpToolRegistry` |
| 29 | A | HIGH | pharos | ProjectRegistry | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `ProjectRegistry` |
| 30 | A | HIGH | pharos | SynonymProvider | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `SynonymProvider` |
| 31 | B | HIGH | lucene | HNSW graph neighbor candidate exploration | 2 | 2 | **1** | **1** | **1** | **1** | **1** | 2 | `HnswGraphSearcher` |
| 32 | B | MED | lucene | inverted index posting list iterator | **1** | **1** | — | 2 | 4 | 4 | 5 | 5 | `PostingIndexInput` |
| 33 | B | HIGH | lucene | segment merge candidate selection | 3 | 3 | 5 | 3 | 5 | 9 | 5 | 4 | `TieredMergePolicy` |
| 34 | B | HIGH | lucene | query parser boolean clause combination | **1** | **1** | 6 | **1** | 4 | 4 | 2 | 2 | `QueryParser` |
| 35 | B | HIGH | lucene | facet taxonomy ordinal mapping | **1** | **1** | — | 2 | 5 | 5 | 2 | 2 | `OrdinalMappingLeafReader` |
| 36 | B | HIGH | lucene | token filter chain pipeline | — | — | — | — | 4 | **1** | — | 3 | `CustomAnalyzer` |
| 37 | B | MED | lucene | BM25 field boost weight computation | — | — | **1** | 8 | 2 | **1** | **1** | **1** | `BM25Similarity` |
| 38 | B | MED | solr | SolrCloud replica state transition | — | — | — | — | — | — | — | — | `ZkStateReader` |
| 39 | B | HIGH | solr | update chain processor document | **1** | **1** | — | **1** | **1** | **1** | **1** | 4 | `UpdateRequestProcessorCha` |
| 40 | B | MED | solr | distributed search shard merge collector | — | — | — | — | — | — | — | — | `SearchHandler` |
| 41 | B | HIGH | solr | JSON facet aggregation bucket | **1** | **1** | 4 | 2 | 3 | 2 | 5 | 6 | `BucketJsonFacet` |
| 42 | B | HIGH | solr | transaction log apply replay | 2 | 2 | **1** | **1** | **1** | **1** | **1** | **1** | `UpdateLog` |
| 43 | B | HIGH | solr | overseer state machine operation | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `Overseer` |
| 44 | B | HIGH | solr | SolrJ HTTP request response | **1** | **1** | — | **1** | **1** | **1** | 10 | 10 | `HttpSolrClient` |
| 45 | B | MED | vespa | ranking expression tensor computation | **1** | **1** | — | **1** | **1** | **1** | **1** | 2 | `RankingExpression` |
| 46 | B | HIGH | vespa | document feed put remove handler | — | — | — | — | 7 | 8 | — | 5 | `DocumentV1ApiHandler` |
| 47 | B | HIGH | vespa | bucket distributor content cluster | — | — | **1** | 4 | 2 | 2 | 6 | 8 | `DistributorCluster` |
| 48 | B | HIGH | vespa | grouping aggregation expression result | **1** | **1** | — | 2 | 7 | 6 | 7 | 7 | `ExpressionCountAggregatio` |
| 49 | B | MED | vespa | YQL query parsing select where | **1** | **1** | — | 3 | **1** | **1** | 7 | 5 | `Query` |
| 50 | B | HIGH | vespa | ONNX model inference runtime | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `TritonOnnxRuntime` |
| 51 | B | HIGH | pharos | BordaMerger Borda count agreement bonus | **1** | **1** | **1** | **1** | **1** | **1** | 3 | 3 | `BordaMerger` |
| 52 | B | HIGH | pharos | cross-project link resolution | **1** | **1** | **1** | **1** | **1** | **1** | 2 | **1** | `CrossProjectLinker` |
| 53 | B | HIGH | pharos | incremental index file state | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `FileStateTracker` |
| 54 | B | HIGH | pharos | MCP server tool registration | 2 | 2 | **1** | 2 | 2 | 2 | 3 | 3 | `McpToolRegistry` |
| 55 | B | HIGH | pharos | query hint project language modifier | **1** | **1** | 6 | 2 | 2 | 2 | **1** | **1** | `SearchEngine` |
| 56 | C | HIGH | lucene | returns the number of deleted documents in | 5 | 5 | 7 | 6 | 4 | 3 | 3 | 4 | `LiveDocs` |
| 57 | C | MED | lucene | opens a reader on the most recent commit p | **1** | **1** | 2 | 3 | 2 | 2 | 2 | 4 | `DirectoryReader` |
| 58 | C | LOW | lucene | normalises similarity score to probability | — | — | — | — | — | — | — | — | `SimilarityBase` |
| 59 | C | MED | lucene | deprecated use of direct byte buffer for f | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `DirectIODirectory` |
| 60 | C | HIGH | lucene | default merge policy for tiered segment me | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `TieredMergePolicy` |
| 61 | C | HIGH | lucene | collector that tracks top scoring document | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `TopScoreDocCollector` |
| 62 | C | MED | lucene | encodes token positions for phrase matchin | **1** | **1** | — | 5 | **1** | **1** | 4 | 4 | `PhrasePositions` |
| 63 | C | HIGH | lucene | reads stored fields for a given document | 4 | 4 | — | 5 | 7 | 10 | 3 | 4 | `StoredFieldVisitor` |
| 64 | C | MED | lucene | returns true if the term exists in the ind | **1** | **1** | — | 4 | **1** | **1** | **1** | 2 | `DirectoryReader` |
| 65 | C | MED | lucene | applies boost to matching documents | 8 | 8 | 2 | 3 | 6 | 7 | 5 | 4 | `BoostQuery` |
| 66 | C | MED | solr | handles incoming update requests for SolrC | — | — | — | — | — | — | — | — | `DistributedUpdateProcesso` |
| 67 | C | LOW | solr | writes index segments to disk on commit | — | — | — | — | — | — | — | — | `SolrCore` |
| 68 | C | HIGH | solr | evicts least recently used entries from qu | **1** | **1** | **1** | **1** | 2 | 2 | **1** | **1** | `ConcurrentLRUCache` |
| 69 | C | HIGH | solr | recovers replica state from transaction lo | **1** | **1** | 4 | **1** | 2 | 2 | **1** | **1** | `UpdateLog` |
| 70 | C | LOW | solr | forwards query to appropriate shard and me | — | — | — | — | — | — | — | — | `SearchHandler` |
| 71 | C | HIGH | solr | registers a new collection in ZooKeeper st | — | — | 6 | — | — | — | — | — | `CreateCollectionCmd` |
| 72 | C | HIGH | solr | parses field analysis request and returns  | **1** | 2 | — | 3 | 4 | 4 | 9 | **1** | `FieldAnalysisRequestHandl` |
| 73 | C | MED | solr | validates document schema before indexing | 7 | 7 | — | 8 | **1** | **1** | — | 4 | `DocumentBuilder` |
| 74 | C | MED | solr | applies machine learning model score to do | — | 10 | **1** | 4 | 8 | 5 | 9 | 6 | `LTRScoringQuery` |
| 75 | C | HIGH | solr | builds inverted index from document tokens | **1** | **1** | — | 3 | 3 | 3 | 3 | 2 | `DocumentBuilder` |
| 76 | C | HIGH | vespa | evaluates ranking expression for a documen | **1** | **1** | 2 | **1** | **1** | 2 | **1** | **1** | `RankingExpression` |
| 77 | C | MED | vespa | dispatches query to content nodes | — | — | — | — | 10 | — | — | — | `Dispatcher` |
| 78 | C | MED | vespa | updates document in persistent storage | **1** | **1** | 2 | 5 | **1** | **1** | 3 | 3 | `DocumentUpdate` |
| 79 | C | LOW | vespa | flushes memory index to disk | — | — | — | — | — | — | — | — | `DiskIndexFlushConfig` |
| 80 | C | MED | vespa | iterates over documents in a bucket | — | — | — | — | — | — | — | — | `VisitorParameters` |
| 81 | C | MED | vespa | resolves field path in document type | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 3 | `FieldPath` |
| 82 | C | MED | vespa | converts query to Vespa internal represent | **1** | **1** | — | 2 | **1** | 3 | 6 | 7 | `TextualQueryRepresentatio` |
| 83 | C | HIGH | vespa | computes dot product between query and doc | 2 | 2 | **1** | **1** | **1** | **1** | 2 | 3 | `DotProductItem` |
| 84 | C | HIGH | vespa | applies document processing chain | **1** | **1** | 6 | **1** | **1** | **1** | **1** | 2 | `DocumentProcessing` |
| 85 | C | HIGH | pharos | scores result with cross-encoder model | 2 | **1** | 2 | **1** | **1** | **1** | 2 | 2 | `CrossEncoderProvider` |
| 86 | D | MED | lucene | find the code that prevents two writers fr | — | — | — | — | — | — | — | — | `NativeFSLockFactory` |
| 87 | D | MED | lucene | find code that decides which segments are  | — | — | 6 | 8 | — | — | — | — | `TieredMergePolicy` |
| 88 | D | MED | lucene | find code that turns a stream of tokens in | **1** | **1** | — | **1** | 2 | 2 | 8 | 8 | `KnnFloatVectorField` |
| 89 | D | LOW | lucene | find code that skips over non-matching doc | — | — | — | — | — | — | — | — | `DocIdSetIterator` |
| 90 | D | MED | lucene | find code that keeps the most relevant pas | — | — | — | — | — | — | — | — | `PassageScorer` |
| 91 | D | MED | lucene | find code that retries failed segment merg | **1** | **1** | — | — | 7 | 7 | **1** | 4 | `MergePolicy` |
| 92 | D | HIGH | lucene | find code that estimates the memory usage  | **1** | **1** | **1** | **1** | **1** | **1** | 5 | **1** | `RamUsageEstimator` |
| 93 | D | HIGH | lucene | find code that makes sure deleted document | — | — | — | — | — | — | — | — | `Bits` |
| 94 | D | MED | lucene | find code that reorders search results by  | — | — | — | — | — | — | — | — | `Sort` |
| 95 | D | HIGH | lucene | find code that assigns unique ids to facet | — | — | — | — | — | — | — | — | `DirectoryTaxonomyWriter` |
| 96 | D | MED | solr | find code that synchronises collection sta | 4 | 4 | 4 | 3 | **1** | **1** | 2 | **1** | `ZkStateReader` |
| 97 | D | HIGH | solr | find code that routes a document to the co | 2 | 2 | — | — | 3 | 3 | **1** | **1** | `CloudSolrClient` |
| 98 | D | MED | solr | find code that deduplicates results by a f | — | — | — | — | — | — | — | — | `CollapsingQParserPlugin` |
| 99 | D | HIGH | solr | find code that warms a new searcher before | 3 | 3 | — | 7 | 8 | 8 | 6 | 9 | `SolrEventListener` |
| 100 | D | MED | solr | find code that applies custom boosting to  | — | — | — | — | — | — | — | — | `QueryElevationComponent` |
| 101 | D | MED | solr | find code that streams large result sets w | — | — | — | — | — | — | — | — | `ExportHandler` |
| 102 | D | LOW | solr | find code that manages leader election whe | 2 | 2 | 8 | **1** | **1** | **1** | 3 | **1** | `LeaderElector` |
| 103 | D | MED | solr | find code that validates an update before  | — | — | — | — | 2 | 2 | — | 5 | `UpdateRequestProcessor` |
| 104 | D | HIGH | solr | find code that computes the score contribu | **1** | **1** | 8 | 2 | **1** | **1** | **1** | **1** | `PhrasesIdentificationComp` |
| 105 | D | HIGH | solr | find code that selects the best spell corr | **1** | **1** | — | **1** | **1** | **1** | 3 | 3 | `SpellCheckCorrection` |
| 106 | D | MED | vespa | find code that picks which content node se | 5 | 5 | — | — | — | — | 7 | — | `LoadBalancer` |
| 107 | D | LOW | vespa | find code that computes nearness between q | **1** | **1** | — | **1** | **1** | **1** | 3 | 3 | `FieldMatchMetricsComputer` |
| 108 | D | MED | vespa | find code that aggregates counts across do | — | 7 | **1** | **1** | 2 | 2 | 2 | 4 | `ExpressionCountAggregatio` |
| 109 | D | HIGH | vespa | find code that applies a tensor operation  | **1** | **1** | 8 | **1** | 3 | 3 | **1** | **1** | `RankFeatures` |
| 110 | D | MED | vespa | find code that rewrites a query before exe | — | — | 5 | — | 7 | 10 | 2 | 9 | `RewriterUtils` |
| 111 | D | MED | vespa | find code that assigns documents to storag | **1** | **1** | — | — | 3 | 3 | 3 | **1** | `StorageCluster` |
| 112 | D | MED | vespa | find code that merges results from multipl | 2 | 3 | — | — | 5 | 4 | 2 | 3 | `VespaModel` |
| 113 | D | HIGH | vespa | find code that validates a ranking express | **1** | **1** | — | 2 | **1** | **1** | 6 | 5 | `RankingExpression` |
| 114 | D | HIGH | vespa | find code that serializes a document to bi | 2 | 2 | 5 | 3 | 3 | 3 | **1** | **1** | `BinaryFormat` |
| 115 | D | MED | vespa | find code that loads a machine learning mo | — | — | — | — | — | — | — | — | `TritonOnnxRuntime` |
| 116 | D | HIGH | pharos | find code that picks the best result from  | 2 | **1** | **1** | **1** | **1** | **1** | 3 | 2 | `BordaMerger` |
| 117 | D | HIGH | pharos | find code that decides how much to boost a | 3 | **1** | 3 | 2 | **1** | **1** | 2 | 2 | `SearchEngine` |
| 118 | D | HIGH | pharos | find code that splits a compound query int | 2 | 2 | 2 | 2 | **1** | **1** | **1** | **1** | `SearchEngine` |
| 119 | D | HIGH | pharos | find code that converts a method body to a | 6 | 6 | — | 2 | — | — | — | — | `PassageBuilder` |
| 120 | D | HIGH | pharos | find code that connects methods across pro | 10 | — | — | 3 | 3 | 3 | — | 2 | `CrossProjectLinker` |
| 121 | D | HIGH | pharos | find code that skips re-indexing files tha | **1** | 2 | 2 | 3 | 2 | 3 | 2 | **1** | `FileStateTracker` |
| 122 | D | HIGH | pharos | find code that registers all CLI subcomman | — | 7 | **1** | **1** | 9 | — | **1** | — | `CodeSearchCommand` |
| 123 | D | HIGH | pharos | find code that maps a parsed method to a L | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `DocumentMapper` |
| 124 | D | HIGH | pharos | find code that expands search results with | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `SearchEngine` |
| 125 | D | HIGH | pharos | find code that detects whether a query is  | — | 7 | **1** | **1** | **1** | **1** | **1** | **1** | `DefaultQueryClassifier` |
| 126 | E | HIGH | lucene | handle corrupt index exception on startup | **1** | **1** | 2 | **1** | **1** | **1** | 2 | 2 | `CorruptIndexException` |
| 127 | E | HIGH | lucene | close all resources when index writer is s | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `IndexWriter` |
| 128 | E | MED | lucene | recover from merge exception and continue | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `MergePolicy` |
| 129 | E | MED | lucene | handle out of memory error during indexing | — | — | — | — | — | — | — | — | `IndexWriter` |
| 130 | E | HIGH | lucene | lock factory acquire release file lock | 3 | 3 | 5 | 5 | **1** | **1** | 4 | 5 | `NativeFSLockFactory` |
| 131 | E | HIGH | solr | SolrCore close hook cleanup | **1** | **1** | **1** | **1** | **1** | **1** | 2 | 2 | `CloseHook` |
| 132 | E | LOW | solr | handle network partition in ZooKeeper | — | — | 4 | — | — | — | — | — | `SolrZkClient` |
| 133 | E | MED | solr | rollback incomplete transaction on error | 3 | 3 | — | — | — | 9 | 9 | 9 | `UpdateLog` |
| 134 | E | HIGH | solr | replica recovery exception handling | 2 | 2 | **1** | **1** | **1** | **1** | 2 | **1** | `RecoveryStrategy` |
| 135 | E | LOW | solr | graceful shutdown of distributed search | — | — | — | — | — | — | — | — | `SolrCore` |
| 136 | E | MED | vespa | handle failed document processing graceful | **1** | **1** | — | — | 7 | 5 | 4 | 5 | `DocumentProcessing` |
| 137 | E | HIGH | vespa | retry failed feed operation | 2 | 2 | — | **1** | **1** | **1** | 2 | 2 | `RetryFailedConsumer` |
| 138 | E | MED | vespa | node crash recovery persistence | **1** | **1** | 2 | 2 | 2 | 2 | **1** | **1** | `StateChangeHandler` |
| 139 | E | MED | vespa | handle timeout in query execution | **1** | **1** | — | 2 | 5 | 4 | 8 | 8 | `AsyncExecution` |
| 140 | E | LOW | vespa | container startup initialization sequence | — | — | — | — | — | — | — | — | `Container` |
| 141 | E | MED | pharos | close Lucene index reader after search | **1** | **1** | **1** | **1** | **1** | **1** | 3 | **1** | `LuceneIndexer` |
| 142 | E | HIGH | pharos | handle embedding model load failure | 2 | 2 | **1** | **1** | **1** | 2 | **1** | **1** | `DjlEmbeddingProvider` |
| 143 | E | MED | pharos | daemon startup health check | 3 | 3 | 2 | 2 | 2 | **1** | 2 | 3 | `WebServer` |
| 144 | E | HIGH | pharos | handle cross-encoder scoring exception | 4 | 4 | 6 | 2 | 2 | 2 | 8 | 9 | `CrossEncoderTranslator` |
| 145 | E | MED | pharos | recover from corrupt index state | **1** | **1** | — | **1** | 2 | 3 | **1** | **1** | `ProjectIndexManager` |
| 146 | F | HIGH | lucene | set maximum merge segment size megabytes | — | 2 | 2 | 5 | 2 | 2 | 2 | 2 | `TieredMergePolicy` |
| 147 | F | MED | lucene | configure buffer size for writing postings | 2 | 2 | — | 2 | 3 | 3 | 3 | 2 | `BufferedIndexInput` |
| 148 | F | HIGH | lucene | set maximum number of boolean clauses | 2 | **1** | — | 3 | 2 | 2 | **1** | **1** | `IndexSearcher` |
| 149 | F | MED | lucene | tune HNSW beam width for recall accuracy t | — | — | — | — | — | — | — | — | `HnswGraphBuilder` |
| 150 | F | MED | solr | configure cache size for filter queries | **1** | **1** | — | 4 | **1** | **1** | **1** | **1** | `SolrIndexSearcher` |
| 151 | F | HIGH | solr | set number of shards for new collection | 3 | 3 | **1** | 2 | **1** | **1** | **1** | **1** | `CollectionAdminRequest` |
| 152 | F | HIGH | solr | configure maximum connections in HTTP clie | — | **1** | **1** | 5 | 10 | **1** | **1** | **1** | `HttpSolrClientBuilderBase` |
| 153 | F | MED | solr | tune replica sync timeout | 4 | **1** | — | 2 | — | 3 | **1** | **1** | `PeerSync` |
| 154 | F | HIGH | solr | set soft commit interval for near real tim | 6 | 6 | — | — | — | — | — | — | `DirectUpdateHandler2` |
| 155 | F | LOW | vespa | configure ranking profile weights | **1** | 2 | **1** | **1** | 4 | 4 | **1** | **1** | `RankProfile` |
| 156 | F | HIGH | vespa | set thread pool size for query execution | 2 | 3 | — | — | 5 | 5 | 3 | 3 | `ContainerThreadPool` |
| 157 | F | LOW | vespa | configure document expiry age | — | — | — | — | — | — | — | — | `MemoryFileSystem` |
| 158 | F | HIGH | vespa | tune redundancy for content cluster | **1** | 2 | 6 | 2 | **1** | 2 | **1** | **1** | `RedundancyBuilder` |
| 159 | F | HIGH | vespa | set maximum query timeout | **1** | 5 | — | **1** | **1** | **1** | 4 | 5 | `Query` |
| 160 | F | HIGH | pharos | configure embedding model URL | **1** | 2 | 2 | 2 | **1** | **1** | 2 | 2 | `IndexConfig` |
| 161 | F | LOW | pharos | set heap size for JVM indexing | — | — | **1** | 2 | 9 | — | 3 | — | `IndexConfig` |
| 162 | F | HIGH | pharos | configure parse thread count | **1** | **1** | 2 | **1** | **1** | **1** | **1** | **1** | `IndexConfig` |
| 163 | F | HIGH | pharos | set HNSW max connections per node | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `IndexConfig` |
| 164 | F | HIGH | pharos | configure search result limit | — | **1** | 2 | — | 8 | 7 | 2 | 3 | `SearchRequest` |
| 165 | F | MED | pharos | enable cross encoder reranking | — | — | — | 6 | 3 | 2 | — | 3 | `IndexConfig` |
| 166 | G | MED | lucene | interface for custom document similarity f | — | **1** | **1** | **1** | 4 | 4 | 2 | 2 | `Similarity` |
| 167 | G | HIGH | lucene | interface for collecting search results | — | — | — | — | — | — | — | — | `Collector` |
| 168 | G | HIGH | lucene | interface for reading index segments | — | — | — | — | — | — | — | — | `LeafReader` |
| 169 | G | MED | lucene | interface for tokenizing text | — | — | — | — | — | — | — | — | `Tokenizer` |
| 170 | G | HIGH | lucene | interface for merging index segments | 4 | **1** | — | **1** | — | 9 | 2 | 8 | `MergePolicy` |
| 171 | G | HIGH | solr | interface for Solr request processing | **1** | **1** | 6 | **1** | **1** | **1** | 7 | 4 | `SolrRequestHandler` |
| 172 | G | HIGH | solr | interface for custom update processing | 4 | 4 | 5 | — | 7 | — | 3 | — | `UpdateRequestProcessor` |
| 173 | G | LOW | solr | interface for search result ranking | — | — | — | — | — | — | — | — | `ReRankQParserPlugin` |
| 174 | G | HIGH | solr | interface for cache implementation | 3 | **1** | 2 | 2 | **1** | **1** | 4 | 4 | `SolrCache` |
| 175 | G | MED | vespa | interface for Vespa document operations | 7 | — | — | — | 7 | 3 | 8 | 8 | `Processing` |
| 176 | G | LOW | vespa | interface for ranking feature computation | — | — | — | 8 | — | — | — | — | `RankingExpression` |
| 177 | G | MED | vespa | interface for query processing chain | — | — | — | 4 | — | — | — | — | `SearchChainDispatcherSear` |
| 178 | G | HIGH | pharos | interface for embedding generation | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `EmbeddingProvider` |
| 179 | G | HIGH | pharos | interface for search pipeline stage retrie | 5 | **1** | **1** | **1** | 2 | **1** | 2 | 2 | `RetrievalStage` |
| 180 | G | HIGH | pharos | interface for cross-encoder scoring | 2 | 3 | 2 | **1** | 3 | 3 | **1** | **1** | `CrossEncoder` |
| 181 | H | LOW | lucene | factory pattern dependency injection comma | — | — | — | — | — | — | — | — | `TokenizerFactory` |
| 182 | H | HIGH | lucene | visitor pattern document field traversal | **1** | **1** | 5 | 2 | 3 | 3 | 2 | 2 | `DocumentStoredFieldVisito` |
| 183 | H | MED | lucene | decorator pattern similarity score modific | — | — | — | — | — | — | — | — | `PerFieldSimilarityWrapper` |
| 184 | H | MED | lucene | strategy pattern query execution plan | 3 | 3 | — | 3 | 6 | 4 | 2 | 3 | `KnnSearchStrategy` |
| 185 | H | HIGH | lucene | observer pattern index change notification | **1** | **1** | **1** | **1** | **1** | **1** | **1** | 2 | `IndexObserver` |
| 186 | H | HIGH | solr | pipeline pattern search processing chain | 2 | 2 | — | 3 | 4 | 4 | **1** | **1** | `UpdateRequestProcessorCha` |
| 187 | H | HIGH | solr | circuit breaker pattern distributed query | 2 | 2 | 2 | 2 | 4 | 4 | 3 | 2 | `CircuitBreaker` |
| 188 | H | LOW | solr | retry pattern with backoff for replication | — | — | — | — | — | — | — | — | `PeerSync` |
| 189 | H | LOW | solr | bulkhead pattern request isolation | — | — | — | — | — | — | — | — | `CircuitBreaker` |
| 190 | H | HIGH | solr | event sourcing transaction log document up | 2 | 2 | 2 | 2 | 2 | 3 | 6 | 6 | `TransactionLog` |
| 191 | H | HIGH | vespa | chain of responsibility document processin | **1** | **1** | — | **1** | 3 | **1** | **1** | **1** | `DocumentProcessing` |
| 192 | H | LOW | vespa | template method ranking computation | — | — | — | — | — | — | — | — | `Searcher` |
| 193 | H | HIGH | vespa | composite pattern query expression tree | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `QueryTree` |
| 194 | H | MED | vespa | adapter pattern ONNX model integration | 2 | 2 | **1** | **1** | **1** | 2 | 3 | 5 | `OnnxModelInfo` |
| 195 | H | LOW | vespa | builder pattern ranking profile configurat | — | — | — | **1** | 2 | 2 | — | 3 | `RankProfile` |
| 196 | H | HIGH | pharos | pipeline stage retrieval merge rerank | **1** | **1** | 2 | **1** | 9 | 9 | **1** | **1** | `SearchPipeline` |
| 197 | H | HIGH | pharos | adapter pattern cross encoder provider | **1** | **1** | **1** | **1** | **1** | **1** | **1** | 2 | `CrossEncoderProviderAdapt` |
| 198 | H | HIGH | pharos | builder pattern search pipeline constructi | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `SearchPipeline` |
| 199 | H | MED | pharos | strategy pattern search type selection | **1** | **1** | — | 3 | 5 | 5 | 2 | **1** | `HybridSearchStrategy` |
| 200 | H | HIGH | pharos | facade pattern search engine orchestration | **1** | **1** | **1** | **1** | **1** | **1** | **1** | **1** | `SearchEngine` |

