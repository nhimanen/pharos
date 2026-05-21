#!/usr/bin/env python3
"""
200-query pipeline comparison with ground truth labels.
Metrics: Precision@1, Precision@3, MRR@10 per pipeline per category.

Flags:
  --goldenset       also run goldenset evaluation after the main benchmark
  --goldenset-only  run only goldenset evaluation (skip 200-query benchmark)
"""
import argparse, hashlib, json, os, sys, urllib.parse, urllib.request
from collections import defaultdict

BASE_URL = "http://localhost:7171"
LIMIT = 10
# Cache lives next to this script so results persist across sessions
CACHE_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_cache.json")
GOLDENSET_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "goldenset.jsonl")
PIPELINES = ["keyword", "auto", "vector", "unified", "hybrid", "hybrid-diverse", "hybrid-reranked", "hybrid-reranked-diverse"]

CAT_LABELS = {
    "A": "Exact name (single term)",
    "B": "Named-concept phrase",
    "C": "Javadoc / documentation",
    "D": "Behavioral / intent",
    "E": "Error / lifecycle",
    "F": "Config / tuning",
    "G": "Interface / contract",
    "H": "Cross-cutting / pattern",
}

# (idx, query, project, category, ground_truth_id, confidence)
QUERIES = [
    (1,  "HnswGraphBuilder", "lucene", "A", "lucene:org.apache.lucene.util.hnsw.HnswGraphBuilder", "HIGH"),
    (2,  "TieredMergePolicy", "lucene", "A", "lucene:org.apache.lucene.index.TieredMergePolicy", "HIGH"),
    (3,  "BooleanQuery", "lucene", "A", "lucene:org.apache.lucene.search.BooleanQuery", "HIGH"),
    (4,  "DirectoryReader", "lucene", "A", "lucene:org.apache.lucene.index.DirectoryReader", "HIGH"),
    (5,  "UnifiedHighlighter", "lucene", "A", "lucene:org.apache.lucene.search.uhighlight.UnifiedHighlighter", "HIGH"),
    (6,  "IndexWriter", "lucene", "A", "lucene:org.apache.lucene.index.IndexWriter", "HIGH"),
    (7,  "PerFieldSimilarityWrapper", "lucene", "A", "lucene:org.apache.lucene.search.similarities.PerFieldSimilarityWrapper", "HIGH"),
    (8,  "KnnFloatVectorQuery", "lucene", "A", "lucene:org.apache.lucene.search.KnnFloatVectorQuery", "HIGH"),
    (9,  "CollapsingQParser", "solr", "A", "solr:org.apache.solr.search.CollapsingQParserPlugin.CollapsingQParser", "HIGH"),
    (10, "SolrCloud", "solr", "A", "solr:org.apache.solr.cloud.ZkController", "HIGH"),
    (11, "UpdateLog", "solr", "A", "solr:org.apache.solr.update.UpdateLog", "HIGH"),
    (12, "SpellCheckComponent", "solr", "A", "solr:org.apache.solr.handler.component.SpellCheckComponent", "HIGH"),
    (13, "StreamExpression", "solr", "A", "solr:org.apache.solr.client.solrj.io.stream.expr.StreamExpression", "HIGH"),
    (14, "CaffeineCache", "solr", "A", "solr:org.apache.solr.search.CaffeineCache", "HIGH"),
    (15, "Overseer", "solr", "A", "solr:org.apache.solr.cloud.Overseer", "HIGH"),
    (16, "TritonOnnxEvaluator", "vespa", "A", "vespa:ai.vespa.triton.TritonOnnxEvaluator", "MED"),
    (17, "DocumentTypeManager", "vespa", "A", "vespa:com.yahoo.document.DocumentTypeManager", "MED"),
    (18, "SearchChainDispatcher", "vespa", "A", "vespa:com.yahoo.search.query.rewrite.SearchChainDispatcherSearcher", "MED"),
    (19, "VisitorParameters", "vespa", "A", "vespa:com.yahoo.documentapi.VisitorParameters", "HIGH"),
    (20, "ExpressionConverter", "vespa", "A", "vespa:com.yahoo.vespa.indexinglanguage.ExpressionConverter", "HIGH"),
    (21, "BordaMerger", "pharos", "A", "pharos:com.pharos.search.pipeline.BordaMerger", "HIGH"),
    (22, "CrossEncoderProvider", "pharos", "A", "pharos:com.pharos.embedding.CrossEncoderProvider", "HIGH"),
    (23, "CallGraphBuilder", "pharos", "A", "pharos:com.pharos.graph.CallGraphBuilder", "HIGH"),
    (24, "LuceneIndexer", "pharos", "A", "pharos:com.pharos.indexer.LuceneIndexer", "HIGH"),
    (25, "SearchEngine", "pharos", "A", "pharos:com.pharos.search.SearchEngine", "HIGH"),
    (26, "FileStateTracker", "pharos", "A", "pharos:com.pharos.indexer.FileStateTracker", "HIGH"),
    (27, "DocumentMapper", "pharos", "A", "pharos:com.pharos.indexer.DocumentMapper", "HIGH"),
    (28, "McpToolRegistry", "pharos", "A", "pharos:com.pharos.mcp.McpToolRegistry", "HIGH"),
    (29, "ProjectRegistry", "pharos", "A", "pharos:com.pharos.config.ProjectRegistry", "HIGH"),
    (30, "SynonymProvider", "pharos", "A", "pharos:com.pharos.analysis.SynonymProvider", "HIGH"),
    # B: Named-concept phrase
    (31, "HNSW graph neighbor candidate exploration", "lucene", "B", "lucene:org.apache.lucene.util.hnsw.HnswGraphSearcher", "HIGH"),
    (32, "inverted index posting list iterator", "lucene", "B", "lucene:org.apache.lucene.codecs.lucene104.PostingIndexInput", "MED"),
    (33, "segment merge candidate selection", "lucene", "B", "lucene:org.apache.lucene.index.TieredMergePolicy", "HIGH"),
    (34, "query parser boolean clause combination", "lucene", "B", "lucene:org.apache.lucene.queryparser.classic.QueryParser", "HIGH"),
    (35, "facet taxonomy ordinal mapping", "lucene", "B", "lucene:org.apache.lucene.facet.taxonomy.OrdinalMappingLeafReader", "HIGH"),
    (36, "token filter chain pipeline", "lucene", "B", "lucene:org.apache.lucene.analysis.custom.CustomAnalyzer", "HIGH"),
    (37, "BM25 field boost weight computation", "lucene", "B", "lucene:org.apache.lucene.search.similarities.BM25Similarity", "MED"),
    (38, "SolrCloud replica state transition", "solr", "B", "solr:org.apache.solr.common.cloud.ZkStateReader", "MED"),
    (39, "update chain processor document", "solr", "B", "solr:org.apache.solr.update.processor.UpdateRequestProcessorChain", "HIGH"),
    (40, "distributed search shard merge collector", "solr", "B", "solr:org.apache.solr.handler.component.SearchHandler", "MED"),
    (41, "JSON facet aggregation bucket", "solr", "B", "solr:org.apache.solr.client.solrj.response.json.BucketJsonFacet", "HIGH"),
    (42, "transaction log apply replay", "solr", "B", "solr:org.apache.solr.update.UpdateLog", "HIGH"),
    (43, "overseer state machine operation", "solr", "B", "solr:org.apache.solr.cloud.Overseer", "HIGH"),
    (44, "SolrJ HTTP request response", "solr", "B", "solr:org.apache.solr.client.solrj.apache.HttpSolrClient", "HIGH"),
    (45, "ranking expression tensor computation", "vespa", "B", "vespa:com.yahoo.searchlib.rankingexpression.RankingExpression", "MED"),
    (46, "document feed put remove handler", "vespa", "B", "vespa:com.yahoo.document.restapi.resource.DocumentV1ApiHandler", "HIGH"),
    (47, "bucket distributor content cluster", "vespa", "B", "vespa:com.yahoo.vespa.model.content.DistributorCluster", "HIGH"),
    (48, "grouping aggregation expression result", "vespa", "B", "vespa:com.yahoo.searchlib.aggregation.ExpressionCountAggregationResult", "HIGH"),
    (49, "YQL query parsing select where", "vespa", "B", "vespa:com.yahoo.search.Query", "MED"),
    (50, "ONNX model inference runtime", "vespa", "B", "vespa:ai.vespa.triton.TritonOnnxRuntime", "HIGH"),
    (51, "BordaMerger Borda count agreement bonus", "pharos", "B", "pharos:com.pharos.search.pipeline.BordaMerger", "HIGH"),
    (52, "cross-project link resolution", "pharos", "B", "pharos:com.pharos.graph.CrossProjectLinker", "HIGH"),
    (53, "incremental index file state", "pharos", "B", "pharos:com.pharos.indexer.FileStateTracker", "HIGH"),
    (54, "MCP server tool registration", "pharos", "B", "pharos:com.pharos.mcp.McpToolRegistry", "HIGH"),
    (55, "query hint project language modifier", "pharos", "B", "pharos:com.pharos.search.SearchEngine", "HIGH"),
    # C: Javadoc / documentation phrasing
    (56, "returns the number of deleted documents in a segment", "lucene", "C", "lucene:org.apache.lucene.util.LiveDocs", "HIGH"),
    (57, "opens a reader on the most recent commit point", "lucene", "C", "lucene:org.apache.lucene.index.DirectoryReader", "MED"),
    (58, "normalises similarity score to probability between zero and one", "lucene", "C", "lucene:org.apache.lucene.search.similarities.SimilarityBase", "LOW"),
    (59, "deprecated use of direct byte buffer for file access", "lucene", "C", "lucene:org.apache.lucene.misc.store.DirectIODirectory", "MED"),
    (60, "default merge policy for tiered segment merging", "lucene", "C", "lucene:org.apache.lucene.index.TieredMergePolicy", "HIGH"),
    (61, "collector that tracks top scoring documents", "lucene", "C", "lucene:org.apache.lucene.search.TopScoreDocCollector", "HIGH"),
    (62, "encodes token positions for phrase matching", "lucene", "C", "lucene:org.apache.lucene.search.PhrasePositions", "MED"),
    (63, "reads stored fields for a given document", "lucene", "C", "lucene:org.apache.lucene.index.StoredFieldVisitor", "HIGH"),
    (64, "returns true if the term exists in the index", "lucene", "C", "lucene:org.apache.lucene.index.DirectoryReader", "MED"),
    (65, "applies boost to matching documents", "lucene", "C", "lucene:org.apache.lucene.search.BoostQuery", "MED"),
    (66, "handles incoming update requests for SolrCloud", "solr", "C", "solr:org.apache.solr.update.processor.DistributedUpdateProcessor", "MED"),
    (67, "writes index segments to disk on commit", "solr", "C", "solr:org.apache.solr.core.SolrCore", "LOW"),
    (68, "evicts least recently used entries from query cache", "solr", "C", "solr:org.apache.solr.util.ConcurrentLRUCache", "HIGH"),
    (69, "recovers replica state from transaction log", "solr", "C", "solr:org.apache.solr.update.UpdateLog", "HIGH"),
    (70, "forwards query to appropriate shard and merges results", "solr", "C", "solr:org.apache.solr.handler.component.SearchHandler", "LOW"),
    (71, "registers a new collection in ZooKeeper state", "solr", "C", "solr:org.apache.solr.cloud.api.collections.CreateCollectionCmd", "HIGH"),
    (72, "parses field analysis request and returns token breakdown", "solr", "C", "solr:org.apache.solr.handler.FieldAnalysisRequestHandler", "HIGH"),
    (73, "validates document schema before indexing", "solr", "C", "solr:org.apache.solr.update.DocumentBuilder", "MED"),
    (74, "applies machine learning model score to document", "solr", "C", "solr:org.apache.solr.ltr.LTRScoringQuery", "MED"),
    (75, "builds inverted index from document tokens", "solr", "C", "solr:org.apache.solr.update.DocumentBuilder", "HIGH"),
    (76, "evaluates ranking expression for a document", "vespa", "C", "vespa:com.yahoo.searchlib.rankingexpression.RankingExpression", "HIGH"),
    (77, "dispatches query to content nodes", "vespa", "C", "vespa:com.yahoo.search.dispatch.Dispatcher", "MED"),
    (78, "updates document in persistent storage", "vespa", "C", "vespa:com.yahoo.document.DocumentUpdate", "MED"),
    (79, "flushes memory index to disk", "vespa", "C", "vespa:com.yahoo.vespa.searchlib.index.DiskIndexFlushConfig", "LOW"),
    (80, "iterates over documents in a bucket", "vespa", "C", "vespa:com.yahoo.documentapi.VisitorParameters", "MED"),
    (81, "resolves field path in document type", "vespa", "C", "vespa:com.yahoo.document.FieldPath", "MED"),
    (82, "converts query to Vespa internal representation", "vespa", "C", "vespa:com.yahoo.prelude.query.textualrepresentation.TextualQueryRepresentation", "MED"),
    (83, "computes dot product between query and document vectors", "vespa", "C", "vespa:com.yahoo.prelude.query.DotProductItem", "HIGH"),
    (84, "applies document processing chain", "vespa", "C", "vespa:com.yahoo.application.container.DocumentProcessing", "HIGH"),
    (85, "scores result with cross-encoder model", "pharos", "C", "pharos:com.pharos.embedding.CrossEncoderProvider", "HIGH"),
    # D: Behavioral / intent
    (86,  "find the code that prevents two writers from accessing the same directory simultaneously", "lucene", "D", "lucene:org.apache.lucene.store.NativeFSLockFactory", "MED"),
    (87,  "find code that decides which segments are worth merging right now", "lucene", "D", "lucene:org.apache.lucene.index.TieredMergePolicy", "MED"),
    (88,  "find code that turns a stream of tokens into a fixed-size float vector", "lucene", "D", "lucene:org.apache.lucene.document.KnnFloatVectorField", "MED"),
    (89,  "find code that skips over non-matching documents efficiently", "lucene", "D", "lucene:org.apache.lucene.search.DocIdSetIterator", "LOW"),
    (90,  "find code that keeps the most relevant passages for highlighting", "lucene", "D", "lucene:org.apache.lucene.search.uhighlight.PassageScorer", "MED"),
    (91,  "find code that retries failed segment merges", "lucene", "D", "lucene:org.apache.lucene.index.MergePolicy", "MED"),
    (92,  "find code that estimates the memory usage of a query", "lucene", "D", "lucene:org.apache.lucene.util.RamUsageEstimator", "HIGH"),
    (93,  "find code that makes sure deleted documents do not appear in results", "lucene", "D", "lucene:org.apache.lucene.util.Bits", "HIGH"),
    (94,  "find code that reorders search results by a secondary criterion", "lucene", "D", "lucene:org.apache.lucene.search.Sort", "MED"),
    (95,  "find code that assigns unique ids to facet terms", "lucene", "D", "lucene:org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter", "HIGH"),
    (96,  "find code that synchronises collection state across all nodes", "solr", "D", "solr:org.apache.solr.common.cloud.ZkStateReader", "MED"),
    (97,  "find code that routes a document to the correct shard", "solr", "D", "solr:org.apache.solr.client.solrj.impl.CloudSolrClient", "HIGH"),
    (98,  "find code that deduplicates results by a field value", "solr", "D", "solr:org.apache.solr.search.CollapsingQParserPlugin", "MED"),
    (99,  "find code that warms a new searcher before it goes live", "solr", "D", "solr:org.apache.solr.core.SolrEventListener", "HIGH"),
    (100, "find code that applies custom boosting to specific documents", "solr", "D", "solr:org.apache.solr.search.QueryElevationComponent", "MED"),
    (101, "find code that streams large result sets without loading into memory", "solr", "D", "solr:org.apache.solr.handler.ExportHandler", "MED"),
    (102, "find code that manages leader election when a node fails", "solr", "D", "solr:org.apache.solr.cloud.LeaderElector", "LOW"),
    (103, "find code that validates an update before applying it", "solr", "D", "solr:org.apache.solr.update.processor.UpdateRequestProcessor", "MED"),
    (104, "find code that computes the score contribution of each field", "solr", "D", "solr:org.apache.solr.handler.component.PhrasesIdentificationComponent", "HIGH"),
    (105, "find code that selects the best spell correction candidate", "solr", "D", "solr:org.apache.solr.spelling.SpellCheckCorrection", "HIGH"),
    (106, "find code that picks which content node serves a query", "vespa", "D", "vespa:com.yahoo.search.dispatch.LoadBalancer", "MED"),
    (107, "find code that computes nearness between query terms in a document", "vespa", "D", "vespa:com.yahoo.searchlib.ranking.features.fieldmatch.FieldMatchMetricsComputer", "LOW"),
    (108, "find code that aggregates counts across document groups", "vespa", "D", "vespa:com.yahoo.searchlib.aggregation.ExpressionCountAggregationResult", "MED"),
    (109, "find code that applies a tensor operation to rank features", "vespa", "D", "vespa:com.yahoo.search.query.ranking.RankFeatures", "HIGH"),
    (110, "find code that rewrites a query before execution", "vespa", "D", "vespa:com.yahoo.search.query.rewrite.RewriterUtils", "MED"),
    (111, "find code that assigns documents to storage nodes", "vespa", "D", "vespa:com.yahoo.vespa.model.content.storagecluster.StorageCluster", "MED"),
    (112, "find code that merges results from multiple content clusters", "vespa", "D", "vespa:com.yahoo.vespa.model.VespaModel", "MED"),
    (113, "find code that validates a ranking expression before deployment", "vespa", "D", "vespa:com.yahoo.searchlib.rankingexpression.RankingExpression", "HIGH"),
    (114, "find code that serializes a document to binary format", "vespa", "D", "vespa:com.yahoo.slime.BinaryFormat", "HIGH"),
    (115, "find code that loads a machine learning model for inference", "vespa", "D", "vespa:ai.vespa.triton.TritonOnnxRuntime", "MED"),
    (116, "find code that picks the best result from two lists without duplicates", "pharos", "D", "pharos:com.pharos.search.pipeline.BordaMerger", "HIGH"),
    (117, "find code that decides how much to boost a result from the target project", "pharos", "D", "pharos:com.pharos.search.SearchEngine", "HIGH"),
    (118, "find code that splits a compound query into project and language hints", "pharos", "D", "pharos:com.pharos.search.SearchEngine", "HIGH"),
    (119, "find code that converts a method body to a short passage for scoring", "pharos", "D", "pharos:com.pharos.search.pipeline.PassageBuilder", "HIGH"),
    (120, "find code that connects methods across project boundaries in the call graph", "pharos", "D", "pharos:com.pharos.graph.CrossProjectLinker", "HIGH"),
    (121, "find code that skips re-indexing files that have not changed", "pharos", "D", "pharos:com.pharos.indexer.FileStateTracker", "HIGH"),
    (122, "find code that registers all CLI subcommands in one place", "pharos", "D", "pharos:com.pharos.cli.CodeSearchCommand", "HIGH"),
    (123, "find code that maps a parsed method to a Lucene document", "pharos", "D", "pharos:com.pharos.indexer.DocumentMapper", "HIGH"),
    (124, "find code that expands search results with related callee methods", "pharos", "D", "pharos:com.pharos.search.SearchEngine", "HIGH"),
    (125, "find code that detects whether a query is a class name or natural language", "pharos", "D", "pharos:com.pharos.search.DefaultQueryClassifier", "HIGH"),
    # E: Error / exception / lifecycle
    (126, "handle corrupt index exception on startup", "lucene", "E", "lucene:org.apache.lucene.index.CorruptIndexException", "HIGH"),
    (127, "close all resources when index writer is shut down", "lucene", "E", "lucene:org.apache.lucene.index.IndexWriter", "HIGH"),
    (128, "recover from merge exception and continue", "lucene", "E", "lucene:org.apache.lucene.index.MergePolicy", "MED"),
    (129, "handle out of memory error during indexing", "lucene", "E", "lucene:org.apache.lucene.index.IndexWriter", "MED"),
    (130, "lock factory acquire release file lock", "lucene", "E", "lucene:org.apache.lucene.store.NativeFSLockFactory", "HIGH"),
    (131, "SolrCore close hook cleanup", "solr", "E", "solr:org.apache.solr.core.CloseHook", "HIGH"),
    (132, "handle network partition in ZooKeeper", "solr", "E", "solr:org.apache.solr.common.cloud.SolrZkClient", "LOW"),
    (133, "rollback incomplete transaction on error", "solr", "E", "solr:org.apache.solr.update.UpdateLog", "MED"),
    (134, "replica recovery exception handling", "solr", "E", "solr:org.apache.solr.cloud.RecoveryStrategy", "HIGH"),
    (135, "graceful shutdown of distributed search", "solr", "E", "solr:org.apache.solr.core.SolrCore", "LOW"),
    (136, "handle failed document processing gracefully", "vespa", "E", "vespa:com.yahoo.application.container.DocumentProcessing", "MED"),
    (137, "retry failed feed operation", "vespa", "E", "vespa:ai.vespa.util.http.hc4.retry.RetryFailedConsumer", "HIGH"),
    (138, "node crash recovery persistence", "vespa", "E", "vespa:com.yahoo.vespa.clustercontroller.core.StateChangeHandler", "MED"),
    (139, "handle timeout in query execution", "vespa", "E", "vespa:com.yahoo.search.searchchain.AsyncExecution", "MED"),
    (140, "container startup initialization sequence", "vespa", "E", "vespa:com.yahoo.container.jdisc.Container", "LOW"),
    (141, "close Lucene index reader after search", "pharos", "E", "pharos:com.pharos.indexer.LuceneIndexer", "MED"),
    (142, "handle embedding model load failure", "pharos", "E", "pharos:com.pharos.embedding.DjlEmbeddingProvider", "HIGH"),
    (143, "daemon startup health check", "pharos", "E", "pharos:com.pharos.web.WebServer", "MED"),
    (144, "handle cross-encoder scoring exception", "pharos", "E", "pharos:com.pharos.embedding.CrossEncoderTranslator", "HIGH"),
    (145, "recover from corrupt index state", "pharos", "E", "pharos:com.pharos.indexer.ProjectIndexManager", "MED"),
    # F: Configuration / tuning
    (146, "set maximum merge segment size megabytes", "lucene", "F", "lucene:org.apache.lucene.index.TieredMergePolicy", "HIGH"),
    (147, "configure buffer size for writing postings", "lucene", "F", "lucene:org.apache.lucene.store.BufferedIndexInput", "MED"),
    (148, "set maximum number of boolean clauses", "lucene", "F", "lucene:org.apache.lucene.search.IndexSearcher", "HIGH"),
    (149, "tune HNSW beam width for recall accuracy tradeoff", "lucene", "F", "lucene:org.apache.lucene.util.hnsw.HnswGraphBuilder", "MED"),
    (150, "configure cache size for filter queries", "solr", "F", "solr:org.apache.solr.search.SolrIndexSearcher", "MED"),
    (151, "set number of shards for new collection", "solr", "F", "solr:org.apache.solr.client.solrj.request.CollectionAdminRequest", "HIGH"),
    (152, "configure maximum connections in HTTP client pool", "solr", "F", "solr:org.apache.solr.client.solrj.impl.HttpSolrClientBuilderBase", "HIGH"),
    (153, "tune replica sync timeout", "solr", "F", "solr:org.apache.solr.update.PeerSync", "MED"),
    (154, "set soft commit interval for near real time", "solr", "F", "solr:org.apache.solr.update.DirectUpdateHandler2", "HIGH"),
    (155, "configure ranking profile weights", "vespa", "F", "vespa:com.yahoo.schema.RankProfile", "LOW"),
    (156, "set thread pool size for query execution", "vespa", "F", "vespa:com.yahoo.container.handler.threadpool.ContainerThreadPool", "HIGH"),
    (157, "configure document expiry age", "vespa", "F", "vespa:com.yahoo.vespa.curator.mock.MemoryFileSystem", "LOW"),
    (158, "tune redundancy for content cluster", "vespa", "F", "vespa:com.yahoo.vespa.model.content.cluster.RedundancyBuilder", "HIGH"),
    (159, "set maximum query timeout", "vespa", "F", "vespa:com.yahoo.search.Query", "HIGH"),
    (160, "configure embedding model URL", "pharos", "F", "pharos:com.pharos.config.IndexConfig", "HIGH"),
    (161, "set heap size for JVM indexing", "pharos", "F", "pharos:com.pharos.config.IndexConfig", "LOW"),
    (162, "configure parse thread count", "pharos", "F", "pharos:com.pharos.config.IndexConfig", "HIGH"),
    (163, "set HNSW max connections per node", "pharos", "F", "pharos:com.pharos.config.IndexConfig", "HIGH"),
    (164, "configure search result limit", "pharos", "F", "pharos:com.pharos.search.SearchRequest", "HIGH"),
    (165, "enable cross encoder reranking", "pharos", "F", "pharos:com.pharos.config.IndexConfig", "MED"),
    # G: Interface / contract
    (166, "interface for custom document similarity function", "lucene", "G", "lucene:org.apache.lucene.search.similarities.Similarity", "MED"),
    (167, "interface for collecting search results", "lucene", "G", "lucene:org.apache.lucene.search.Collector", "HIGH"),
    (168, "interface for reading index segments", "lucene", "G", "lucene:org.apache.lucene.index.LeafReader", "HIGH"),
    (169, "interface for tokenizing text", "lucene", "G", "lucene:org.apache.lucene.analysis.Tokenizer", "MED"),
    (170, "interface for merging index segments", "lucene", "G", "lucene:org.apache.lucene.index.MergePolicy", "HIGH"),
    (171, "interface for Solr request processing", "solr", "G", "solr:org.apache.solr.request.SolrRequestHandler", "HIGH"),
    (172, "interface for custom update processing", "solr", "G", "solr:org.apache.solr.update.processor.UpdateRequestProcessor", "HIGH"),
    (173, "interface for search result ranking", "solr", "G", "solr:org.apache.solr.search.ReRankQParserPlugin", "LOW"),
    (174, "interface for cache implementation", "solr", "G", "solr:org.apache.solr.search.SolrCache", "HIGH"),
    (175, "interface for Vespa document operations", "vespa", "G", "vespa:com.yahoo.docproc.Processing", "MED"),
    (176, "interface for ranking feature computation", "vespa", "G", "vespa:com.yahoo.searchlib.rankingexpression.RankingExpression", "LOW"),
    (177, "interface for query processing chain", "vespa", "G", "vespa:com.yahoo.search.query.rewrite.SearchChainDispatcherSearcher", "MED"),
    (178, "interface for embedding generation", "pharos", "G", "pharos:com.pharos.embedding.EmbeddingProvider", "HIGH"),
    (179, "interface for search pipeline stage retrieval", "pharos", "G", "pharos:com.pharos.search.pipeline.RetrievalStage", "HIGH"),
    (180, "interface for cross-encoder scoring", "pharos", "G", "pharos:com.pharos.search.pipeline.CrossEncoder", "HIGH"),
    # H: Cross-cutting / pattern
    (181, "factory pattern dependency injection command", "lucene", "H", "lucene:org.apache.lucene.analysis.TokenizerFactory", "LOW"),
    (182, "visitor pattern document field traversal", "lucene", "H", "lucene:org.apache.lucene.document.DocumentStoredFieldVisitor", "HIGH"),
    (183, "decorator pattern similarity score modification", "lucene", "H", "lucene:org.apache.lucene.search.similarities.PerFieldSimilarityWrapper", "MED"),
    (184, "strategy pattern query execution plan", "lucene", "H", "lucene:org.apache.lucene.search.knn.KnnSearchStrategy", "MED"),
    (185, "observer pattern index change notification", "lucene", "H", "lucene:org.apache.lucene.luke.app.IndexObserver", "HIGH"),
    (186, "pipeline pattern search processing chain", "solr", "H", "solr:org.apache.solr.update.processor.UpdateRequestProcessorChain", "HIGH"),
    (187, "circuit breaker pattern distributed query", "solr", "H", "solr:org.apache.solr.util.circuitbreaker.CircuitBreaker", "HIGH"),
    (188, "retry pattern with backoff for replication", "solr", "H", "solr:org.apache.solr.update.PeerSync", "LOW"),
    (189, "bulkhead pattern request isolation", "solr", "H", "solr:org.apache.solr.util.circuitbreaker.CircuitBreaker", "LOW"),
    (190, "event sourcing transaction log document update", "solr", "H", "solr:org.apache.solr.update.TransactionLog", "HIGH"),
    (191, "chain of responsibility document processing", "vespa", "H", "vespa:com.yahoo.application.container.DocumentProcessing", "HIGH"),
    (192, "template method ranking computation", "vespa", "H", "vespa:com.yahoo.search.Searcher", "LOW"),
    (193, "composite pattern query expression tree", "vespa", "H", "vespa:com.yahoo.search.query.QueryTree", "HIGH"),
    (194, "adapter pattern ONNX model integration", "vespa", "H", "vespa:com.yahoo.vespa.model.ml.OnnxModelInfo", "MED"),
    (195, "builder pattern ranking profile configuration", "vespa", "H", "vespa:com.yahoo.schema.RankProfile", "LOW"),
    (196, "pipeline stage retrieval merge rerank", "pharos", "H", "pharos:com.pharos.search.pipeline.SearchPipeline", "HIGH"),
    (197, "adapter pattern cross encoder provider", "pharos", "H", "pharos:com.pharos.search.pipeline.CrossEncoderProviderAdapter", "HIGH"),
    (198, "builder pattern search pipeline construction", "pharos", "H", "pharos:com.pharos.search.pipeline.SearchPipeline", "HIGH"),
    (199, "strategy pattern search type selection", "pharos", "H", "pharos:com.pharos.search.HybridSearchStrategy", "MED"),
    (200, "facade pattern search engine orchestration", "pharos", "H", "pharos:com.pharos.search.SearchEngine", "HIGH"),
]

assert len(QUERIES) == 200


def api_search(query: str, project: str, pipeline: str) -> list:
    for proj in [project, None]:  # retry without project scope on error
        params = {"q": query, "pipeline": pipeline, "limit": LIMIT}
        if proj:
            params["project"] = proj
        url = f"{BASE_URL}/api/search?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.loads(r.read())
                # unwrap envelope format {"results": [...], "searchMeta": {...}}
                if isinstance(data, dict):
                    data = data.get("results", [])
                # filter to target project if we dropped the scope param
                if proj is None and project:
                    data = [r for r in data if r.get("project") == project]
                return data
        except Exception as e:
            if proj is not None:
                continue
            print(f"  [WARN] {pipeline}/{project} failed for '{query[:40]}': {e}", file=sys.stderr)
            return []
    return []


def hit_rank(results: list, gt_id: str) -> int | None:
    """Return 1-based rank of gt_id in results, or None."""
    for i, r in enumerate(results):
        if r.get("id", "").startswith(gt_id) or gt_id.startswith(r.get("id", "")):
            return i + 1
        # class-level match: gt is class FQN, result is a method of that class
        rid = r.get("id", "")
        if ":" in gt_id and ":" in rid:
            gt_class = gt_id.split(":")[1].split("#")[0]
            r_class  = rid.split(":")[1].split("#")[0]
            if gt_class == r_class:
                return i + 1
    return None


def reciprocal_rank(rank: int | None) -> float:
    return (1.0 / rank) if rank else 0.0


def ndcg(rank: int | None, k: int) -> float:
    """NDCG@k for a single relevant document: 1/log2(rank+1) if rank≤k, else 0."""
    import math
    if rank is None or rank > k:
        return 0.0
    return 1.0 / math.log2(rank + 1)


def _query_fingerprint(query: str, project: str) -> str:
    """Short hash of query+project so stale cache entries are detected."""
    return hashlib.md5(f"{query}|{project}".encode()).hexdigest()[:8]


def _load_cache() -> dict:
    """Load cache from disk. Returns {} on missing/corrupt file."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(cache: dict) -> None:
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)


def load_goldenset() -> list[dict]:
    """Load goldenset.jsonl; last entry per (query,project,pipeline,result_id) wins."""
    if not os.path.exists(GOLDENSET_FILE):
        return []
    seen = {}
    with open(GOLDENSET_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                key = (entry["query"], entry.get("project", ""),
                       entry["pipeline"], entry["result_id"])
                seen[key] = entry
            except Exception:
                pass
    return [e for e in seen.values() if e.get("rating") in ("good", "bad")]


def run_goldenset_eval(cache: dict) -> str:
    """Evaluate pipelines using user-supplied relevance judgments from goldenset.jsonl."""
    entries = load_goldenset()
    if not entries:
        return "## Goldenset Evaluation\n\nNo judgments found in eval/goldenset.jsonl.\n"

    # Group: (query, project, pipeline) → {good: set[id], bad: set[id]}
    groups: dict = defaultdict(lambda: {"good": set(), "bad": set()})
    for e in entries:
        key = (e["query"], e.get("project", ""), e["pipeline"])
        groups[key][e["rating"]].add(e["result_id"])

    def gs_cache_key(query: str, project: str, pipeline: str) -> str:
        h = hashlib.md5(f"gs|{query}|{project}|{pipeline}".encode()).hexdigest()[:8]
        return f"gs:{h}"

    # Fetch / serve from cache
    needed_gs = [(q, p, pl) for (q, p, pl) in groups
                 if gs_cache_key(q, p, pl) not in cache
                 or cache[gs_cache_key(q, p, pl)].get("fp") != _query_fingerprint(q, p)]
    if needed_gs:
        print(f"Fetching {len(needed_gs)} goldenset (query, pipeline) pairs...", file=sys.stderr)
    for (q, p, pl) in needed_gs:
        ck = gs_cache_key(q, p, pl)
        cache[ck] = {"fp": _query_fingerprint(q, p), "results": api_search(q, p, pl)}
    if needed_gs:
        _save_cache(cache)

    per_pipeline: dict = defaultdict(lambda: {"mrr": 0.0, "p1": 0, "p3": 0, "p5": 0, "p10": 0, "n": 0})
    rows = []
    for (query, project, pipeline), judged in sorted(groups.items()):
        ck = gs_cache_key(query, project, pipeline)
        results = cache.get(ck, {}).get("results", [])
        good_ids = judged["good"]
        if not good_ids:
            continue
        first_good = None
        for i, r in enumerate(results):
            rid = r.get("id", "")
            if any(rid.startswith(g) or g.startswith(rid) for g in good_ids):
                first_good = i + 1
                break
        rr = reciprocal_rank(first_good)
        pp = per_pipeline[pipeline]
        pp["n"]   += 1
        pp["mrr"] += rr
        for k, kkey in [(1, "p1"), (3, "p3"), (5, "p5"), (10, "p10")]:
            if first_good and first_good <= k:
                pp[kkey] += 1
        rows.append((query[:40], project, pipeline,
                     first_good or ">10", f"{rr:.2f}",
                     len(good_ids), len(judged["bad"])))

    lines = ["## Goldenset Evaluation (user-rated judgments)\n"]
    n_unique = len({(q, p) for q, p, _ in groups})
    lines.append(f"{len(entries)} ratings across {n_unique} unique (query, project) pairs, "
                 f"{len(per_pipeline)} pipeline(s).\n")

    pls = sorted(per_pipeline)
    lines.append("| Pipeline | n | P@1 | P@3 | P@5 | P@10 | MRR |")
    lines.append("|----------|---|-----|-----|-----|------|-----|")
    for pl in pls:
        s = per_pipeline[pl]
        n = s["n"] or 1
        lines.append(f"| {pl} | {s['n']} | {s['p1']/n:.2f} | {s['p3']/n:.2f} | "
                     f"{s['p5']/n:.2f} | {s['p10']/n:.2f} | {s['mrr']/n:.3f} |")
    lines.append("")

    lines.append("| Query | Project | Pipeline | First-good rank | RR | +rated | −rated |")
    lines.append("|-------|---------|----------|-----------------|----|--------|--------|")
    for row in rows:
        lines.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |")
    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--goldenset",      action="store_true", help="Run goldenset eval after main benchmark")
    parser.add_argument("--goldenset-only", action="store_true", help="Run only goldenset eval")
    args = parser.parse_args()

    cache = _load_cache()

    if args.goldenset_only:
        print(run_goldenset_eval(cache))
        _save_cache(cache)
        return

    # Build set of (idx, pipeline) pairs we still need to fetch
    needed = []
    for idx, query, project, cat, gt_id, conf in QUERIES:
        fp = _query_fingerprint(query, project)
        for pl in PIPELINES:
            key = f"{idx}:{pl}"
            entry = cache.get(key)
            if entry is None or entry.get("fp") != fp:
                needed.append((idx, query, project, pl, fp))

    if needed:
        print(f"Fetching {len(needed)} uncached (query, pipeline) pairs "
              f"({len(needed)} of {len(QUERIES)*len(PIPELINES)} total)...", file=sys.stderr)
        done = 0
        for idx, query, project, pl, fp in needed:
            results = api_search(query, project, pl)
            cache[f"{idx}:{pl}"] = {"fp": fp, "results": results}
            done += 1
            if done % 100 == 0:
                _save_cache(cache)
                print(f"  {done}/{len(needed)}", file=sys.stderr)
        _save_cache(cache)
        print(f"Done. Cache saved to {CACHE_FILE}", file=sys.stderr)
    else:
        print(f"All {len(QUERIES)*len(PIPELINES)} results cached — no API calls needed.", file=sys.stderr)

    # Reconstruct raw dict from cache
    raw: dict[tuple, list] = {}
    for idx, query, project, cat, gt_id, conf in QUERIES:
        for pl in PIPELINES:
            entry = cache.get(f"{idx}:{pl}", {})
            raw[(idx, pl)] = entry.get("results", [])

    print("Computing metrics...", file=sys.stderr)

    # ranks[(idx, pl)] = rank (int) or None
    ranks: dict[tuple, int | None] = {}
    for (idx, query, project, cat, gt_id, conf) in QUERIES:
        for pl in PIPELINES:
            ranks[(idx, pl)] = hit_rank(raw[(idx, pl)], gt_id)

    lines = []
    lines.append("# Pipeline Comparison — 200 Queries with Ground Truth\n")
    lines.append(f"Pipelines: {', '.join(PIPELINES)}  ")
    lines.append(f"Queries: 200 across 8 categories, 4 projects (lucene, solr, vespa, pharos)  ")
    lines.append(f"Results per query: {LIMIT}  \n")
    lines.append("> All embeddings indexed. All 9 pipelines active. ")
    lines.append("> `auto` routes single-token/FQN queries to keyword, NL queries to hybrid. ")
    lines.append("> `unified` uses BM25 + late-interaction vector boost in a single pass.\n")

    KS = [1, 3, 5, 10]

    # ── Overall metrics ────────────────────────────────────────────────────────
    def metrics(query_subset):
        pk   = {k: {pl: 0   for pl in PIPELINES} for k in KS}
        mrr  = {pl: 0.0 for pl in PIPELINES}
        ndcgs = {k: {pl: 0.0 for pl in PIPELINES} for k in KS}
        n = len(query_subset)
        for (idx, query, project, cat, gt_id, conf) in query_subset:
            for pl in PIPELINES:
                r = ranks[(idx, pl)]
                for k in KS:
                    if r is not None and r <= k:
                        pk[k][pl] += 1
                    ndcgs[k][pl] += ndcg(r, k)
                mrr[pl] += reciprocal_rank(r)
        return (
            {k: {pl: pk[k][pl]/n   for pl in PIPELINES} for k in KS},
            {pl: mrr[pl]/n         for pl in PIPELINES},
            {k: {pl: ndcgs[k][pl]/n for pl in PIPELINES} for k in KS},
            n
        )

    pk_all, mrr_all, ndcg_all, _ = metrics(QUERIES)

    lines.append("## Overall Metrics (200 queries)\n")
    lines.append(f"| Metric | {' | '.join(PIPELINES)} |")
    lines.append(f"|--------|{'|'.join(['---']*len(PIPELINES))}|")
    for k in KS:
        lines.append(f"| P@{k:<3}  | {' | '.join(f'{pk_all[k][pl]:.3f}' for pl in PIPELINES)} |")
    lines.append(f"| MRR@10 | {' | '.join(f'{mrr_all[pl]:.3f}' for pl in PIPELINES)} |")
    for k in KS:
        lines.append(f"| NDCG@{k:<2} | {' | '.join(f'{ndcg_all[k][pl]:.3f}' for pl in PIPELINES)} |")
    lines.append("")

    # ── Per-category metrics ───────────────────────────────────────────────────
    for k in KS:
        lines.append(f"## P@{k} by Category\n")
        lines.append(f"| Category | n | {' | '.join(pl[:12] for pl in PIPELINES)} | best |")
        lines.append(f"|----------|---|{'|'.join(['---']*len(PIPELINES))}|------|")
        for cat in sorted(CAT_LABELS):
            subset = [q for q in QUERIES if q[3] == cat]
            pk_cat, _, _ndcg, n = metrics(subset)
            vals = [pk_cat[k][pl] for pl in PIPELINES]
            best_pl = PIPELINES[vals.index(max(vals))]
            best_abbr = {"keyword": "kw", "auto": "au", "vector": "ve", "unified": "un", "hybrid": "hy", "hybrid-diverse": "hd", "hybrid-reranked": "hr", "hybrid-reranked-diverse": "rd"}[best_pl]
            cells = ' | '.join(f'{v:.2f}' for v in vals)
            lines.append(f"| {cat}: {CAT_LABELS[cat][:26]} | {n} | {cells} | **{best_abbr}** |")
        lines.append("")

    # ── MRR by category ────────────────────────────────────────────────────────
    lines.append("## MRR@10 by Category\n")
    lines.append(f"| Category | n | {' | '.join(pl[:12] for pl in PIPELINES)} | best |")
    lines.append(f"|----------|---|{'|'.join(['---']*len(PIPELINES))}|------|")
    for cat in sorted(CAT_LABELS):
        subset = [q for q in QUERIES if q[3] == cat]
        pk_cat, mrr_cat, _ndcg, n = metrics(subset)
        vals = [mrr_cat[pl] for pl in PIPELINES]
        best_pl = PIPELINES[vals.index(max(vals))]
        best_abbr = {"keyword": "kw", "auto": "au", "vector": "ve", "unified": "un", "hybrid": "hy", "hybrid-diverse": "hd", "hybrid-reranked": "hr", "hybrid-reranked-diverse": "rd"}[best_pl]
        cells = ' | '.join(f'{v:.3f}' for v in vals)
        lines.append(f"| {cat}: {CAT_LABELS[cat][:26]} | {n} | {cells} | **{best_abbr}** |")
    lines.append("")

    # ── Per-project metrics ────────────────────────────────────────────────────
    lines.append("## All Metrics by Project\n")
    for proj in ["lucene", "solr", "vespa", "pharos"]:
        subset = [q for q in QUERIES if q[2] == proj]
        pk_proj, mrr_proj, _ndcg, n = metrics(subset)
        lines.append(f"### {proj.capitalize()} (n={n})\n")
        lines.append(f"| Metric | {' | '.join(PIPELINES)} |")
        lines.append(f"|--------|{'|'.join(['---']*len(PIPELINES))}|")
        for k in KS:
            lines.append(f"| P@{k:<3}  | {' | '.join(f'{pk_proj[k][pl]:.3f}' for pl in PIPELINES)} |")
        lines.append(f"| MRR@10 | {' | '.join(f'{mrr_proj[pl]:.3f}' for pl in PIPELINES)} |")
        lines.append("")
    lines.append("")

    # ── Category × pipeline heatmap (MRR) ─────────────────────────────────────
    lines.append("## MRR Heatmap: Category × Pipeline\n")
    pl_headers = " | ".join(pl[:10] for pl in PIPELINES)
    pl_seps    = "|".join(["---"] * len(PIPELINES))
    lines.append(f"| Category | {pl_headers} | Winner |")
    lines.append(f"|----------|{pl_seps}|--------|")
    for cat in sorted(CAT_LABELS):
        subset = [q for q in QUERIES if q[3] == cat]
        _, mrr, _ndcg, _ = metrics(subset)
        vals = [mrr[pl] for pl in PIPELINES]
        best = PIPELINES[vals.index(max(vals))]
        cells = ' | '.join(f'{v:.3f}' for v in vals)
        lines.append(f"| {cat}: {CAT_LABELS[cat][:28]} | {cells} | **{best}** |")
    lines.append("")

    # ── Queries where reranker beats keyword by ≥2 ranks ─────────────────────
    lines.append("## Queries Where Reranker Improves Rank by ≥2 Positions\n")
    lines.append("| # | Cat | Project | Query | kw rank | reranked rank | Δ |")
    lines.append("|---|-----|---------|-------|---------|---------------|---|")
    improvements = []
    for (idx, query, project, cat, gt_id, conf) in QUERIES:
        kw_r = ranks[(idx, "keyword")]
        hr_r = ranks[(idx, "hybrid-reranked")]
        if kw_r and hr_r and hr_r < kw_r and (kw_r - hr_r) >= 2:
            improvements.append((kw_r - hr_r, idx, query, project, cat, kw_r, hr_r))
    for delta, idx, query, project, cat, kw_r, hr_r in sorted(improvements, reverse=True)[:25]:
        lines.append(f"| {idx} | {cat} | {project} | {query[:50]} | {kw_r} | {hr_r} | +{delta} |")
    lines.append("")

    # ── Queries where reranker hurts ─────────────────────────────────────────
    lines.append("## Queries Where Reranker Hurts (rank regresses ≥2)\n")
    lines.append("| # | Cat | Project | Query | kw rank | reranked rank | Δ |")
    lines.append("|---|-----|---------|-------|---------|---------------|---|")
    regressions = []
    for (idx, query, project, cat, gt_id, conf) in QUERIES:
        kw_r = ranks[(idx, "keyword")]
        hr_r = ranks[(idx, "hybrid-reranked")]
        if kw_r and hr_r and hr_r > kw_r and (hr_r - kw_r) >= 2:
            regressions.append((hr_r - kw_r, idx, query, project, cat, kw_r, hr_r))
        elif kw_r and not hr_r:
            regressions.append((10, idx, query, project, cat, kw_r, ">10"))
    for delta, idx, query, project, cat, kw_r, hr_r in sorted(regressions, reverse=True)[:20]:
        lines.append(f"| {idx} | {cat} | {project} | {query[:50]} | {kw_r} | {hr_r} | -{delta} |")
    lines.append("")

    # ── Queries where nothing works (all pipelines miss) ──────────────────────
    lines.append("## Hard Queries — Ground Truth Not Found by Any Pipeline\n")
    lines.append("| # | Cat | Conf | Project | Query | Ground truth |")
    lines.append("|---|-----|------|---------|-------|--------------|")
    for (idx, query, project, cat, gt_id, conf) in QUERIES:
        if all(ranks[(idx, pl)] is None for pl in PIPELINES):
            gt_short = gt_id.split(":")[-1][:60]
            lines.append(f"| {idx} | {cat} | {conf} | {project} | {query[:50]} | `{gt_short}` |")
    lines.append("")

    # ── Per-query detail table ─────────────────────────────────────────────────
    lines.append("## Per-Query Results\n")
    lines.append("Rank shown for each pipeline; `—` = not in top 10.\n")
    pl_cols = " | ".join(pl[:10] for pl in PIPELINES)
    pl_seps = "|".join(["---"] * len(PIPELINES))
    lines.append(f"| # | Cat | Conf | Project | Query | {pl_cols} | GT class |")
    lines.append(f"|---|-----|------|---------|-------|{pl_seps}|----------|")
    for (idx, query, project, cat, gt_id, conf) in QUERIES:
        def rfmt(pl):
            r = ranks[(idx, pl)]
            return f"**{r}**" if r == 1 else (str(r) if r else "—")
        gt_short = gt_id.split(":")[-1].split("#")[0].split(".")[-1][:25]
        rank_cells = " | ".join(rfmt(pl) for pl in PIPELINES)
        lines.append(f"| {idx} | {cat} | {conf} | {project} | {query[:42]} | {rank_cells} | `{gt_short}` |")
    lines.append("")

    print("\n".join(lines))

    if args.goldenset:
        print()
        print(run_goldenset_eval(cache))
        _save_cache(cache)


if __name__ == "__main__":
    main()
