package com.pharos.search.pipeline;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.QueryRouter;
import com.pharos.search.SearchRequest;
import com.pharos.search.VectorSearchStrategy;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles {@link SearchPipeline} instances from {@link PipelineConfig} descriptors.
 *
 * <p>Stage instances are singletons shared across all pipelines — stages are stateless
 * so sharing is safe and avoids redundant allocations.
 *
 * <p>Two-pass build: first, all non-dispatch pipelines are built and registered by name;
 * then dispatch pipelines (e.g. {@code auto}) are built, referencing the first-pass results.
 */
public final class PipelineFactory {

    private final KeywordRetrievalStage  kwStage;
    private final VectorRetrievalStage   vecStage;
    private final UnifiedRetrievalStage  unifiedStage;
    private final BordaMerger            borda;
    private final RrfMerger              rrf;
    private final CrossEncoderMerger     ceMerger;
    private final CrossEncoderReranker   ceReranker;
    private final DiversityReranker      diversity;
    private final MmrClassDiversifier    mmr;
    private final QueryRouter            queryRouter;
    private final boolean                embedsAvailable;
    private final boolean                ceAvailable;

    public PipelineFactory(KeywordSearchStrategy kwStrategy,
                           VectorSearchStrategy  vecStrategy,
                           EmbeddingProvider     embedder,
                           CrossEncoder          crossEncoder,
                           QueryRouter           queryRouter) {
        this.kwStage      = new KeywordRetrievalStage(kwStrategy);
        this.vecStage     = new VectorRetrievalStage(vecStrategy);
        this.unifiedStage = new UnifiedRetrievalStage(kwStrategy, embedder);
        this.borda        = new BordaMerger();
        this.rrf          = new RrfMerger();
        this.ceMerger     = new CrossEncoderMerger(crossEncoder);
        this.ceReranker   = new CrossEncoderReranker(crossEncoder);
        this.diversity    = new DiversityReranker(0.5f);
        this.mmr          = new MmrClassDiversifier(0.5f);
        this.queryRouter  = queryRouter;
        this.embedsAvailable = embedder.isAvailable();
        this.ceAvailable     = crossEncoder.isAvailable();
    }

    public record Result(
            Map<SearchRequest.SearchType, SearchPipeline> pipelines,
            List<PipelineDescriptor> descriptors
    ) {}

    public Result build(List<PipelineConfig> configs) {
        // Pass 1: build all non-dispatch pipelines
        Map<String, SearchPipeline> byName = new LinkedHashMap<>();
        for (PipelineConfig cfg : configs) {
            if (cfg.dispatch == null) {
                byName.put(cfg.name, buildSimple(cfg));
            }
        }
        // Pass 2: build dispatch pipelines (these reference pass-1 results by name)
        for (PipelineConfig cfg : configs) {
            if (cfg.dispatch != null) {
                byName.put(cfg.name, buildDispatch(cfg, byName));
            }
        }

        Map<SearchRequest.SearchType, SearchPipeline> typeMap = new EnumMap<>(SearchRequest.SearchType.class);
        List<PipelineDescriptor> descriptors = new ArrayList<>();

        for (PipelineConfig cfg : configs) {
            SearchPipeline pipeline = byName.get(cfg.name);
            if (pipeline == null) continue;

            // Map pipeline name to enum — names not in the enum are internal-only (no public registration)
            try {
                SearchRequest.SearchType type = SearchRequest.SearchType.from(cfg.name);
                typeMap.put(type, pipeline);
            } catch (Exception ignored) {
                // Not a public SearchType — child pipeline only, no enum mapping needed
            }

            if (cfg.visible) {
                boolean available = isAvailable(cfg);
                descriptors.add(new PipelineDescriptor(
                        cfg.name,
                        cfg.label != null ? cfg.label : cfg.name,
                        cfg.description != null ? cfg.description : "",
                        available));
            }
        }

        return new Result(Map.copyOf(typeMap), List.copyOf(descriptors));
    }

    private SearchPipeline buildSimple(PipelineConfig cfg) {
        SearchPipeline.Builder b = SearchPipeline.builder();

        if (cfg.router != null) b.router(resolveRouter(cfg.router));
        for (String r : cfg.retrievers) b.retriever(resolveRetriever(r));
        if (cfg.oversample > 1) b.oversample(cfg.oversample);
        for (String pre : cfg.premerge) b.premerge(resolveReranker(pre));
        if (cfg.merger != null) b.merger(resolveMerger(cfg.merger));
        for (String rr : cfg.rerankers) b.reranker(resolveReranker(rr));

        return b.build();
    }

    private SearchPipeline buildDispatch(PipelineConfig cfg, Map<String, SearchPipeline> byName) {
        Map<SearchRequest.SearchType, SearchPipeline> children = new EnumMap<>(SearchRequest.SearchType.class);
        SearchRequest.SearchType defaultType = SearchRequest.SearchType.HYBRID;

        for (Map.Entry<String, String> entry : cfg.dispatch.entrySet()) {
            String key = entry.getKey();
            String targetName = entry.getValue();
            SearchPipeline target = byName.get(targetName);
            if (target == null) throw new IllegalArgumentException(
                    "Dispatch pipeline '" + cfg.name + "' references unknown pipeline: " + targetName);

            if ("default".equals(key)) {
                defaultType = SearchRequest.SearchType.from(
                        nameToTypeString(targetName, cfg.dispatch));
            } else {
                SearchRequest.SearchType type = SearchRequest.SearchType.valueOf(key);
                children.put(type, target);
                defaultType = type; // last non-default entry becomes fallback if no 'default' key
            }
        }

        // Re-resolve default from the 'default' key if present
        if (cfg.dispatch.containsKey("default")) {
            String defaultTarget = cfg.dispatch.get("default");
            for (Map.Entry<SearchRequest.SearchType, SearchPipeline> e : children.entrySet()) {
                if (byName.get(defaultTarget) == e.getValue()) {
                    defaultType = e.getKey();
                    break;
                }
            }
        }

        RouterDispatcher dispatcher = new RouterDispatcher(children, defaultType);

        SearchPipeline.Builder b = SearchPipeline.builder();
        if (cfg.router != null) b.router(resolveRouter(cfg.router));
        b.retriever(dispatcher);
        return b.build();
    }

    private boolean isAvailable(PipelineConfig cfg) {
        for (String req : cfg.requires) {
            if ("embeddings".equals(req) && !embedsAvailable) return false;
            if ("cross-encoder".equals(req) && !ceAvailable) return false;
        }
        return true;
    }

    private QueryRouter resolveRouter(String name) {
        return switch (name) {
            case "query-classifier" -> queryRouter;
            default -> throw new IllegalArgumentException("Unknown router: " + name);
        };
    }

    private RetrievalStage resolveRetriever(String name) {
        return switch (name) {
            case "keyword" -> kwStage;
            case "vector"  -> vecStage;
            case "unified" -> unifiedStage;
            default -> throw new IllegalArgumentException("Unknown retriever: " + name);
        };
    }

    private MergeStage resolveMerger(String name) {
        return switch (name) {
            case "borda"               -> borda;
            case "rrf"                 -> rrf;
            case "cross-encoder-merge" -> ceMerger;
            default -> throw new IllegalArgumentException("Unknown merger: " + name);
        };
    }

    private RerankStage resolveReranker(String name) {
        return switch (name) {
            case "mmr"           -> mmr;
            case "diversity"     -> diversity;
            case "cross-encoder" -> ceReranker;
            default -> throw new IllegalArgumentException("Unknown reranker: " + name);
        };
    }

    /** Resolves a child pipeline name to a SearchType string for default-type lookup. */
    private static String nameToTypeString(String pipelineName,
                                           Map<String, String> dispatch) {
        // Find which dispatch key maps to this pipeline name
        for (Map.Entry<String, String> e : dispatch.entrySet()) {
            if (!e.getKey().equals("default") && e.getValue().equals(pipelineName)) {
                return e.getKey();
            }
        }
        return "HYBRID";
    }
}
