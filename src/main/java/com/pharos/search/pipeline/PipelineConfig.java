package com.pharos.search.pipeline;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Declarative configuration for a single search pipeline, loaded from {@code pipelines.yaml}.
 *
 * <p>Example (non-dispatch pipeline):
 * <pre>
 *   name: hybrid-rrf
 *   label: "Hybrid RRF"
 *   description: "RRF fusion of keyword and vector results"
 *   retrievers: [keyword, vector]
 *   merger: rrf
 *   rerankers: [mmr]
 * </pre>
 *
 * <p>Example (dispatch pipeline — auto):
 * <pre>
 *   name: auto
 *   label: "Auto"
 *   router: query-classifier
 *   dispatch:
 *     KEYWORD: _auto-keyword
 *     HYBRID: _auto-hybrid-rrf
 *     default: _auto-hybrid-rrf
 * </pre>
 *
 * <p>Stage identifiers:
 * <ul>
 *   <li>Retrievers: {@code keyword}, {@code vector}, {@code unified}</li>
 *   <li>Mergers: {@code borda}, {@code rrf}, {@code cross-encoder-merge}</li>
 *   <li>Rerankers: {@code mmr}, {@code diversity}, {@code cross-encoder}</li>
 *   <li>Routers: {@code query-classifier}</li>
 * </ul>
 *
 * <p>Availability guards:
 * <ul>
 *   <li>{@code embeddings} — pipeline is marked unavailable when no embedding model is loaded</li>
 *   <li>{@code cross-encoder} — pipeline is marked unavailable when no cross-encoder is configured</li>
 * </ul>
 */
public class PipelineConfig {

    /** Pipeline identifier used in API requests and as SearchType key. */
    public String name;

    /** Human-readable name shown in the UI selector. Defaults to {@link #name} if absent. */
    public String label;

    /** One-line description shown in the UI tooltip. */
    public String description;

    /**
     * When {@code false}, the pipeline is built and routable but not listed in the UI selector.
     * Useful for internal child pipelines referenced by a dispatch pipeline.
     */
    public boolean visible = true;

    /**
     * Capability guards. If any listed capability is unavailable at runtime the pipeline
     * descriptor is marked as unavailable (greyed out in the UI) but the pipeline is still built.
     * Recognised values: {@code embeddings}, {@code cross-encoder}.
     */
    public List<String> requires = new ArrayList<>();

    /**
     * Router name applied before retrieval. Currently only {@code query-classifier} is supported.
     * Must be set for dispatch pipelines and for UNIFIED (which reads intent for adaptive weights).
     */
    public String router;

    /** Ordered retriever stage names. At least one required. */
    public List<String> retrievers = new ArrayList<>();

    /** Multiply each retriever's fetch limit by this factor. 0 or 1 = no oversampling. */
    public int oversample = 0;

    /** Reranker stages applied to each retriever's list independently, before the merge. */
    public List<String> premerge = new ArrayList<>();

    /** Merge stage name. Required when {@link #retrievers} has more than one entry. */
    public String merger;

    /** Reranker stages applied after the merge, in order. */
    public List<String> rerankers = new ArrayList<>();

    /**
     * Dispatch table for router-based pipelines (e.g. auto).
     * Keys are {@link com.pharos.search.SearchRequest.SearchType} names (e.g. {@code KEYWORD},
     * {@code HYBRID}) or the literal {@code default}.
     * Values are names of other pipelines defined in the same YAML file.
     * When this field is non-null, {@link #retrievers} is ignored and the pipeline is built
     * as a {@link RouterDispatcher}.
     */
    public Map<String, String> dispatch;

    /** Wrapper for the top-level {@code pipelines:} list in YAML. */
    public record Root(@JsonProperty("pipelines") List<PipelineConfig> pipelines) {}
}
