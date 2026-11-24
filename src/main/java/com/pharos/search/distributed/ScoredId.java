package com.pharos.search.distributed;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A document of one shard with a first-pass score: what the two-round merges exchange. {@code doc}
 * is the Lucene doc id in the pinned generation (immutable, so the same on every replica of it);
 * {@code id} is its stable id, checked against it (two documents may share an id).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScoredId(String id, int doc, float score) {}
