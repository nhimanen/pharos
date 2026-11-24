package com.pharos.search.distributed;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.SearchResult;

/**
 * One candidate of a shard's raw retrieval: the unboosted retrieval score, the inputs the
 * coordinator's graph boost and shaping need ({@code inDegree}, {@code filePath}, {@code docType},
 * {@code classType}, {@code access}), and the result fields themselves.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RawCandidate(float rawScore, int inDegree, String filePath, String docType, String classType,
                           String access, SearchResult result) {}
