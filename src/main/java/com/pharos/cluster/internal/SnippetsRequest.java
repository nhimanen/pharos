package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/** {@code POST /internal/snippets}: snippets for result {@code ids} of the pinned {@code generation}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SnippetsRequest(String shard, Long generation, List<String> ids, String query, float[] queryVector) {}
