package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.Snippet;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SnippetsResponse(long generation, Map<String, Snippet> snippets) {}
