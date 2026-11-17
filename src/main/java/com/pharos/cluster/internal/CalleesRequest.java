package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/** {@code POST /internal/callees}: the response is {@code {fqn: [callee fqns]}}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CalleesRequest(String shard, Long generation, List<String> fqns) {}
