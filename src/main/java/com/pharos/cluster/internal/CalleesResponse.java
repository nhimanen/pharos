package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/** {@code callees} maps each requested fqn to its callee fqns; {@code generation} is the one read. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CalleesResponse(long generation, Map<String, List<String>> callees) {}
