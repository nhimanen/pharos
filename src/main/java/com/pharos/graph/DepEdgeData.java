package com.pharos.graph;

/**
 * Immutable snapshot of a dependency edge read from the ArcadeDB module graph.
 * Replaces the mutable {@link ModuleDep} JGraphT edge.
 */
public record DepEdgeData(
        String scope,           // Maven scope: compile / test / provided / runtime
        String declaredVersion  // as declared in pom.xml; may be null
) {}
