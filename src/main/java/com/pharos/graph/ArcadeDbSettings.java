package com.pharos.graph;

import com.arcadedb.GlobalConfiguration;

/**
 * JVM-wide ArcadeDB settings that keep pharos' graph databases readable by ArcadeDB 25.1.1
 * (pharos 1.11.x), so a downgrade, a re-upgrade or a mixed-version install needs no re-index.
 * Applied once, before {@link CallGraph} or {@link ModuleGraph} opens any database; both run
 * {@link #apply()} from their static initializers.
 *
 * <ul>
 *   <li>{@code arcadedb.graph.supernodeThreshold = 0}: ArcadeDB 26 otherwise promotes a vertex
 *       with more than about 4,096 edges in one direction (in lucene: {@code Override}'s
 *       54,963 {@code annotated_by} edges, {@code Document#add}'s 5,692 callers) to a striped layout with a new record type, one way,
 *       which 25.1.1 cannot read ("Cannot find record type"). Promoted vertices also list their
 *       edges only approximately newest-first, while {@link CallGraph#load} relies on the exact
 *       order.</li>
 *   <li>{@code arcadedb.indexBloomFilterRate = 0}: no {@code .bfidx} bloom filters. 25.1.1
 *       ignores them, so after it writes to an index a re-upgraded 26 would consult a filter that
 *       no longer covers the index's keys.</li>
 * </ul>
 */
final class ArcadeDbSettings {

    private ArcadeDbSettings() {}

    static {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
        GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(0f);
    }

    /** Loads this class, which applies the settings once. */
    static void apply() {
        // the static initializer does the work
    }
}
