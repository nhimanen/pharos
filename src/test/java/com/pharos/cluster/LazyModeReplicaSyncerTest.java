package com.pharos.cluster;

import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.config.ClusterConfig;

import java.nio.file.Path;

/**
 * Every {@link ReplicaSyncerTest} case in lazy mode with the default reader config. sample-app's
 * files are all below {@code eagerMaxBytes}, so the generation dirs hold the same files as in full
 * mode (plus the marker) and every Phase 1 assertion applies unchanged; {@link LazyReaderTest}
 * covers generations with remote files.
 */
class LazyModeReplicaSyncerTest extends ReplicaSyncerTest {

    @Override
    LazyGenerations lazyGenerations(Path cacheDir, ReplicaSource source) {
        return new LazyGenerations(cacheDir, new ClusterConfig.ReaderConfig(), source);
    }
}
