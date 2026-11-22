package com.pharos.cluster;

import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.config.ClusterConfig;

import java.nio.file.Path;

/** Every {@link SplitProjectSyncTest} case in lazy mode with the default reader config. */
class LazySplitProjectSyncTest extends SplitProjectSyncTest {

    @Override
    LazyGenerations lazyGenerations(Path cacheDir, ReplicaSource source) {
        return new LazyGenerations(cacheDir, new ClusterConfig.ReaderConfig(), source);
    }
}
