package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class LazyGenerationsTest {

    @TempDir Path tmp;

    private static ReplicaSource unusedSource() {
        return (ReplicaSource) Proxy.newProxyInstance(LazyGenerationsTest.class.getClassLoader(),
                new Class<?>[]{ReplicaSource.class}, (p, m, a) -> { throw new UnsupportedOperationException(); });
    }

    private static ManifestFile lucene(String name, long length) {
        return new ManifestFile(ManifestFile.LUCENE, name, "k/" + name, length, "0");
    }

    @Test
    void aCompoundFileNoLargerThanOneBlockIsEager() {
        ClusterConfig.ReaderConfig cfg = new ClusterConfig.ReaderConfig();
        cfg.setBlockSizeBytes(64 * 1024);
        cfg.setEagerMaxBytes(0);
        try (LazyGenerations lazy = new LazyGenerations(tmp, cfg, unusedSource())) {
            assertThat(lazy.eager(lucene("_0.cfs", 64 * 1024))).as("exactly one block").isTrue();
            assertThat(lazy.eager(lucene("_0.cfs", 10))).isTrue();
            assertThat(lazy.eager(lucene("_0.cfs", 64 * 1024 + 1))).as("more than one block").isFalse();
            assertThat(lazy.eager(lucene("_0.tim", 64 * 1024))).as("not a compound file").isFalse();
        }
    }
}
