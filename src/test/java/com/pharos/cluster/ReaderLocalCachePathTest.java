package com.pharos.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Ruling P6-R3: reader cache dirs embed the shard id URL-safe encoded; a project id keeps its dir. */
class ReaderLocalCachePathTest {

    @Test
    void cacheDirsUseTheEncoding(@TempDir Path dir) {
        ReaderLocalCache cache = new ReaderLocalCache(dir);
        assertThat(cache.shardDir("pharos")).isEqualTo(dir.resolve("pharos").resolve("none"));
        assertThat(cache.shardDir("lucene~ring=s03")).isEqualTo(dir.resolve("lucene~ring%3Ds03").resolve("none"));
        Path gen = dir.resolve(new ShardId("lucene~ring=s03").pathSegment()).resolve("gen-4");
        cache.activate("lucene~ring=s03", gen);
        assertThat(cache.shardDir("lucene~ring=s03")).isEqualTo(gen);
    }
}
