package com.pharos.cluster.warm;

import com.pharos.cluster.objectstore.ObjectMissingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 follow-up M2: only the object store's own "no such key" degrades a lazy reader. A local
 * {@link NoSuchFileException} (a block file, a head sidecar or a temp file gone from the cache dir)
 * is an ordinary read failure and must not mark the served generation garbage-collected.
 */
class ObjectMissingDetectionTest {

    @Test
    void onlyTheStoresMissingObjectCounts() {
        assertThat(CachingRemoteIndexInput.RemoteFile.objectMissing(new ObjectMissingException("shards/a/gen-1/_0.cfs")))
                .isTrue();
        assertThat(CachingRemoteIndexInput.RemoteFile.objectMissing(
                new IOException("block fetch failed", new ObjectMissingException("shards/a/gen-1/_0.cfs")))).isTrue();
        assertThat(CachingRemoteIndexInput.RemoteFile.objectMissing(new NoSuchFileException("/cache/.blocks/ab/cd")))
                .as("a local file gone from the cache dir").isFalse();
        assertThat(CachingRemoteIndexInput.RemoteFile.objectMissing(
                new IOException("wrapped", new NoSuchFileException("/cache/x.tmp")))).isFalse();
    }

    @Test
    void theExceptionIsStillANoSuchFileException() {
        ObjectMissingException e = new ObjectMissingException("k/x");
        assertThat(e).isInstanceOf(NoSuchFileException.class);
        assertThat(e.getMessage()).contains("k/x");
        assertThat(e.key()).isEqualTo("k/x");
    }
}
