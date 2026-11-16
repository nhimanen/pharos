package com.pharos.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SyncLockTest {

    @TempDir Path tmp;

    @Test
    void lockIsExclusiveUntilReleased() throws Exception {
        Path cacheDir = tmp.resolve("cache");
        Optional<SyncLock> first = SyncLock.tryAcquire(cacheDir);
        assertThat(first).isPresent();
        assertThat(cacheDir.resolve(SyncLock.FILE)).isRegularFile();
        assertThat(SyncLock.tryAcquire(cacheDir)).isEmpty();

        first.get().close();
        Optional<SyncLock> again = SyncLock.tryAcquire(cacheDir);
        assertThat(again).isPresent();
        again.get().close();
    }
}
