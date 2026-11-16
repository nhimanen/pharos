package com.pharos.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Exclusive lock on {@code <cacheDir>/.sync.lock}: only its holder runs a {@link ReplicaSyncer}
 * on that cache dir, so two processes (a web daemon and an MCP server, or a daemon restarted while
 * the old one still runs) never delete each other's partial downloads or generation dirs. The
 * lock is an OS file lock, released when the holder closes it or its process dies.
 */
public final class SyncLock implements Closeable {

    public static final String FILE = ".sync.lock";

    private final FileChannel channel;
    private final FileLock lock;

    private SyncLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    /**
     * @return the lock, or empty when another process (or another runtime in this JVM) holds it
     * @throws IOException when the lock file cannot be created or opened
     */
    public static Optional<SyncLock> tryAcquire(Path cacheDir) throws IOException {
        Files.createDirectories(cacheDir);
        FileChannel ch = FileChannel.open(cacheDir.resolve(FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = ch.tryLock();
        } catch (OverlappingFileLockException e) {
            lock = null; // held by another channel of this JVM
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        if (lock == null) {
            ch.close();
            return Optional.empty();
        }
        try {
            ch.truncate(0);
            ch.write(ByteBuffer.wrap((ProcessHandle.current().pid() + "\n").getBytes(StandardCharsets.UTF_8)), 0);
        } catch (IOException ignored) {
            // the pid is informational only
        }
        return Optional.of(new SyncLock(ch, lock));
    }

    @Override
    public void close() throws IOException {
        try {
            if (lock.isValid()) lock.release();
        } finally {
            channel.close();
        }
    }
}
