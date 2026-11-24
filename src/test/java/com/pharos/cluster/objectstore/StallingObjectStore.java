package com.pharos.cluster.objectstore;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link InMemoryObjectStore} whose calls on chosen keys hang, like S3 requests on a saturated
 * MinIO: a call whose key (or listing prefix) contains a substring passed to {@link #stallOn}
 * blocks until {@link #release} and ignores interrupts while it waits (a blocking socket read is
 * not interruptible either). A safety cap of {@link #CAP_SECONDS} keeps a broken test from hanging
 * the build.
 */
public class StallingObjectStore extends InMemoryObjectStore {

    static final long CAP_SECONDS = 60;

    private final Set<String> stalls = ConcurrentHashMap.newKeySet();
    private volatile CountDownLatch gate = new CountDownLatch(1);
    private final AtomicInteger stalled = new AtomicInteger();

    public StallingObjectStore(Clock clock) {
        super(clock);
    }

    /** Calls on keys containing {@code keySubstring} hang from now on, until {@link #release}. */
    public void stallOn(String keySubstring) {
        stalls.add(keySubstring);
    }

    /** Lets every hanging call go on, and stops stalling. */
    public void release() {
        stalls.clear();
        CountDownLatch g = gate;
        gate = new CountDownLatch(1);
        g.countDown();
    }

    /** Calls that hung so far. */
    public int stalledCalls() {
        return stalled.get();
    }

    /** Waits until at least {@code n} calls hung. */
    public boolean awaitStalled(int n, long seconds) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (stalled.get() < n) {
            if (System.nanoTime() > until) return false;
            Thread.sleep(5);
        }
        return true;
    }

    private void maybeStall(String key) {
        boolean hit = false;
        for (String s : stalls) {
            if (key.contains(s)) {
                hit = true;
                break;
            }
        }
        if (!hit) return;
        stalled.incrementAndGet();
        CountDownLatch g = gate;
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(CAP_SECONDS);
        boolean interrupted = false;
        while (true) {
            long left = until - System.nanoTime();
            if (left <= 0) break;
            try {
                if (g.await(left, TimeUnit.NANOSECONDS)) break;
            } catch (InterruptedException e) {
                interrupted = true; // like a socket read: keep waiting
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    @Override
    public Optional<StoredObject> get(String key) {
        maybeStall(key);
        return super.get(key);
    }

    @Override
    public String put(String key, Path source) throws IOException {
        maybeStall(key);
        return super.put(key, source);
    }

    @Override
    public String put(String key, byte[] data) {
        maybeStall(key);
        return super.put(key, data);
    }

    @Override
    public Optional<String> putIfAbsent(String key, byte[] data) {
        maybeStall(key);
        return super.putIfAbsent(key, data);
    }

    @Override
    public Optional<String> putIfMatch(String key, byte[] data, String etag) {
        maybeStall(key);
        return super.putIfMatch(key, data, etag);
    }

    @Override
    public List<ObjectInfo> list(String prefix) {
        maybeStall(prefix);
        return super.list(prefix);
    }

    @Override
    public Optional<ObjectInfo> stat(String key) {
        maybeStall(key);
        return super.stat(key);
    }

    @Override
    public void delete(String key) {
        maybeStall(key);
        super.delete(key);
    }
}
