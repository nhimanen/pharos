package com.pharos.cluster;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The node's role-specific runtime: the replication backend and, on readers, the
 * {@link ReplicaSyncer} that keeps the local cache in step with the object store.
 *
 * <p>Readers: at construction the generations recorded in the reader registry
 * ({@code <cacheDir>/registry.json}) are activated, so a one-shot command (or a process that does
 * not sync) serves what is already on disk. {@link #beginServing} takes the exclusive
 * {@link SyncLock} on the cache dir: its holder runs the syncer (first pass in the background,
 * then every {@code cluster.pollIntervalSeconds}); any other process on the same cache dir runs no
 * syncer and follows the registry the holder maintains (re-activating every poll interval, and
 * taking over the lock once the holder is gone).
 *
 * <p>{@link #close} stops the syncer, releases the lock and closes the backend (object-store
 * clients); it is idempotent.
 */
public final class ClusterRuntime implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClusterRuntime.class);
    private static final Pattern GEN_DIR = Pattern.compile("gen-\\d{1,18}");

    private final IndexConfig config;
    private final ReplicationBackend backend;
    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;
    private final CountDownLatch firstSync = new CountDownLatch(1);

    private ReplicaSyncer syncer;
    private SyncLock syncLock;
    private ScheduledExecutorService follower;
    private Thread firstSyncThread;
    private boolean serving;
    private volatile boolean ready;
    private boolean closed;

    public ClusterRuntime(IndexConfig config, ReplicationBackend backend, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer) {
        this.config = Objects.requireNonNull(config, "config");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.luceneIndexer = Objects.requireNonNull(luceneIndexer, "luceneIndexer");
        if (isReader() && backend.localCache() instanceof ReaderLocalCache) {
            try {
                activateRegistered();
            } catch (RuntimeException e) {
                log.warn("Activating the registered generations failed: {}", e.toString());
            }
        }
    }

    /**
     * The registry a node serves: on readers {@code <cacheDir>/registry.json}, filled by the
     * syncer; otherwise the regular {@code registry.json}.
     */
    public static ProjectRegistry registryFor(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            return ProjectRegistry.at(config.getCluster().getCacheDir().resolve("registry.json"));
        }
        return new ProjectRegistry(config);
    }

    /**
     * Readers never read source files: bodies and snippets come from the index, so an
     * unrelated or stale file at an indexed absolute path cannot leak into answers.
     */
    public static void applySourceReadPolicy(IndexConfig config) {
        SourceReader.setEnabled(config.getCluster().getRole() != NodeRole.READER);
    }

    public IndexPublisher publisher() {
        return backend.publisher();
    }

    private boolean isReader() {
        return config.getCluster().getRole() == NodeRole.READER;
    }

    private Path cacheDir() {
        return config.getCluster().getCacheDir();
    }

    private ReaderLocalCache readerCache() {
        if (!(backend.localCache() instanceof ReaderLocalCache cache)) {
            throw new IllegalStateException("reader role needs a ReaderLocalCache, got " + backend.localCache());
        }
        return cache;
    }

    /**
     * {@link #beginServing}, then waits until the first sync pass has finished (successfully or
     * not: failures are logged and retried every poll interval).
     */
    public void startServing() throws IOException {
        beginServing();
        awaitFirstSync();
    }

    /**
     * Readers: starts serving without waiting for the first sync, so a daemon can bind its port at
     * once ({@link #ready} turns true when the first pass finishes). The process holding the cache
     * dir's {@link SyncLock} syncs; others follow the registry. No-op on other roles and when
     * already serving.
     *
     * @throws IOException when the lock file cannot be created
     */
    public void beginServing() throws IOException {
        synchronized (this) {
            if (!isReader() || serving) return;
            if (closed) throw new IllegalStateException("cluster runtime is closed");
            ReaderLocalCache cache = readerCache();
            Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir());
            serving = true;
            if (lock.isPresent()) {
                becomeOwner(lock.get(), cache);
            } else {
                becomeFollower();
            }
        }
    }

    /** Blocks until the first sync pass of a serving reader has finished; returns at once otherwise. */
    public void awaitFirstSync() {
        synchronized (this) {
            if (!isReader() || !serving) return;
        }
        try {
            firstSync.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** False while a serving reader's first sync pass is still running; true otherwise. */
    public boolean ready() {
        return !isReader() || ready;
    }

    /** True when this process holds the cache dir's sync lock and runs the syncer. */
    public synchronized boolean ownsSync() {
        return syncer != null;
    }

    // Holding the monitor of this.
    private void becomeOwner(SyncLock lock, ReaderLocalCache cache) {
        syncLock = lock;
        ReplicaSyncer s = new ReplicaSyncer(backend.catalog(), backend.source(), cache, registry,
                cacheDir(), config.getSynonymsFile(),
                Duration.ofSeconds(config.getCluster().getPollIntervalSeconds()));
        syncer = s;
        firstSyncThread = new Thread(() -> firstPass(s), "pharos-replica-first-sync");
        firstSyncThread.setDaemon(true);
        firstSyncThread.start();
    }

    private void firstPass(ReplicaSyncer s) {
        try {
            s.syncOnce();
        } catch (Exception e) {
            log.warn("Replica sync from bucket '{}' failed (retrying every {} s): {}",
                    config.getCluster().getObjectStore().getBucket(),
                    config.getCluster().getPollIntervalSeconds(), e.toString());
        } finally {
            Map<ShardId, Generation> served = s.served();
            log.info("Reader {} serving {} shard(s): {}", config.getCluster().effectiveNodeId(), served.size(), served);
            warm(served.keySet().stream().map(ShardId::value).toList());
            ready = true;
            firstSync.countDown();
        }
        try {
            s.start(Duration.ofSeconds(config.getCluster().getPollIntervalSeconds()));
        } catch (IllegalStateException closedMeanwhile) {
            // close() ran during the first pass
        }
    }

    // Holding the monitor of this.
    private void becomeFollower() {
        log.info("Another process holds {}: not syncing here; serving the generations it registers in {}",
                cacheDir().resolve(SyncLock.FILE), cacheDir().resolve("registry.json"));
        warm(activateRegistered());
        ready = true;
        firstSync.countDown();
        long interval = config.getCluster().getPollIntervalSeconds();
        follower = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-replica-follower");
            t.setDaemon(true);
            return t;
        });
        follower.scheduleWithFixedDelay(this::followerTick, interval, interval, TimeUnit.SECONDS);
    }

    /** Follower poll: pick up what the lock holder registered; take over once it is gone. */
    void followerTick() {
        try {
            activateRegistered();
            synchronized (this) {
                if (closed || syncer != null) return;
            }
            Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir());
            if (lock.isEmpty()) return;
            synchronized (this) {
                if (closed || syncer != null) {
                    lock.get().close();
                    return;
                }
                log.info("Took over syncing of {} (the previous holder of the sync lock is gone)", cacheDir());
                if (follower != null) follower.shutdown(); // this task's own executor: no wait
                follower = null;
                becomeOwner(lock.get(), readerCache());
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Following the reader registry failed: {}", e.toString());
        }
    }

    /**
     * Readers: activates, in the {@link ReaderLocalCache}, the generation dir every project of the
     * reader registry points at, when it is a complete {@code <cacheDir>/<project>/gen-N} dir.
     *
     * @return the projects whose active generation changed
     */
    public List<String> activateRegistered() {
        if (!isReader()) return List.of();
        ReaderLocalCache cache = readerCache();
        Path root = cacheDir().toAbsolutePath().normalize();
        List<String> changed = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll()) {
            String name = meta.getName();
            if (name == null || meta.getIndexPath() == null) continue;
            try {
                new ShardId(name);
            } catch (IllegalArgumentException e) {
                continue;
            }
            Path dir = Path.of(meta.getIndexPath()).toAbsolutePath().normalize();
            if (!root.resolve(name).equals(dir.getParent())
                    || !GEN_DIR.matcher(dir.getFileName().toString()).matches()
                    || !Files.isDirectory(dir.resolve(ReplicaSyncer.LUCENE_DIR))
                    || !Files.isRegularFile(dir.resolve(ReplicaSyncer.MANIFEST_FILE))) {
                continue;
            }
            if (cache.active(name).filter(dir::equals).isPresent()) continue;
            cache.activate(name, dir);
            changed.add(name);
        }
        if (!changed.isEmpty()) log.info("Activated registered generations of {}", changed);
        return changed;
    }

    /** Readers: one sync pass now (in addition to the polling); a follower re-reads the registry. */
    public void syncNow() throws IOException {
        ReplicaSyncer s;
        synchronized (this) {
            if (!serving) throw new IllegalStateException("not serving: call startServing() on a reader first");
            s = syncer;
        }
        if (s == null) {
            activateRegistered();
            return;
        }
        s.syncOnce();
    }

    /** Generation served per shard; empty unless this is the syncing reader. */
    public synchronized Map<ShardId, Generation> served() {
        return syncer == null ? Map.of() : syncer.served();
    }

    /** Opens the projects' readers so the first query does not pay for it. */
    private void warm(List<String> projects) {
        for (String project : projects) {
            try {
                luceneIndexer.openReader(project);
            } catch (IOException | RuntimeException e) {
                log.warn("Shard {}: opening the index reader failed: {}", project, e.toString());
            }
        }
    }

    @Override
    public void close() {
        ReplicaSyncer s;
        SyncLock lock;
        ScheduledExecutorService f;
        Thread first;
        synchronized (this) {
            if (closed) return;
            closed = true;
            s = syncer;
            syncer = null;
            lock = syncLock;
            syncLock = null;
            f = follower;
            follower = null;
            first = firstSyncThread;
        }
        if (f != null) f.shutdownNow();
        if (s != null) s.close();
        if (first != null && first != Thread.currentThread()) {
            try {
                first.join(10_000); // keep the lock until an in-flight first pass has stopped
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (lock != null) {
            try {
                lock.close();
            } catch (IOException e) {
                log.warn("Releasing the sync lock failed: {}", e.toString());
            }
        }
        try {
            backend.close();
        } catch (IOException | RuntimeException e) {
            log.warn("Closing the {} backend failed: {}", backend.kind().json(), e.toString());
        }
    }
}
