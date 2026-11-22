package com.pharos.cluster;

import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Keeps a reader's local cache in step with the published generations.
 *
 * <p>For every catalog shard whose {@code LATEST} is newer than the generation served, a sync
 * removes leftover {@code gen-*.partial} dirs, materialises {@code <cacheDir>/<shard>/gen-N.partial/}
 * (Lucene files hard-linked from the active generation when their key is unchanged, else
 * downloaded; the graph as {@code graph.cgf.gz}; the manifest as {@code manifest.json}; refs are
 * not fetched), verifies every file against the manifest, atomically renames it to {@code gen-N},
 * activates it in the {@link ReaderLocalCache} (the {@code LuceneIndexer} swaps readers on its next
 * {@code openReader}), registers the project with {@code indexPath} = the generation dir, and
 * deletes the shard's generation dirs other than the active and the previous one.
 *
 * <p>A failure of one shard (unreadable {@code LATEST}, invalid manifest, failed download,
 * integrity mismatch) is logged; that shard keeps serving its previous generation, and the other
 * shards continue. A failure to fetch the synonyms file is logged too. A complete generation dir
 * found on disk with an identical {@code manifest.json} (a restart) is adopted without downloading.
 *
 * <p>Integrity: every file must have the manifest's length; the graph must match its CRC32; every
 * downloaded Lucene file is checksummed end to end against its footer
 * ({@link CodecUtil#checksumEntireFile}) and that checksum must match the manifest, while files
 * hard-linked from the active generation (already verified when they were downloaded) only have
 * their footer compared.
 *
 * <p>Format gate: a generation whose manifest carries an index format this build cannot serve
 * ({@code outdated} or {@code newer}; a manifest without the stamp counts as format 0) is neither
 * materialised nor swapped in. The shard keeps serving its current generation and a WARN (once per
 * generation) names the shard, both formats and the fix. A served generation dir carries the
 * manifest's stamp as {@code index-format.json}, so {@code index-status} and
 * {@code IndexFormat.check} see the writer's format on the reader.
 *
 * <p>If {@code LATEST} names a generation below the served one (the bucket was wiped, or the
 * shard rolled back), a WARN names both and the published generation is served as a new lineage:
 * it is materialised and activated, and the old lineage's directories are pruned.
 *
 * <p><b>Lazy mode</b> (a {@link LazyGenerations} is given): only the eager Lucene files
 * ({@link LazyGenerations#eager}) are downloaded (or hard-linked) and checksummed, the graph as
 * before; the stamp, the manifest and the {@value LazyGenerations#MARKER} marker are written; then
 * the first bytes of every remote file are downloaded as head sidecars, the blocks of the
 * {@code prefetchExtensions} files are prefetched, and a reader is opened and closed on the
 * generation (Lucene's per-file header and footer reads then cost no fetch), all within
 * {@code prefetchTimeoutSeconds} (after which a WARN is logged, the warm open still gets up to
 * {@code WARM_OPEN_GRACE} (5 s), and the generation is activated anyway, the prefetch going on in
 * the background); only then is the dir moved to {@code gen-N},
 * registered and activated. The previous generation serves meanwhile. A lazy dir is adopted on
 * restart like a full one, except by a full-mode syncer, which materialises it again in full.
 * Remote files are not checksummed when the generation is opened; their lengths come from the
 * manifest. When the format gate refuses a newer generation, the served lazy generation is
 * materialised in the background ({@link LazyGenerations#materialiseInBackground}): the bucket's GC
 * would otherwise delete objects it still reads.
 */
public class ReplicaSyncer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaSyncer.class);

    static final String MANIFEST_FILE = "manifest.json";
    static final String LUCENE_DIR = "lucene";
    private static final String PARTIAL_SUFFIX = ".partial";
    /** After a prefetch timeout, the warm open still gets this long before the generation is activated. */
    static final Duration WARM_OPEN_GRACE = Duration.ofSeconds(5);
    private static final Pattern GEN_DIR = Pattern.compile("gen-(\\d{1,18})");
    /** A plain file name: no separators, no {@code .}/{@code ..}, nothing absolute. */
    private static final Pattern PLAIN_NAME = GenerationFiles.PLAIN_NAME;

    private final ShardCatalog catalog;
    private final ReplicaSource source;
    private final ReaderLocalCache cache;
    private final ProjectRegistry registry;
    private final Path cacheDir;
    private final Path synonymsFile;
    private final Duration pollInterval;
    /** Lazy mode; null in full mode. */
    private final LazyGenerations lazy;

    private final Map<ShardId, Generation> served = new ConcurrentHashMap<>();
    /** Generation last registered per shard; re-tried when it lags {@link #served}. */
    private final Map<ShardId, Generation> registered = new HashMap<>();
    /** Generation last refused per shard by the format gate; its WARN is not repeated every poll. */
    private final Map<ShardId, Generation> refused = new HashMap<>();
    /** Shards this reader is pinned to; empty serves every catalog shard. */
    private volatile Set<ShardId> pinned = Set.of();
    /** Shards dropped by pinning: unregistered, directories removed one pass later (in-flight queries). */
    private final Map<ShardId, Long> retired = new HashMap<>();
    private java.util.function.LongSupplier clock = System::currentTimeMillis;
    private Duration retireGrace = Duration.ofSeconds(120);
    private boolean swept;
    private final Object syncLock = new Object();
    private final Object lifecycleLock = new Object();
    private ScheduledExecutorService scheduler;
    /** Long-poll mode: the thread waiting on {@link #longPoll} between passes; null otherwise. */
    private Thread longPollThread;
    /** The source's long-poll, when it has one (peer-pull): replaces the {@code pollInterval} loop. */
    private final LongPollingSource longPoll;
    /** The shards the last pass wanted (catalog, pinned): what the long-poll waits on. */
    private volatile List<ShardId> wanted = List.of();
    private volatile boolean closed;

    /**
     * @param synonymsFile local synonyms file kept equal to the published one; {@code null} skips it
     */
    public ReplicaSyncer(ShardCatalog catalog, ReplicaSource source, ReaderLocalCache cache,
                         ProjectRegistry registry, Path cacheDir, Path synonymsFile, Duration pollInterval) {
        this(catalog, source, cache, registry, cacheDir, synonymsFile, pollInterval, null);
    }

    /**
     * @param lazy lazy mode's eager-file rule, prefetch and block cache (owned by the caller); null
     *             materialises every file (full mode)
     */
    public ReplicaSyncer(ShardCatalog catalog, ReplicaSource source, ReaderLocalCache cache,
                         ProjectRegistry registry, Path cacheDir, Path synonymsFile, Duration pollInterval,
                         LazyGenerations lazy) {
        this.lazy = lazy;
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.source = Objects.requireNonNull(source, "source");
        this.longPoll = source instanceof LongPollingSource l ? l : null;
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
        this.synonymsFile = synonymsFile;
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
    }

    /** Test hook: the wall clock (millis) retirement is timed with. */
    void setClock(java.util.function.LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * How long a retired shard's directory outlives its unregistration (at least the poll interval),
     * so queries already reading it finish. Set to {@code cluster.retireGraceSeconds}.
     */
    public void setRetireGrace(Duration grace) {
        this.retireGrace = Objects.requireNonNull(grace, "grace");
    }

    /** Pins this reader to {@code shards}; empty serves all. Takes effect on the next pass. */
    public void pin(Collection<ShardId> shards) {
        this.pinned = Set.copyOf(shards);
    }

    /** Generation currently served per shard. */
    public Map<ShardId, Generation> served() {
        return Map.copyOf(served);
    }

    /**
     * One pass over the catalog. Per-shard failures are logged, never thrown.
     *
     * @throws IOException when the catalog cannot be read
     */
    public void syncOnce() throws IOException {
        synchronized (syncLock) {
            List<ShardId> shards;
            try {
                shards = catalog.shards();
            } catch (RuntimeException e) {
                throw new IOException("unreadable shard catalog: " + e.getMessage(), e);
            }
            boolean catalogEmpty = shards.isEmpty();
            Set<ShardId> pin = pinned;
            if (!pin.isEmpty()) shards = shards.stream().filter(pin::contains).toList();
            wanted = List.copyOf(shards);
            if (catalogEmpty && !served.isEmpty()) {
                // an empty read may be transient (a catalog object being rewritten): drop nothing
                log.warn("The shard catalog lists no shard; keeping the {} served shard(s)",
                        served.size());
            } else {
                dropUnwanted(shards);
            }
            for (ShardId shard : shards) {
                if (closed) return;
                retired.remove(shard);
                try {
                    syncShard(shard);
                } catch (IOException | RuntimeException e) {
                    log.warn("Sync of shard {} failed; still serving generation {}: {}", shard,
                            Optional.ofNullable(served.get(shard)).map(g -> String.valueOf(g.value())).orElse("none"),
                            e.toString());
                }
            }
            if (lazy != null) lazy.sweep();
            try {
                syncSynonyms();
            } catch (IOException | RuntimeException e) {
                log.warn("Syncing the synonyms file failed; keeping the local copy: {}", e.toString());
            }
        }
    }

    /** Runs {@link #syncOnce} now and then every {@code pollInterval} on one daemon thread. */
    public void start() {
        start(Duration.ZERO);
    }

    /**
     * Runs {@link #syncOnce} after {@code initialDelay}, then every {@code pollInterval}. With a
     * {@link LongPollingSource} (peer-pull) the passes are driven by its long-polls instead: a pass
     * runs as soon as a wanted shard's writer reports a newer generation, and at least once per the
     * source's refresh interval; {@code initialDelay} is then replaced by the first wait (a zero
     * delay runs a pass first).
     */
    public void start(Duration initialDelay) {
        synchronized (lifecycleLock) {
            if (closed) throw new IllegalStateException("syncer is closed");
            if (scheduler != null || longPollThread != null) return;
            if (longPoll != null) {
                boolean passFirst = initialDelay.isZero() || initialDelay.isNegative();
                longPollThread = new Thread(() -> longPollLoop(passFirst), "pharos-replica-longpoll");
                longPollThread.setDaemon(true);
                longPollThread.start();
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-replica-syncer");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::syncQuietly, initialDelay.toMillis(), pollInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops serving shards no longer wanted (pinned away, or gone from the catalog): unregisters and
     * deactivates them at once, and deletes their directories on the next pass so queries already
     * reading them finish.
     */
    private void dropUnwanted(List<ShardId> wanted) {
        Set<ShardId> keep = new HashSet<>(wanted);
        long now = clock.getAsLong();
        if (!swept) {
            swept = true;
            sweepStrays(keep, now);
        }
        long graceMillis = Math.max(retireGrace.toMillis(), pollInterval.toMillis());
        for (Map.Entry<ShardId, Long> e : List.copyOf(retired.entrySet())) {
            ShardId shard = e.getKey();
            if (keep.contains(shard)) continue;
            if (now - e.getValue() < graceMillis) continue;
            try {
                deleteRecursively(cacheDir.resolve(shard.pathSegment()));
                retired.remove(shard);
            } catch (IOException | RuntimeException ex) {
                log.warn("Shard {}: removing its cache dir failed (retried next pass): {}", shard, ex.toString());
            }
        }
        for (ShardId shard : served.keySet()) {
            if (keep.contains(shard)) continue;
            Path active = cache.active(shard.value()).orElse(null);
            try {
                registry.unregister(shard.value());
            } catch (RuntimeException ex) {
                log.warn("Shard {}: unregistering failed (retried next pass): {}", shard, ex.toString());
                continue;
            }
            cache.deactivate(shard.value());
            if (active != null) GraphViews.evict(active);
            if (lazy != null) lazy.cancelShard(shard.value());
            served.remove(shard);
            registered.remove(shard);
            refused.remove(shard);
            retired.put(shard, now);
            log.info("Shard {} is no longer served by this reader", shard);
        }
    }

    /** Once per process: shard dirs left by an earlier run that are neither wanted nor registered retire too. */
    private void sweepStrays(Set<ShardId> keep, long now) {
        try (Stream<Path> s = Files.list(cacheDir)) {
            for (Path p : s.toList()) {
                if (!Files.isDirectory(p)) continue;
                ShardId id = ShardId.fromPathSegment(p.getFileName().toString()).orElse(null);
                if (id == null) continue; // .blocks, .blocks-private, ...
                if (keep.contains(id) || served.containsKey(id) || retired.containsKey(id)
                        || registry.find(id.value()).isPresent()) continue;
                retired.put(id, now);
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Sweeping {} for stray shard dirs failed: {}", cacheDir, e.toString());
        }
    }

    /** Long-poll mode: wait for news (or the refresh interval), then a pass; until closed. */
    private void longPollLoop(boolean passFirst) {
        if (passFirst) syncQuietly();
        while (!closed) {
            try {
                longPoll.awaitChange(wanted, served());
            } catch (InterruptedException e) {
                break;
            } catch (RuntimeException e) {
                log.warn("Waiting for new generations failed: {}", e.toString());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    break;
                }
            }
            if (closed) break;
            syncQuietly();
        }
        longPoll.cancelPolls();
    }

    private void syncQuietly() {
        try {
            syncOnce();
        } catch (Exception e) {
            log.warn("Replica sync failed: {}", e.toString());
        }
    }

    @Override
    public void close() {
        ScheduledExecutorService s;
        Thread lp;
        synchronized (lifecycleLock) {
            closed = true;
            s = scheduler;
            scheduler = null;
            lp = longPollThread;
            longPollThread = null;
        }
        if (lp != null) {
            lp.interrupt();
            longPoll.cancelPolls();
            try {
                lp.join(10_000);
                if (lp.isAlive()) log.warn("Replica long-poll did not stop within 10 s");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (s == null) return;
        s.shutdownNow();
        try {
            if (!s.awaitTermination(10, TimeUnit.SECONDS)) log.warn("Replica syncer did not stop within 10 s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------------------------------------------------------------------------------

    private void syncShard(ShardId shard) throws IOException {
        Optional<Generation> latest = source.latest(shard);
        if (latest.isEmpty()) return;
        Generation gen = latest.get();
        Generation current = served.get(shard);
        if (current != null && gen.equals(current)) {
            retryRegistration(shard, current);
            return;
        }
        if (current != null && gen.compareTo(current) < 0) {
            log.warn("Shard {}: LATEST points at generation {}, below the served generation {} (bucket wiped"
                    + " or rolled back?); serving generation {} as a new lineage",
                    shard, gen.value(), current.value(), gen.value());
        }

        ShardManifest manifest = source.manifest(shard, gen);
        if (!formatServable(shard, gen, manifest)) {
            // Stuck on the served generation, whose objects the bucket's GC deletes after its retention:
            // a lazy one is made local in the background before that.
            if (lazy != null) cache.active(shard.value()).ifPresent(dir -> lazy.materialiseInBackground(shard, dir));
            return;
        }
        checkFiles(manifest);
        byte[] manifestJson = manifest.toJson();

        Path shardRoot = cacheDir.resolve(shard.pathSegment());
        Files.createDirectories(shardRoot);
        deletePartials(shardRoot);
        Path genDir = shardRoot.resolve("gen-" + gen.value());
        // A full-mode reader can't serve a lazy dir: it materialises the generation again.
        if (Files.isDirectory(genDir) && sameManifest(genDir, manifestJson)
                && (lazy != null || !LazyGenerations.isLazy(genDir))) {
            log.info("Shard {}: adopting complete generation {} found in {}", shard, gen.value(), genDir);
            // materialised by a build that did not write the stamp yet
            if (!Files.exists(genDir.resolve(IndexFormat.FILE_NAME))) writeFormatStamp(genDir, manifest);
        } else {
            if (cache.active(shard.value()).filter(genDir::equals).isPresent()) {
                throw new IOException("active generation dir " + genDir + " does not match its manifest");
            }
            if (Files.exists(genDir)) { // incomplete or stale leftover
                GraphViews.evict(genDir);
                deleteRecursively(genDir);
            }
            materialise(shard, gen, manifest, manifestJson, shardRoot, genDir);
        }

        // Registry first, then the cache: graphs resolve through the registry's indexPath and
        // Lucene through the cache, so a generation is only activated once the registry names it.
        // A failed registration fails this shard's sync: it keeps serving the previous generation
        // (both views agree) and the next poll adopts the materialised dir and retries.
        register(shard, gen, manifest.projectMeta(), genDir);
        cache.activate(shard.value(), genDir);
        served.put(shard, gen);
        if (lazy != null) lazy.stopMaterialising(shard, genDir);
        prune(shard, shardRoot, gen);
    }

    /**
     * The reader compatibility gate: false (and a WARN once per generation) when the manifest's
     * index format is not servable by this build. The caller then keeps the current generation.
     */
    private boolean formatServable(ShardId shard, Generation gen, ShardManifest manifest) {
        int format = manifest.indexFormatVersion();
        IndexFormat.Status status = IndexFormat.statusOf(format);
        if (IndexFormat.servable(status)) {
            refused.remove(shard);
            return true;
        }
        if (!gen.equals(refused.put(shard, gen))) {
            String serving = Optional.ofNullable(served.get(shard))
                    .map(g -> "generation " + g.value()).orElse("nothing yet");
            String fix = status == IndexFormat.Status.NEWER
                    ? "upgrade pharos on this reader to a version that reads index format " + format
                    : "re-index and re-publish the shard on a writer running a current pharos"
                      + " (pharos index --full " + shard.value() + ")";
            log.warn("Shard {}: not materialising generation {}: its index format {} is {} for this reader"
                    + " (pharos {} reads index formats {}-{}); still serving {}. Fix: {}",
                    shard, gen.value(), format, status.wireName(), com.pharos.PharosVersion.get(),
                    IndexFormat.MIN_READABLE_FORMAT, IndexVersions.INDEX_FORMAT, serving, fix);
        }
        return false;
    }

    /** Carries the writer's stamp into the generation dir; no stamp in the manifest means format 0. */
    private static void writeFormatStamp(Path dir, ShardManifest manifest) {
        if (manifest.indexFormat() != null) IndexFormat.write(dir, manifest.indexFormat());
    }

    private static void checkFiles(ShardManifest manifest) throws IOException {
        GenerationFiles.checkFiles(manifest);
    }

    private void materialise(ShardId shard, Generation gen, ShardManifest manifest, byte[] manifestJson,
                             Path shardRoot, Path genDir) throws IOException {
        long startNanos = System.nanoTime();
        Path partial = shardRoot.resolve(genDir.getFileName() + PARTIAL_SUFFIX);
        Path lucene = partial.resolve(LUCENE_DIR);
        Files.createDirectories(lucene);
        Map<String, Path> reusable = reusableLuceneFiles(shard);
        Set<String> downloadedLucene = new HashSet<>();
        Set<String> localLucene = new HashSet<>();
        int linked = 0, downloaded = 0, remote = 0;
        for (ManifestFile f : manifest.files()) {
            switch (f.role()) {
                case ManifestFile.LUCENE -> {
                    if (lazy != null && !lazy.eager(f)) {
                        remote++; // read through the block cache on demand
                        continue;
                    }
                    localLucene.add(f.name());
                    Path target = lucene.resolve(f.name());
                    if (link(reusable.get(f.key()), target, f)) {
                        linked++;
                    } else {
                        source.download(f, target);
                        downloadedLucene.add(f.name());
                        downloaded++;
                    }
                    checkLength(target, f);
                }
                case ManifestFile.GRAPH -> {
                    Path target = partial.resolve(GraphViews.COMPACT_FILE);
                    source.download(f, target);
                    downloaded++;
                    checkLength(target, f);
                    checkCrc32(target, f);
                }
                default -> { } // refs are not needed by readers; unknown roles are ignored
            }
        }
        checkLuceneChecksums(lucene, manifest, localLucene, downloadedLucene);
        writeFormatStamp(partial, manifest);
        Files.write(partial.resolve(MANIFEST_FILE), manifestJson);
        if (lazy != null) {
            Files.write(partial.resolve(LazyGenerations.MARKER), new byte[0]);
            prefetch(shard, gen, manifest, lucene, reusable);
        }
        Files.move(partial, genDir, StandardCopyOption.ATOMIC_MOVE);
        if (lazy == null) {
            log.info("Shard {}: materialised generation {} ({} files hard-linked, {} downloaded) in {} ms",
                    shard, gen.value(), linked, downloaded, (System.nanoTime() - startNanos) / 1_000_000);
        } else {
            log.info("Shard {}: materialised lazy generation {} ({} files hard-linked, {} downloaded, {} Lucene files"
                    + " remote) in {} ms", shard, gen.value(), linked, downloaded, remote,
                    (System.nanoTime() - startNanos) / 1_000_000);
        }
    }

    /**
     * Lazy mode, before the generation is activated and all within one prefetch timeout: downloads
     * the remote files' head sidecars and prefetches the {@code prefetchExtensions} blocks, then
     * opens (and closes) a reader on the generation, so a query's first open costs no fetch. Neither a
     * timeout nor a failure stops the activation: whatever is missing is fetched on first read.
     */
    private void prefetch(ShardId shard, Generation gen, ShardManifest manifest, Path lucene,
                          Map<String, Path> reusable) throws IOException {
        Duration timeout = lazy.prefetchTimeout();
        long start = System.nanoTime();
        long deadline = start + timeout.toNanos();
        LazyGenerations.Prefetch p = lazy.prefetch(manifest, lucene, reusable);
        if (p.heads() == 0 && p.blocks() == 0) return;
        if (timeout.isZero()) { // configured not to wait
            log.info("Shard {}: prefetching {} heads and {} blocks of generation {} in the background; the warm open"
                    + " is skipped (prefetchTimeoutSeconds is 0)", shard, p.heads(), p.blocks(), gen.value());
            return;
        }
        try {
            p.done().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            log.info("Shard {}: prefetched {} heads and {} blocks ({} bytes) of generation {} in {} ms", shard,
                    p.heads(), p.blocks(), p.bytes(), gen.value(), (System.nanoTime() - start) / 1_000_000);
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("Shard {}: the prefetch of generation {} ({} heads, {} blocks, {} bytes) did not finish within"
                    + " {} s; trying the warm open with up to {} s more, then activating it anyway (the prefetch"
                    + " goes on in the background)", shard, gen.value(), p.heads(), p.blocks(), p.bytes(),
                    timeout.toSeconds(), WARM_OPEN_GRACE.toSeconds());
            warmOpen(shard, gen, lucene, System.nanoTime() + WARM_OPEN_GRACE.toNanos(), WARM_OPEN_GRACE);
            return;
        } catch (java.util.concurrent.ExecutionException e) {
            log.warn("Shard {}: the prefetch of generation {} failed; activating it anyway (blocks are fetched"
                    + " when first read): {}", shard, gen.value(), String.valueOf(e.getCause()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted prefetching generation " + gen.value()
                    + " of shard " + shard);
        }
        warmOpen(shard, gen, lucene, deadline, timeout);
    }

    /**
     * Opens and closes a reader on the not yet activated generation, waiting until {@code deadline};
     * {@code budget} is only for the log.
     */
    private void warmOpen(ShardId shard, Generation gen, Path lucene, long deadline, Duration timeout)
            throws IOException {
        long start = System.nanoTime();
        CompletableFuture<Void> open = CompletableFuture.runAsync(() -> {
            try (Directory dir = lazy.open(lucene)) {
                com.pharos.indexer.LuceneIndexer.warmOpen(dir);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }, r -> {
            Thread t = new Thread(r, "pharos-warm-open-" + shard);
            t.setDaemon(true);
            t.start();
        });
        try {
            open.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            log.info("Shard {}: opened generation {} before activating it in {} ms", shard, gen.value(),
                    (System.nanoTime() - start) / 1_000_000);
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("Shard {}: opening generation {} did not finish within {} s; activating it anyway (the first"
                    + " query opens it)", shard, gen.value(), timeout.toSeconds());
        } catch (java.util.concurrent.ExecutionException e) {
            log.warn("Shard {}: opening generation {} before activating it failed; activating it anyway: {}",
                    shard, gen.value(), String.valueOf(e.getCause()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted opening generation " + gen.value()
                    + " of shard " + shard);
        }
    }

    /** Key → file of the active generation, from its {@code manifest.json}; empty if unreadable. */
    private Map<String, Path> reusableLuceneFiles(ShardId shard) {
        Optional<Path> active = cache.active(shard.value());
        if (active.isEmpty()) return Map.of();
        Map<String, Path> out = new HashMap<>();
        try {
            ShardManifest m = ShardManifest.read(Files.readAllBytes(active.get().resolve(MANIFEST_FILE)));
            for (ManifestFile f : m.files()) {
                if (ManifestFile.LUCENE.equals(f.role()) && f.key() != null && f.name() != null
                        && PLAIN_NAME.matcher(f.name()).matches()) {
                    out.put(f.key(), active.get().resolve(LUCENE_DIR).resolve(f.name()));
                }
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Shard {}: cannot read the active manifest, downloading every file: {}", shard, e.toString());
            return Map.of();
        }
        return out;
    }

    private static boolean link(Path existing, Path target, ManifestFile f) {
        if (existing == null) return false;
        try {
            if (!Files.isRegularFile(existing) || Files.size(existing) != f.length()) return false;
            Files.createLink(target, existing);
            return true;
        } catch (IOException | UnsupportedOperationException | SecurityException e) {
            log.debug("hard link {} -> {} failed, downloading instead: {}", target, existing, e.toString());
            try {
                Files.deleteIfExists(target);
            } catch (IOException ignored) {
                // the download overwrites it
            }
            return false;
        }
    }

    private static void checkLength(Path file, ManifestFile f) throws IOException {
        GenerationFiles.checkLength(file, f);
    }

    private static void checkCrc32(Path file, ManifestFile f) throws IOException {
        GenerationFiles.checkCrc32(file, f);
    }

    /** See {@link GenerationFiles#checkLuceneChecksums}. */
    private static void checkLuceneChecksums(Path lucene, ShardManifest manifest, Set<String> local,
                                             Set<String> downloaded) throws IOException {
        GenerationFiles.checkLuceneChecksums(lucene, manifest, local, downloaded);
    }

    private static boolean sameManifest(Path genDir, byte[] manifestJson) {
        try {
            return Arrays.equals(Files.readAllBytes(genDir.resolve(MANIFEST_FILE)), manifestJson);
        } catch (IOException e) {
            return false;
        }
    }

    private void register(ShardId shard, Generation gen, ProjectMeta published, Path genDir) throws IOException {
        ProjectMeta meta = published != null ? ShardManifest.stripRefs(published) : new ProjectMeta();
        meta.setName(shard.value());
        meta.setIndexPath(genDir.toString()); // the writer's indexPath means nothing here
        try {
            registry.register(meta);
            registered.put(shard, gen);
        } catch (RuntimeException e) {
            throw new IOException("registering generation " + gen.value() + " failed: " + e, e);
        }
    }

    private void retryRegistration(ShardId shard, Generation current) throws IOException {
        if (current.equals(registered.get(shard))) return;
        Path genDir = cacheDir.resolve(shard.pathSegment()).resolve("gen-" + current.value());
        ShardManifest m = ShardManifest.read(Files.readAllBytes(genDir.resolve(MANIFEST_FILE)));
        register(shard, current, m.projectMeta(), genDir);
    }

    private static void deletePartials(Path shardRoot) throws IOException {
        List<Path> partials;
        try (Stream<Path> s = Files.list(shardRoot)) {
            partials = s.filter(p -> p.getFileName().toString().endsWith(PARTIAL_SUFFIX)).toList();
        }
        for (Path p : partials) deleteRecursively(p);
    }

    /** Deletes this shard's generation dirs other than {@code active} and the newest one below it. */
    private void prune(ShardId shard, Path shardRoot, Generation active) {
        try {
            List<Long> gens = new ArrayList<>();
            try (Stream<Path> s = Files.list(shardRoot)) {
                for (Path p : s.toList()) {
                    Matcher m = GEN_DIR.matcher(p.getFileName().toString());
                    if (m.matches() && Files.isDirectory(p)) gens.add(Long.parseLong(m.group(1)));
                }
            }
            long previous = gens.stream().filter(g -> g < active.value()).max(Comparator.naturalOrder()).orElse(-1L);
            // Never delete what the registry (graph reads, other processes on this cacheDir) or
            // the cache (Lucene reads) still points at.
            Set<Path> inUse = new HashSet<>();
            registry.find(shard.value()).map(ProjectMeta::getIndexPath)
                    .ifPresent(p -> inUse.add(Path.of(p).toAbsolutePath().normalize()));
            cache.active(shard.value()).ifPresent(p -> inUse.add(p.toAbsolutePath().normalize()));
            for (long g : gens) {
                if (g == active.value() || g == previous) continue;
                Path dir = shardRoot.resolve("gen-" + g);
                if (inUse.contains(dir.toAbsolutePath().normalize())) {
                    log.info("Shard {}: keeping generation dir {} (still referenced)", shard, dir);
                    continue;
                }
                GraphViews.evict(dir);
                deleteRecursively(dir);
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Shard {}: pruning old generations failed: {}", shard, e.toString());
        }
    }

    private void syncSynonyms() throws IOException {
        if (synonymsFile == null) return;
        Optional<byte[]> remote = source.synonyms();
        if (remote.isEmpty()) return;
        Path target = synonymsFile.toAbsolutePath();
        if (Files.isRegularFile(target) && Arrays.equals(Files.readAllBytes(target), remote.get())) return;
        Path parent = target.getParent();
        Files.createDirectories(parent);
        Path tmp = Files.createTempFile(parent, "." + target.getFileName(), ".tmp");
        try {
            Files.write(tmp, remote.get());
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("Updated synonyms file {} from the cluster catalog", target);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        List<Path> paths;
        try (Stream<Path> s = Files.walk(dir)) {
            paths = s.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path p : paths) Files.deleteIfExists(p);
    }
}
