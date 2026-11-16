package com.pharos.cluster;

import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.GraphViews;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

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
 * shards continue. A complete generation dir found on disk with an identical {@code manifest.json}
 * (a restart) is adopted without downloading.
 */
public class ReplicaSyncer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicaSyncer.class);

    static final String MANIFEST_FILE = "manifest.json";
    static final String LUCENE_DIR = "lucene";
    private static final String PARTIAL_SUFFIX = ".partial";
    private static final Pattern GEN_DIR = Pattern.compile("gen-(\\d{1,18})");
    /** A plain file name: no separators, no {@code .}/{@code ..}, nothing absolute. */
    private static final Pattern PLAIN_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,254}");

    private final ShardCatalog catalog;
    private final ReplicaSource source;
    private final ReaderLocalCache cache;
    private final ProjectRegistry registry;
    private final Path cacheDir;
    private final Path synonymsFile;
    private final Duration pollInterval;

    private final Map<ShardId, Generation> served = new ConcurrentHashMap<>();
    /** Generation last registered per shard; re-tried when it lags {@link #served}. */
    private final Map<ShardId, Generation> registered = new HashMap<>();
    private final Object syncLock = new Object();
    private final Object lifecycleLock = new Object();
    private ScheduledExecutorService scheduler;
    private volatile boolean closed;

    /**
     * @param synonymsFile local synonyms file kept equal to the published one; {@code null} skips it
     */
    public ReplicaSyncer(ShardCatalog catalog, ReplicaSource source, ReaderLocalCache cache,
                         ProjectRegistry registry, Path cacheDir, Path synonymsFile, Duration pollInterval) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.source = Objects.requireNonNull(source, "source");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
        this.synonymsFile = synonymsFile;
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
    }

    /** Generation currently served per shard. */
    public Map<ShardId, Generation> served() {
        return Map.copyOf(served);
    }

    /**
     * One pass over the catalog. Per-shard failures are logged, never thrown.
     *
     * @throws IOException when the catalog or the synonyms file cannot be read or written
     */
    public void syncOnce() throws IOException {
        synchronized (syncLock) {
            List<ShardId> shards;
            try {
                shards = catalog.shards();
            } catch (RuntimeException e) {
                throw new IOException("unreadable shard catalog: " + e.getMessage(), e);
            }
            for (ShardId shard : shards) {
                if (closed) return;
                try {
                    syncShard(shard);
                } catch (IOException | RuntimeException e) {
                    log.warn("Sync of shard {} failed; still serving generation {}: {}", shard,
                            Optional.ofNullable(served.get(shard)).map(g -> String.valueOf(g.value())).orElse("none"),
                            e.toString());
                }
            }
            syncSynonyms();
        }
    }

    /** Runs {@link #syncOnce} now and then every {@code pollInterval} on one daemon thread. */
    public void start() {
        synchronized (lifecycleLock) {
            if (closed) throw new IllegalStateException("syncer is closed");
            if (scheduler != null) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-replica-syncer");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::syncQuietly, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
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
        synchronized (lifecycleLock) {
            closed = true;
            s = scheduler;
            scheduler = null;
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
        if (current != null && gen.compareTo(current) <= 0) {
            retryRegistration(shard, current);
            return;
        }

        ShardManifest manifest = source.manifest(shard, gen);
        checkFiles(manifest);
        byte[] manifestJson = manifest.toJson();

        Path shardRoot = cacheDir.resolve(shard.value());
        Files.createDirectories(shardRoot);
        deletePartials(shardRoot);
        Path genDir = shardRoot.resolve("gen-" + gen.value());
        if (Files.isDirectory(genDir) && sameManifest(genDir, manifestJson)) {
            log.info("Shard {}: adopting complete generation {} found in {}", shard, gen.value(), genDir);
        } else {
            if (cache.active(shard.value()).filter(genDir::equals).isPresent()) {
                throw new IOException("active generation dir " + genDir + " does not match its manifest");
            }
            if (Files.exists(genDir)) deleteRecursively(genDir); // incomplete or stale leftover
            materialise(shard, gen, manifest, manifestJson, shardRoot, genDir);
        }

        cache.activate(shard.value(), genDir);
        served.put(shard, gen);
        register(shard, gen, manifest.projectMeta(), genDir);
        prune(shard, shardRoot, gen);
    }

    /** Every entry needs a role and a plain file name; at least one Lucene file, at most one graph. */
    private static void checkFiles(ShardManifest manifest) throws IOException {
        Set<String> luceneNames = new HashSet<>();
        int graphs = 0;
        for (ManifestFile f : manifest.files()) {
            if (f.role() == null) throw new IOException("manifest gen-" + manifest.generation() + " has no role in " + f);
            if (f.name() == null || !PLAIN_NAME.matcher(f.name()).matches()) {
                throw new IOException("manifest gen-" + manifest.generation() + " has an unsafe file name: " + f);
            }
            if (f.length() < 0) throw new IOException("negative length in " + f);
            if (ManifestFile.LUCENE.equals(f.role()) && !luceneNames.add(f.name())) {
                throw new IOException("duplicate Lucene file " + f.name());
            }
            if (ManifestFile.GRAPH.equals(f.role())) graphs++;
        }
        if (luceneNames.isEmpty()) throw new IOException("manifest gen-" + manifest.generation() + " has no Lucene files");
        if (graphs > 1) throw new IOException("manifest gen-" + manifest.generation() + " lists " + graphs + " graphs");
    }

    private void materialise(ShardId shard, Generation gen, ShardManifest manifest, byte[] manifestJson,
                             Path shardRoot, Path genDir) throws IOException {
        Path partial = shardRoot.resolve(genDir.getFileName() + PARTIAL_SUFFIX);
        Path lucene = partial.resolve(LUCENE_DIR);
        Files.createDirectories(lucene);
        Map<String, Path> reusable = reusableLuceneFiles(shard);
        int linked = 0, downloaded = 0;
        for (ManifestFile f : manifest.files()) {
            switch (f.role()) {
                case ManifestFile.LUCENE -> {
                    Path target = lucene.resolve(f.name());
                    if (link(reusable.get(f.key()), target, f)) {
                        linked++;
                    } else {
                        source.download(f, target);
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
        checkLuceneFooters(lucene, manifest);
        Files.write(partial.resolve(MANIFEST_FILE), manifestJson);
        Files.move(partial, genDir, StandardCopyOption.ATOMIC_MOVE);
        log.info("Shard {}: materialised generation {} ({} files hard-linked, {} downloaded)",
                shard, gen.value(), linked, downloaded);
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
        long actual = Files.size(file);
        if (actual != f.length()) {
            throw new IOException("integrity check failed for " + f.key() + ": " + actual
                    + " bytes, manifest says " + f.length());
        }
    }

    private static void checkCrc32(Path file, ManifestFile f) throws IOException {
        CRC32 crc = new CRC32();
        try (InputStream in = new CheckedInputStream(Files.newInputStream(file), crc)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        if (crc.getValue() != parseChecksum(f)) {
            throw new IOException("integrity check failed for " + f.key() + ": CRC32 "
                    + Long.toHexString(crc.getValue()) + ", manifest says " + f.checksum());
        }
    }

    /** Lucene footer checksums must match the manifest, and the commit must be readable. */
    private static void checkLuceneFooters(Path lucene, ShardManifest manifest) throws IOException {
        try (Directory dir = FSDirectory.open(lucene)) {
            for (ManifestFile f : manifest.files()) {
                if (!ManifestFile.LUCENE.equals(f.role())) continue;
                long actual;
                try (IndexInput in = dir.openInput(f.name(), IOContext.READONCE)) {
                    actual = CodecUtil.retrieveChecksum(in);
                }
                if (actual != parseChecksum(f)) {
                    throw new IOException("integrity check failed for " + f.key() + ": footer checksum "
                            + Long.toHexString(actual) + ", manifest says " + f.checksum());
                }
            }
            SegmentInfos.readLatestCommit(dir);
        }
    }

    private static long parseChecksum(ManifestFile f) throws IOException {
        try {
            return Long.parseUnsignedLong(Objects.requireNonNull(f.checksum(), "checksum"), 16);
        } catch (NumberFormatException | NullPointerException e) {
            throw new IOException("invalid checksum in " + f);
        }
    }

    private static boolean sameManifest(Path genDir, byte[] manifestJson) {
        try {
            return Arrays.equals(Files.readAllBytes(genDir.resolve(MANIFEST_FILE)), manifestJson);
        } catch (IOException e) {
            return false;
        }
    }

    private void register(ShardId shard, Generation gen, ProjectMeta published, Path genDir) {
        ProjectMeta meta = published != null ? ShardManifest.stripRefs(published) : new ProjectMeta();
        meta.setName(shard.value());
        meta.setIndexPath(genDir.toString()); // the writer's indexPath means nothing here
        try {
            registry.register(meta);
            registered.put(shard, gen);
        } catch (RuntimeException e) {
            log.warn("Shard {}: serving generation {} but registering it failed (will retry): {}",
                    shard, gen.value(), e.toString());
        }
    }

    private void retryRegistration(ShardId shard, Generation current) throws IOException {
        if (current.equals(registered.get(shard))) return;
        Path genDir = cacheDir.resolve(shard.value()).resolve("gen-" + current.value());
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
            for (long g : gens) {
                if (g == active.value() || g == previous) continue;
                deleteRecursively(shardRoot.resolve("gen-" + g));
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
