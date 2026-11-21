package com.pharos.cluster.peer;

import com.pharos.cluster.ReaderLocalCache;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.warm.BlockCache;
import com.pharos.cluster.warm.BlockKey;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.graph.GraphViews;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A peer-mode reader serving the files of the generations it holds in full to other readers
 * (Phase 5 Task 3), so a cold read need not reach the writer:
 * {@code GET /replication/<shard>/files/<key>}, whole files only (P5-R10: a {@code Range} request gets
 * 416, since a peer's range could not be checked end to end against the manifest), with the same key rules ({@link ReplicationEndpoint#validFileKey}: no
 * traversal, data dirs only) and the cluster token on every request.
 *
 * <p><b>What is served.</b> Only files of the shard's <em>active</em> generation, and only while that
 * generation is held in full: every Lucene file and the graph are on local disk with the manifest's
 * length (a full-mode generation; or the eager files of a lazy one), or, in a lazy generation, every
 * block of a remote Lucene file is in the block cache. Anything else is 404 (the asking reader goes
 * on to its next peer or the writer); no request ever makes this reader fetch anything.
 * {@link #generationsHeld} is what the heartbeat advertises ({@code generationsHeld}, P5-R5).
 *
 * <p><b>Own queries first.</b> At most {@code cluster.peers.maxTransfersPerReader} transfers run at
 * once; more get 503 with {@code Retry-After} at once (the asking reader moves on). A transfer reads
 * a file it opened first (a generation pruned mid-transfer stays readable) or leases cached blocks
 * one at a time.
 *
 * <p>Nothing here is trusted by the asking reader: it checks every byte against the manifest.
 */
public final class PeerFileSource {

    private static final Logger log = LoggerFactory.getLogger(PeerFileSource.class);
    private static final Pattern GEN_DIR = Pattern.compile("gen-(\\d{1,18})");
    /** How long the held state of a lazy generation (blocks come and go) is reused. */
    static final long LAZY_RECHECK_NANOS = 5_000_000_000L;

    /** The block cache of a lazy reader; null when it has none. */
    @FunctionalInterface
    public interface BlockCacheSupplier {
        BlockCache get() throws IOException;
    }

    private final ReaderLocalCache cache;
    private final BlockCacheSupplier blocks;
    private final Predicate<String> token;
    private final TransferLimiter transfers;
    private final LongSupplier nanoClock;
    private final AtomicLong served = new AtomicLong();
    /** Parsed manifests and held state per active generation dir. */
    private final Map<Path, Held> heldByDir = new ConcurrentHashMap<>();

    /** One active generation: its files by key, and whether all are held (as of {@code checkedNanos}). */
    private record Held(long generation, Path genDir, boolean lazy, Map<String, ManifestFile> files, boolean full,
                        long checkedNanos) {}

    /**
     * @param blocks       the lazy reader's block cache; null in full mode
     * @param token        the cluster token check ({@link InternalApi#tokenMatcher})
     * @param maxTransfers {@code cluster.peers.maxTransfersPerReader}
     */
    public PeerFileSource(ReaderLocalCache cache, BlockCacheSupplier blocks, Predicate<String> token,
                          int maxTransfers, LongSupplier nanoClock) {
        this.cache = Objects.requireNonNull(cache, "cache");
        this.blocks = blocks;
        this.token = Objects.requireNonNull(token, "token");
        this.transfers = new TransferLimiter(maxTransfers);
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /** The transfer cap. */
    public TransferLimiter transfers() { return transfers; }

    /** File transfers this reader started serving to peers. */
    public long transfersServed() { return served.get(); }

    /**
     * {@code {shard: generation}} of the active generations held in full: the heartbeat's
     * {@code generationsHeld}. A shard whose active generation is partly remote is left out.
     */
    public Map<String, Long> generationsHeld() {
        Map<String, Long> out = new TreeMap<>();
        java.util.Set<Path> active = new java.util.HashSet<>();
        for (String project : cache.activeProjects()) {
            Optional<Path> dir = cache.active(project);
            if (dir.isEmpty()) continue;
            active.add(dir.get());
            held(dir.get()).filter(Held::full).ifPresent(h -> out.put(project, h.generation()));
        }
        heldByDir.keySet().retainAll(active); // retired generations are forgotten
        return out;
    }

    /** The held state of an active generation dir; empty when it is not one or cannot be read. */
    private Optional<Held> held(Path genDir) {
        Held h = heldByDir.get(genDir);
        long now = nanoClock.getAsLong();
        if (h != null && (!h.lazy() || now - h.checkedNanos() < LAZY_RECHECK_NANOS)) return Optional.of(h);
        try {
            var m = GEN_DIR.matcher(genDir.getFileName().toString());
            if (!m.matches()) return Optional.empty();
            long gen = Long.parseLong(m.group(1));
            Map<String, ManifestFile> files;
            if (h != null) {
                files = h.files();
            } else {
                ShardManifest manifest = ShardManifest.read(Files.readAllBytes(genDir.resolve("manifest.json")));
                if (manifest.generation() != gen) return Optional.empty();
                files = new HashMap<>();
                for (ManifestFile f : manifest.files()) {
                    if (ManifestFile.LUCENE.equals(f.role()) || ManifestFile.GRAPH.equals(f.role())) {
                        files.put(f.key(), f);
                    }
                }
                files = Map.copyOf(files);
            }
            boolean lazy = LazyGenerations.isLazy(genDir);
            boolean full = !files.isEmpty();
            for (ManifestFile f : files.values()) {
                if (!full) break;
                full = localFile(genDir, f).isPresent() || (lazy && allBlocksCached(f));
            }
            Held fresh = new Held(gen, genDir, lazy, files, full, now);
            heldByDir.put(genDir, fresh);
            return Optional.of(fresh);
        } catch (IOException | RuntimeException e) {
            log.debug("generation dir {} is not servable to peers: {}", genDir, e.toString());
            return Optional.empty();
        }
    }

    /** The file on local disk with the manifest's length, if it is there. */
    private static Optional<Path> localFile(Path genDir, ManifestFile f) {
        Path p = ManifestFile.GRAPH.equals(f.role()) ? genDir.resolve(GraphViews.COMPACT_FILE)
                : genDir.resolve("lucene").resolve(f.name());
        try {
            return Files.isRegularFile(p) && Files.size(p) == f.length() ? Optional.of(p) : Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private BlockCache blockCache() {
        if (blocks == null) return null;
        try {
            return blocks.get();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private boolean allBlocksCached(ManifestFile f) {
        if (!ManifestFile.LUCENE.equals(f.role())) return false;
        BlockCache bc = blockCache();
        if (bc == null || f.length() == 0) return false;
        long n = (f.length() + bc.blockSize() - 1) / bc.blockSize();
        for (long i = 0; i < n; i++) {
            if (!bc.isCached(new BlockKey(f.key(), i))) return false;
        }
        return true;
    }

    // ── route ─────────────────────────────────────────────────────────────────

    private static final class Refused extends RuntimeException {
        final int status;
        final String error;

        Refused(int status, String error) {
            super(error, null, false, false);
            this.status = status;
            this.error = error;
        }
    }

    /** Mounts {@code GET /replication/<shard>/files/<key>} (and the token filter over {@code /replication/*}). */
    public void mount(Javalin app) {
        app.exception(Refused.class, (e, ctx) -> {
            ctx.status(e.status).contentType("application/json");
            ctx.result("{\"error\":\"" + e.error + "\"}");
        });
        app.before(ReplicationEndpoint.PREFIX + "/*", this::requireToken);
        app.get(ReplicationEndpoint.PREFIX + "/{shard}/files/<key>", ctx -> {
            requireToken(ctx); // checked again: no route reaches a handler without the token
            file(ctx);
        });
    }

    private void requireToken(Context ctx) {
        if (!token.test(ctx.header(InternalApi.TOKEN_HEADER))) throw new Refused(401, "unauthorized");
    }

    private void file(Context ctx) throws IOException {
        ShardId shard;
        try {
            shard = new ShardId(ctx.pathParam("shard"));
        } catch (IllegalArgumentException e) {
            throw new Refused(400, "invalid shard id");
        }
        String key = ctx.pathParam("key");
        if (!ReplicationEndpoint.validFileKey(PeerReplicaSource.LAYOUT, shard, key)) {
            throw new Refused(400, "not a file key of shard " + shard);
        }
        Optional<Path> dir = cache.active(shard.value());
        Optional<Held> held = dir.flatMap(this::held);
        ManifestFile f = held.filter(Held::full).map(h -> h.files().get(key)).orElse(null);
        if (f == null) throw new Refused(404, "this reader does not hold " + key + " in full");
        if (ctx.header("Range") != null) { // whole files only (P5-R10): a range could not be verified end to end
            ctx.header("Content-Range", "bytes */" + f.length());
            throw new Refused(416, "peer readers serve whole files only; fetch ranges from the writer");
        }
        Optional<TransferLimiter.Permit> permit = transfers.tryAcquire();
        if (permit.isEmpty()) {
            ctx.header("Retry-After", Integer.toString(ReplicationEndpoint.RETRY_AFTER_SECONDS));
            throw new Refused(503, "too many peer transfers on this reader (max " + transfers.max() + ")");
        }
        try (TransferLimiter.Permit slot = permit.get()) {
            Optional<Path> local = localFile(dir.get(), f);
            if (local.isPresent()) {
                FileChannel ch;
                try {
                    ch = FileChannel.open(local.get(), StandardOpenOption.READ); // survives a prune from now on
                } catch (NoSuchFileException e) {
                    throw new Refused(404, "this reader no longer holds " + key);
                }
                try (ch) {
                    if (ch.size() != f.length()) throw new Refused(404, "this reader does not hold " + key + " in full");
                    served.incrementAndGet();
                    ReplicationEndpoint.serve(ctx, f.length(), ch::read, null, slot);
                }
                return;
            }
            BlockCache bc = held.get().lazy() ? blockCache() : null;
            if (bc == null || !allBlocksCached(f)) throw new Refused(404, "this reader does not hold " + key + " in full");
            served.incrementAndGet();
            ReplicationEndpoint.serve(ctx, f.length(), new CachedBlocks(bc, f), null, slot);
        }
    }

    /**
     * Positional reads over a file's cached blocks, one short lease at a time. A block evicted since
     * the check is never fetched: the read fails, the transfer is cut short, and the asking reader's
     * length check rejects it.
     */
    private record CachedBlocks(BlockCache cache, ManifestFile f) implements ReplicationEndpoint.Positional {
        private static final BlockCache.BlockFetcher NEVER = (key, offset, length, target) -> {
            throw new IOException("block " + key.index() + " of " + key.objectKey() + " left the cache");
        };

        @Override
        public int read(ByteBuffer buf, long pos) throws IOException {
            if (pos >= f.length()) return -1;
            int bs = cache.blockSize();
            long index = pos / bs;
            long blockStart = index * bs;
            long blockLen = Math.min(bs, f.length() - blockStart);
            BlockKey key = new BlockKey(f.key(), index);
            if (!cache.isCached(key)) throw new IOException("block " + index + " of " + f.key() + " left the cache");
            try (BlockCache.Lease lease = cache.acquire(key, blockLen, NEVER)) {
                int inBlock = (int) (pos - blockStart);
                int n = (int) Math.min(buf.remaining(), blockLen - inBlock);
                MemorySegment.ofBuffer(buf).asSlice(0, n)
                        .copyFrom(lease.segment().asSlice(inBlock, n));
                buf.position(buf.position() + n);
                return n;
            }
        }
    }
}
