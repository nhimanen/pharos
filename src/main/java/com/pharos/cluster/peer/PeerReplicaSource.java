package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.LongPollingSource;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ManifestValidation;
import com.pharos.cluster.objectstore.ObjectMissingException;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.objectstore.SynonymsMerge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A peer-mode reader's {@link ReplicaSource}: each shard is read from the writer that owns it in the
 * {@link PeerCatalog}, over {@code /replication/*} ({@link ReplicationEndpoint}). Everything read is
 * validated as from a bucket: {@code LATEST} must name {@code gen-N.json} of its generation, manifests
 * pass {@link ManifestValidation#check} against the writer's store layout (no prefix), and file
 * lengths are checked here (the syncer checksums them).
 *
 * <ul>
 *   <li>A file the writer pruned (410) throws {@link ObjectMissingException}, as a bucket does, so a
 *       lazy reader degrades the shard and the syncer moves on to the current generation.</li>
 *   <li>503 and connection errors are retried with backoff and jitter ({@link PeerHttp#send}); a
 *       writer that stays unreachable fails the call with {@code writer A (url) is unreachable: …}:
 *       a cold read during a writer outage fails clearly while cached data keeps serving.</li>
 *   <li>{@link #synonyms} merges every writer's {@code /replication/synonyms} (P5-R4).</li>
 *   <li>{@link #awaitChange} long-polls each wanted shard's writer ({@code latest?wait=&after=}).</li>
 * </ul>
 */
public final class PeerReplicaSource implements ReplicaSource, LongPollingSource, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PeerReplicaSource.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern MANIFEST_NAME = Pattern.compile("gen-(\\d+)\\.json");
    /** A writer's store has no prefix: manifest keys are {@code shards/<shard>/…}. */
    static final ObjectStoreLayout LAYOUT = new ObjectStoreLayout("");
    /** Extra time a long-poll request gets on top of its {@code wait}. */
    static final Duration LONG_POLL_SLACK = Duration.ofSeconds(15);
    /** The longest backoff of one shard's long-poll after repeated failures. */
    static final Duration MAX_POLL_BACKOFF = Duration.ofSeconds(30);
    /** A long-poll answered faster than this without news is not re-issued before it passes. */
    static final Duration MIN_POLL_SPACING = Duration.ofSeconds(1);

    private final PeerCatalog catalog;
    private final PeerHttp http;
    private final Duration longPoll;
    private volatile LongSupplier clock = System::nanoTime;

    /** Each writer's last served synonyms, by node id (kept while it is away). */
    private final Map<String, CachedSynonyms> synonymsByWriter = new ConcurrentHashMap<>();
    private volatile List<String> synonymsConflicts = List.of();

    private record CachedSynonyms(String etag, String text) {}

    /**
     * @param longPoll {@code cluster.peers.longPollSeconds}: how long one {@code latest} long-poll waits
     */
    public PeerReplicaSource(PeerCatalog catalog, PeerHttp http, Duration longPoll) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.http = Objects.requireNonNull(http, "http");
        this.longPoll = Objects.requireNonNull(longPoll, "longPoll");
    }

    void setClock(LongSupplier clock) { this.clock = clock; }

    public PeerCatalog catalog() { return catalog; }

    /** {@code synonyms of shard X served by A and B; using A's}, from the last {@link #synonyms}. */
    public List<String> synonymsConflicts() { return synonymsConflicts; }

    // ── ReplicaSource ─────────────────────────────────────────────────────────

    @Override
    public Optional<Generation> latest(ShardId shard) throws IOException {
        PeerCatalog.WriterRef w = writer(shard);
        HttpResponse<byte[]> r = http.send(w.toString(), http.request(w.url(), shardPath(shard) + "/latest",
                http.requestTimeout()).build(), HttpResponse.BodyHandlers.ofByteArray());
        if (r.statusCode() == 404) return Optional.empty();
        if (r.statusCode() != 200) {
            throw new IOException("writer " + w + ": LATEST of shard " + shard + " answered "
                    + PeerHttp.describe(r.statusCode(), r.body()));
        }
        return Optional.of(parseLatest(w, shard, r.body()));
    }

    @Override
    public ShardManifest manifest(ShardId shard, Generation gen) throws IOException {
        PeerCatalog.WriterRef w = writer(shard);
        String name = ObjectStoreLayout.manifestName(gen);
        HttpResponse<byte[]> r = http.send(w.toString(), http.request(w.url(), shardPath(shard) + "/manifests/" + name,
                http.requestTimeout()).build(), HttpResponse.BodyHandlers.ofByteArray());
        String where = "manifest " + name + " of shard " + shard + " on writer " + w;
        if (r.statusCode() == 410) {
            throw new IOException(where + " was pruned: " + PeerHttp.describe(r.statusCode(), r.body()));
        }
        if (r.statusCode() != 200) throw new IOException(where + ": " + PeerHttp.describe(r.statusCode(), r.body()));
        ShardManifest m;
        try {
            m = ShardManifest.read(r.body());
        } catch (IOException e) {
            throw new IOException("unparseable " + where + ": " + e.getMessage(), e);
        }
        ManifestValidation.check(shard, gen, m, LAYOUT, where);
        return m;
    }

    @Override
    public void download(ManifestFile file, Path target) throws IOException {
        try (InputStream in = open(file, -1, -1)) {
            write(in, target, -1, file.key());
        }
    }

    @Override
    public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
        com.pharos.cluster.objectstore.ObjectStore.checkRange(offset, length);
        if (length == 0) {
            write(InputStream.nullInputStream(), target, 0, file.key());
            return;
        }
        try (InputStream in = open(file, offset, length)) {
            write(in, target, length, file.key());
        }
    }

    @Override
    public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
        com.pharos.cluster.objectstore.ObjectStore.checkRange(offset, length);
        if (length == 0) return InputStream.nullInputStream();
        return open(file, offset, length);
    }

    /**
     * The cluster-wide synonyms (P5-R4): every live writer's {@code /replication/synonyms}
     * (conditional on its last {@code ETag}), merged canonically with the shards' owners winning
     * conflicting sections ({@link SynonymsMerge#mergeWriters}). A writer that is missing (not yet
     * dropped) keeps its last file in the merge; a dropped one's is forgotten (P5-R9). Empty when no writer has any.
     */
    @Override
    public Optional<byte[]> synonyms() throws IOException {
        for (PeerCatalog.WriterRef w : catalog.liveWriters()) {
            CachedSynonyms cached = synonymsByWriter.get(w.node());
            HttpRequest.Builder req = http.request(w.url(), ReplicationEndpoint.PREFIX + "/synonyms",
                    http.requestTimeout());
            if (cached != null) req.header("If-None-Match", cached.etag());
            try {
                HttpResponse<byte[]> r = http.send(w.toString(), req.build(), HttpResponse.BodyHandlers.ofByteArray());
                switch (r.statusCode()) {
                    case 200 -> synonymsByWriter.put(w.node(), new CachedSynonyms(
                            r.headers().firstValue("ETag").orElse(""), new String(r.body(), StandardCharsets.UTF_8)));
                    case 304 -> { } // unchanged
                    case 404 -> synonymsByWriter.remove(w.node());
                    default -> log.warn("Synonyms of writer {} answered {}; keeping its last copy", w,
                            PeerHttp.describe(r.statusCode(), r.body()));
                }
            } catch (IOException e) {
                log.debug("Synonyms of writer {} failed; keeping its last copy: {}", w, e.toString());
            }
        }
        // the merge input is exactly the tracked writer set (P5-R9): a dropped writer's file goes with it
        Set<String> tracked = new java.util.HashSet<>();
        catalog.writers().forEach(w -> tracked.add(w.node()));
        synonymsByWriter.keySet().retainAll(tracked);
        Map<String, String> texts = new TreeMap<>();
        synonymsByWriter.forEach((node, c) -> texts.put(node, c.text()));
        List<String> conflicts = new ArrayList<>();
        String merged = SynonymsMerge.mergeWriters(texts, shard -> {
            try {
                return catalog.owner(new ShardId(shard)).map(PeerCatalog.WriterRef::node);
            } catch (IllegalArgumentException notAShard) {
                return Optional.empty();
            }
        }, conflicts::add);
        for (String c : conflicts) if (!synonymsConflicts.contains(c)) log.warn("Conflicting synonyms: {}", c);
        synonymsConflicts = List.copyOf(conflicts);
        return merged.isEmpty() ? Optional.empty() : Optional.of(merged.getBytes(StandardCharsets.UTF_8));
    }

    // ── long-poll ─────────────────────────────────────────────────────────────

    private record Poll(String node, long after, long issuedAt, CompletableFuture<HttpResponse<byte[]>> future) {}

    private final Object pollLock = new Object();
    /** Guarded by pollLock. */
    private final Map<ShardId, Poll> polls = new HashMap<>();
    private final Map<ShardId, Long> notBefore = new HashMap<>();
    private final Map<ShardId, Integer> failures = new HashMap<>();
    /** The newest generation reported per shard, so a shard whose sync fails does not spin. */
    private final Map<ShardId, Long> reported = new ConcurrentHashMap<>();

    @Override
    public void awaitChange(Collection<ShardId> wanted, Map<ShardId, Generation> served) throws InterruptedException {
        long deadline = clock.getAsLong() + catalog.refreshInterval().toNanos();
        Set<ShardId> want = Set.copyOf(wanted);
        synchronized (pollLock) {
            for (ShardId s : List.copyOf(polls.keySet())) {
                if (!want.contains(s)) polls.remove(s).future().cancel(true);
            }
        }
        while (true) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            List<CompletableFuture<?>> waiting = new ArrayList<>();
            long wakeAt = deadline;
            boolean changed = false;
            synchronized (pollLock) {
                long now = clock.getAsLong();
                for (ShardId shard : want) {
                    Optional<PeerCatalog.WriterRef> w = catalog.owner(shard);
                    Poll p = polls.get(shard);
                    // a writer whose lineage restarted below what was reported (store wiped): forget it
                    long listed = catalog.listedLatest(shard).orElse(Long.MAX_VALUE);
                    if (listed < reported.getOrDefault(shard, 0L)) reported.remove(shard);
                    long after = Math.max(Optional.ofNullable(served.get(shard)).map(Generation::value).orElse(0L),
                            reported.getOrDefault(shard, 0L));
                    if (p != null && (w.isEmpty() || !p.node().equals(w.get().node()) || p.after() != after)) {
                        p.future().cancel(true);
                        polls.remove(shard);
                        p = null;
                    }
                    if (w.isEmpty()) continue;
                    if (p == null) {
                        long nb = notBefore.getOrDefault(shard, Long.MIN_VALUE);
                        if (now - nb < 0) {
                            wakeAt = Math.min(wakeAt, nb);
                            continue;
                        }
                        p = issue(shard, w.get(), after, now);
                        polls.put(shard, p);
                    }
                    if (p.future().isDone()) {
                        polls.remove(shard);
                        changed |= settle(shard, p, now);
                    } else {
                        waiting.add(p.future());
                    }
                }
            }
            if (changed) return;
            long now = clock.getAsLong();
            if (now - deadline >= 0) return;
            long waitNanos = Math.max(1, Math.min(wakeAt, deadline) - now);
            if (waiting.isEmpty()) {
                TimeUnit.NANOSECONDS.sleep(Math.min(waitNanos, TimeUnit.MILLISECONDS.toNanos(250)));
                continue;
            }
            try {
                CompletableFuture.anyOf(waiting.toArray(CompletableFuture[]::new)).get(waitNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException | ExecutionException | java.util.concurrent.CancellationException e) {
                // a poll failed or nothing arrived yet: the loop looks at each one
            }
        }
    }

    private Poll issue(ShardId shard, PeerCatalog.WriterRef w, long after, long now) {
        long wait = longPoll.toSeconds();
        HttpRequest req = http.request(w.url(), shardPath(shard) + "/latest?wait=" + wait + "&after=" + after,
                longPoll.plus(LONG_POLL_SLACK)).build();
        return new Poll(w.node(), after, now, http.sendAsync(req));
    }

    /** Looks at a finished long-poll; true when it reported a newer generation. Holding pollLock. */
    private boolean settle(ShardId shard, Poll p, long now) {
        HttpResponse<byte[]> r;
        try {
            r = p.future().getNow(null);
        } catch (RuntimeException e) { // CompletionException or CancellationException
            int n = failures.merge(shard, 1, Integer::sum);
            Duration b = PeerHttp.backoff(n - 1);
            if (b.compareTo(MAX_POLL_BACKOFF) > 0) b = MAX_POLL_BACKOFF;
            notBefore.put(shard, now + b.toNanos());
            log.debug("long-poll of shard {} on {} failed (retry in {} ms): {}", shard, p.node(), b.toMillis(),
                    String.valueOf(e.getCause()));
            return false;
        }
        boolean quick = now - p.issuedAt() < MIN_POLL_SPACING.toNanos();
        switch (r.statusCode()) {
            case 200 -> {
                failures.remove(shard);
                try {
                    long gen = parseLatest(new PeerCatalog.WriterRef(p.node(), null), shard, r.body()).value();
                    if (gen > p.after()) {
                        reported.merge(shard, gen, Math::max);
                        return true;
                    }
                } catch (IOException e) {
                    log.warn("long-poll of shard {} on {}: {}", shard, p.node(), e.getMessage());
                    quick = true;
                }
            }
            case 503 -> {
                notBefore.put(shard, now + PeerHttp.retryAfter(r.headers().firstValue("Retry-After")).toNanos());
                return false;
            }
            default -> {
                int n = failures.merge(shard, 1, Integer::sum);
                Duration b = PeerHttp.backoff(n - 1);
                if (b.compareTo(MAX_POLL_BACKOFF) > 0) b = MAX_POLL_BACKOFF;
                notBefore.put(shard, now + b.toNanos());
                if (r.statusCode() != 404) {
                    log.debug("long-poll of shard {} on {} answered {}", shard, p.node(),
                            PeerHttp.describe(r.statusCode(), r.body()));
                }
                return false;
            }
        }
        if (quick) notBefore.put(shard, now + MIN_POLL_SPACING.toNanos());
        return false;
    }

    @Override
    public void cancelPolls() {
        synchronized (pollLock) {
            polls.values().forEach(p -> p.future().cancel(true));
            polls.clear();
        }
    }

    @Override
    public void close() {
        cancelPolls();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private PeerCatalog.WriterRef writer(ShardId shard) throws IOException {
        Optional<PeerCatalog.WriterRef> w = catalog.owner(shard);
        if (w.isPresent()) return w.get();
        throw new IOException("shard " + shard + " has no live writer in this peer-mode cluster ("
                + (catalog.orphans().contains(shard) ? "its writer was dropped after " + PeerCatalog.DROP_AFTER
                + " missed refreshes" : "no discovered writer lists it") + "); only its cached generation can be read");
    }

    private static String shardPath(ShardId shard) {
        return ReplicationEndpoint.PREFIX + "/" + shard.value();
    }

    /** The shard of a manifest key {@code shards/<shard>/…}. */
    static ShardId shardOf(String key) throws IOException {
        String[] seg = key == null ? new String[0] : key.split("/", 3);
        if (seg.length < 3 || !seg[0].equals("shards")) throw new IOException("not a peer-store file key: " + key);
        try {
            return new ShardId(seg[1]);
        } catch (IllegalArgumentException e) {
            throw new IOException("not a peer-store file key: " + key);
        }
    }

    private static String filePath(ShardId shard, String key) {
        StringBuilder sb = new StringBuilder(shardPath(shard)).append("/files");
        for (String seg : key.split("/")) sb.append('/').append(URLEncoder.encode(seg, StandardCharsets.UTF_8));
        return sb.toString();
    }

    /**
     * The bytes of a file ({@code length} &lt; 0: the whole file) from its shard's writer; the stream
     * is cut at exactly {@code length} bytes.
     */
    private InputStream open(ManifestFile file, long offset, long length) throws IOException {
        ShardId shard = shardOf(file.key());
        PeerCatalog.WriterRef w = writer(shard);
        HttpRequest.Builder req = http.request(w.url(), filePath(shard, file.key()), http.requestTimeout());
        if (length >= 0) req.header("Range", "bytes=" + offset + "-" + (offset + length - 1));
        HttpResponse<InputStream> r = http.send(w.toString(), req.build(), HttpResponse.BodyHandlers.ofInputStream());
        int status = r.statusCode();
        boolean ok = length >= 0 ? status == 206 : status == 200;
        if (ok && length >= 0) {
            String cr = r.headers().firstValue("Content-Range").orElse("");
            if (!cr.startsWith("bytes " + offset + "-" + (offset + length - 1) + "/")) {
                PeerHttp.discard(r);
                throw new IOException("writer " + w + " answered range " + offset + "+" + length + " of " + file.key()
                        + " with Content-Range '" + cr + "'");
            }
        }
        if (ok) {
            InputStream body = http.guard(r.body(), file.key() + " from writer " + w);
            return length >= 0 ? new Limited(body, length) : body;
        }
        byte[] body;
        try (InputStream in = r.body()) {
            body = in.readNBytes(300);
        }
        String why = PeerHttp.describe(status, body);
        if (status == 410) { // pruned by the writer's GC: the same signal as a key gone from a bucket
            log.debug("{} was pruned on writer {}: {}", file.key(), w, why);
            throw new ObjectMissingException(file.key());
        }
        if (status == 416) throw new EOFException("range " + offset + "+" + length + " is past the end of "
                + file.key() + " on writer " + w);
        throw new IOException("writer " + w + ": " + file.key() + " answered " + why);
    }

    /** Streams to a temp file beside {@code target}, checks the length ({@code expected} ≥ 0), then moves it. */
    private static void write(InputStream in, Path target, long expected, String key) throws IOException {
        Path dir = target.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, "." + target.getFileName(), ".part");
        try {
            long n;
            try (OutputStream out = Files.newOutputStream(tmp)) {
                n = in.transferTo(out);
            }
            if (expected >= 0 && n != expected) {
                throw new IOException("short body for " + key + ": " + n + " of " + expected + " bytes");
            }
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static Generation parseLatest(PeerCatalog.WriterRef w, ShardId shard, byte[] body) throws IOException {
        String where = "LATEST of shard " + shard + " on writer " + w.node();
        JsonNode node;
        try {
            node = JSON.readTree(body);
        } catch (IOException e) {
            throw new IOException("unreadable " + where + ": " + e.getMessage(), e);
        }
        JsonNode g = node == null ? null : node.get("generation");
        JsonNode m = node == null ? null : node.get("manifest");
        if (g == null || !g.isIntegralNumber() || !g.canConvertToLong() || g.asLong() <= 0) {
            throw new IOException(where + " has no valid generation");
        }
        if (m == null || !m.isTextual()) throw new IOException(where + " has no manifest name");
        Matcher mm = MANIFEST_NAME.matcher(m.asText());
        long named;
        try {
            named = mm.matches() ? Long.parseLong(mm.group(1)) : -1;
        } catch (NumberFormatException e) {
            named = -1;
        }
        if (named != g.asLong()) throw new IOException(where + " names manifest '" + m.asText() + "' for generation "
                + g.asLong());
        return new Generation(g.asLong());
    }

    /** Exactly {@code limit} bytes of a body at most; closing closes the body. */
    private static final class Limited extends FilterInputStream {
        private long left;

        Limited(InputStream in, long limit) {
            super(in);
            this.left = limit;
        }

        @Override
        public int read() throws IOException {
            if (left <= 0) return -1;
            int b = super.read();
            if (b >= 0) left--;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (left <= 0) return -1;
            int n = super.read(buf, off, (int) Math.min(len, left));
            if (n > 0) left -= n;
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long s = super.skip(Math.min(n, left));
            left -= s;
            return s;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(super.available(), left);
        }
    }
}
