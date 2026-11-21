package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.GenerationFiles;
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
import java.net.URI;
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
        if (r.statusCode() == 404) {
            holders.remove(shard);
            return Optional.empty();
        }
        if (r.statusCode() != 200) {
            throw new IOException("writer " + w + ": LATEST of shard " + shard + " answered "
                    + PeerHttp.describe(r.statusCode(), r.body()));
        }
        Generation gen = parseLatest(w, shard, r.body());
        holders.put(shard, new Holders(gen.value(), parsePeers(r.body())));
        return Optional.of(gen);
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
        if (fromPeersToFile(file, -1, -1, target)) return;
        try (InputStream in = open(file, -1, -1)) {
            write(in, target, -1, file.key());
        }
        fromWriter.incrementAndGet();
    }

    @Override
    public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
        com.pharos.cluster.objectstore.ObjectStore.checkRange(offset, length);
        if (length == 0) {
            write(InputStream.nullInputStream(), target, 0, file.key());
            return;
        }
        if (fromPeersToFile(file, offset, length, target)) return;
        try (InputStream in = open(file, offset, length)) {
            write(in, target, length, file.key());
        }
        fromWriter.incrementAndGet();
    }

    @Override
    public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
        com.pharos.cluster.objectstore.ObjectStore.checkRange(offset, length);
        if (length == 0) return InputStream.nullInputStream();
        if (length <= MAX_PEER_BUFFER) {
            Optional<byte[]> fromPeer = fromPeersToMemory(file, offset, length);
            if (fromPeer.isPresent()) return new java.io.ByteArrayInputStream(fromPeer.get());
        }
        fromWriter.incrementAndGet();
        return open(file, offset, length);
    }

    // ── peer readers (Task 3) ─────────────────────────────────────────────────

    /** At most this many peer readers are tried for one file or range before the writer. */
    static final int MAX_PEER_TRIES = 2;
    /** A peer that served bytes not matching the manifest (or failed) is skipped this long. */
    static final Duration SUSPECT_FOR = Duration.ofMinutes(2);
    /** Headers of a peer's answer must arrive within this; a slow peer must not slow a cold read much. */
    static final Duration PEER_TIMEOUT = Duration.ofSeconds(10);
    /** {@link #openRange} buffers a peer's range to verify it before handing it out; longer ranges use the writer. */
    static final long MAX_PEER_BUFFER = 16L << 20;

    /** The peer readers the writer's last {@code latest} listed for a shard, holding {@code generation} in full. */
    private record Holders(long generation, List<URI> peers) {}

    private final Map<ShardId, Holders> holders = new ConcurrentHashMap<>();
    /** Peer URL → until when (this source's clock) it is skipped. */
    private final Map<URI, Long> suspects = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong filesFromPeers = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong rangesFromPeers = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong fromWriter = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong peerRejects = new java.util.concurrent.atomic.AtomicLong();
    /** This reader's own advertised base URL (never fetched from); null when it advertises none. */
    private volatile URI self;
    /** May this peer URL receive the cluster token? Default: {@link PeerDiscovery#trustedPeer}. */
    private volatile java.util.function.Predicate<URI> trusted = this::trustedByDiscovery;

    /** This reader's {@code cluster.advertiseUrl}: a writer may list it, and it never fetches from itself. */
    public void setSelf(String advertiseUrl) {
        URI u = null;
        if (advertiseUrl != null && !advertiseUrl.isBlank()) {
            try {
                u = PeerDiscovery.baseUrl(advertiseUrl.strip());
            } catch (IllegalArgumentException e) {
                u = null;
            }
        }
        this.self = u;
    }

    /** Test hook: the trust rule for peer URLs. */
    void setTrusted(java.util.function.Predicate<URI> trusted) { this.trusted = Objects.requireNonNull(trusted); }

    private boolean trustedByDiscovery(URI peer) {
        Set<String> writerHosts = new java.util.HashSet<>();
        for (PeerCatalog.WriterStatus w : catalog.writers()) {
            try {
                String h = URI.create(w.url()).getHost();
                if (h != null) writerHosts.add(h.toLowerCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                // not a URL: no host to trust
            }
        }
        return catalog.discovery().trustedPeer(peer, writerHosts);
    }

    /**
     * Counters of peer fetching for {@code /health}: files and ranges served by peers, transfers that
     * went to the writer, peer transfers rejected (bytes not matching the manifest, or a failure),
     * and the peers currently suspect.
     */
    public Map<String, Object> peerFetchStats() {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("filesFromPeers", filesFromPeers.get());
        out.put("rangesFromPeers", rangesFromPeers.get());
        out.put("fromWriter", fromWriter.get());
        out.put("rejected", peerRejects.get());
        long now = clock.getAsLong();
        out.put("suspects", suspects.entrySet().stream().filter(e -> now - e.getValue() < 0)
                .map(e -> e.getKey().toString()).sorted().toList());
        return out;
    }

    /** The peers listed for {@code key}'s shard, as base URLs: the writer's latest answer {@code peers}. */
    private static List<URI> parsePeers(byte[] latestBody) {
        JsonNode peers;
        try {
            JsonNode n = JSON.readTree(latestBody);
            peers = n == null ? null : n.get("peers");
        } catch (IOException e) {
            return List.of();
        }
        if (peers == null || !peers.isArray()) return List.of(); // an older writer lists none
        List<URI> out = new ArrayList<>();
        for (JsonNode p : peers) {
            if (!p.isTextual() || out.size() >= ReplicationEndpoint.MAX_LISTED_PEERS * 2) continue;
            try {
                URI u = PeerDiscovery.baseUrl(p.asText().strip());
                if (!out.contains(u)) out.add(u);
            } catch (IllegalArgumentException notAUrl) {
                // skip it
            }
        }
        return List.copyOf(out);
    }

    /** At most {@link #MAX_PEER_TRIES} listed peers for the shard: not this reader, not suspect, trusted. */
    private List<URI> peersFor(ShardId shard) {
        Holders h = holders.get(shard);
        if (h == null || h.peers().isEmpty()) return List.of();
        List<URI> shuffled = new ArrayList<>(h.peers());
        java.util.Collections.shuffle(shuffled);
        long now = clock.getAsLong();
        List<URI> out = new ArrayList<>(MAX_PEER_TRIES);
        for (URI p : shuffled) {
            if (out.size() == MAX_PEER_TRIES) break;
            if (p.equals(self)) continue;
            Long until = suspects.get(p);
            if (until != null) {
                if (now - until < 0) continue;
                suspects.remove(p, until);
            }
            if (!trusted.test(p)) continue;
            out.add(p);
        }
        return out;
    }

    /** True when the bytes of {@code f} can be checked against the manifest (else only the writer is used). */
    private static boolean verifiable(ManifestFile f, boolean ranged) {
        if (ManifestFile.LUCENE.equals(f.role())) {
            return com.pharos.cluster.warm.CachingRemoteIndexInput.manifestFooter(f) != null;
        }
        if (ranged) return false; // only Lucene files carry a footer a range can be checked against
        try {
            GenerationFiles.parseChecksum(f);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Tries the listed peers for {@code f} (whole when {@code length} &lt; 0) into {@code target}:
     * true once one peer's bytes were verified and moved into place.
     */
    private boolean fromPeersToFile(ManifestFile f, long offset, long length, Path target) throws IOException {
        if (!verifiable(f, length >= 0)) return false;
        List<URI> peers = peersFor(shardOf(f.key()));
        if (peers.isEmpty()) return false;
        Path dir = target.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        for (URI peer : peers) {
            Path tmp = Files.createTempFile(dir, "." + target.getFileName(), ".peer.part");
            try {
                boolean ok;
                try (OutputStream out = Files.newOutputStream(tmp)) {
                    ok = fromPeer(peer, f, offset, length, out);
                }
                if (ok) {
                    try {
                        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    } catch (AtomicMoveNotSupportedException e) {
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                    return true;
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        }
        return false;
    }

    /** {@link #fromPeersToFile} for a range kept in memory ({@link #openRange}). */
    private Optional<byte[]> fromPeersToMemory(ManifestFile f, long offset, long length) throws IOException {
        if (!verifiable(f, true)) return Optional.empty();
        for (URI peer : peersFor(shardOf(f.key()))) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream((int) length);
            if (fromPeer(peer, f, offset, length, out)) return Optional.of(out.toByteArray());
        }
        return Optional.empty();
    }

    /**
     * One peer transfer of {@code f} ({@code length} &lt; 0: the whole file) into {@code out}, checked
     * against the manifest as it streams ({@link PeerCheck}): true when it was. A peer that does not
     * hold the file (404, 410) or is busy (503) is just passed over; any other failure, and bytes that
     * do not match, mark it suspect for {@link #SUSPECT_FOR}. Never throws for the peer's sake: the
     * writer is the fallback.
     */
    private boolean fromPeer(URI peer, ManifestFile f, long offset, long length, OutputStream out)
            throws IOException {
        boolean whole = length < 0;
        long off = whole ? 0 : offset;
        long len = whole ? f.length() : length;
        ShardId shard = shardOf(f.key());
        HttpRequest.Builder req = http.request(peer, filePath(shard, f.key()), PEER_TIMEOUT);
        if (!whole) req.header("Range", "bytes=" + off + "-" + (off + len - 1));
        HttpResponse<InputStream> r;
        try {
            r = http.sendOnce(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (java.io.InterruptedIOException e) {
            throw e;
        } catch (IOException e) {
            suspect(peer, f, "unreachable: " + e);
            return false;
        }
        int status = r.statusCode();
        if (status == 404 || status == 410 || status == 503) { // does not hold it (any more), or busy
            PeerHttp.discard(r);
            log.debug("peer {} did not serve {}: {}", peer, f.key(), status);
            return false;
        }
        if (status != (whole ? 200 : 206)) {
            PeerHttp.discard(r);
            suspect(peer, f, "answered " + status);
            return false;
        }
        if (!whole) {
            String want = "bytes " + off + "-" + (off + len - 1) + "/" + f.length();
            String cr = r.headers().firstValue("Content-Range").orElse("");
            if (!cr.equals(want)) {
                PeerHttp.discard(r);
                suspect(peer, f, "answered Content-Range '" + cr + "' for '" + want + "'");
                return false;
            }
        }
        PeerCheck check;
        try {
            check = PeerCheck.of(f, off, len, whole);
        } catch (IOException unverifiable) {
            PeerHttp.discard(r);
            return false;
        }
        String mismatch;
        try (InputStream in = http.guard(r.body(), f.key() + " from peer " + peer)) {
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (!check.update(buf, 0, n)) break; // more bytes than asked for: stop reading
                out.write(buf, 0, n);
            }
            mismatch = check.finish();
        } catch (java.io.InterruptedIOException e) {
            throw e;
        } catch (IOException e) {
            suspect(peer, f, "transfer failed: " + e);
            return false;
        }
        if (mismatch != null) {
            peerRejects.incrementAndGet();
            suspect(peer, f, mismatch);
            return false;
        }
        (whole ? filesFromPeers : rangesFromPeers).incrementAndGet();
        return true;
    }

    private void suspect(URI peer, ManifestFile f, String why) {
        suspects.put(peer, clock.getAsLong() + SUSPECT_FOR.toNanos());
        log.warn("Peer reader {} failed {}: {}; skipping it for {} s and using the writer", peer, f.key(), why,
                SUSPECT_FOR.toSeconds());
    }

    /**
     * Checks the bytes of a peer transfer against the manifest as they stream past (Review Focus 2):
     * the exact length always; a whole file's checksum (Lucene: the CRC32 of everything before the
     * stored checksum, which is what its footer records, plus the footer itself; graph and refs: the
     * CRC32 of the file); and any bytes of a Lucene file's last 16 that a range covers against the
     * footer built from the manifest checksum (the Phase 2 footer check).
     */
    static final class PeerCheck {
        private final ManifestFile f;
        private final long offset;
        private final long length;
        /** CRC over [0, crcEnd) of the file; -1 for a range. */
        private final long crcEnd;
        private final long expectedCrc;
        private final byte[] footer;
        private final long footerStart;
        private final java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        private long seen;
        private String mismatch;

        private PeerCheck(ManifestFile f, long offset, long length, long crcEnd, long expectedCrc, byte[] footer) {
            this.f = f;
            this.offset = offset;
            this.length = length;
            this.crcEnd = crcEnd;
            this.expectedCrc = expectedCrc;
            this.footer = footer;
            this.footerStart = footer == null ? Long.MAX_VALUE : f.length() - footer.length;
        }

        /** @throws IOException when the manifest gives nothing to check these bytes against */
        static PeerCheck of(ManifestFile f, long offset, long length, boolean whole) throws IOException {
            long expected = GenerationFiles.parseChecksum(f);
            boolean lucene = ManifestFile.LUCENE.equals(f.role());
            byte[] footer = lucene ? com.pharos.cluster.warm.CachingRemoteIndexInput.manifestFooter(f) : null;
            if (lucene && footer == null) throw new IOException("no footer for " + f.key());
            if (!lucene && !whole) throw new IOException("a range of " + f.key() + " cannot be checked");
            long crcEnd = !whole ? -1 : lucene ? f.length() - Long.BYTES : f.length();
            return new PeerCheck(f, offset, length, crcEnd, expected, footer);
        }

        /** Feeds the next bytes; false (and a mismatch) when they run past the expected length. */
        boolean update(byte[] b, int off, int n) {
            if (seen + n > length) {
                mismatch = "more than the " + length + " bytes asked for";
                return false;
            }
            long abs = offset + seen;
            if (crcEnd > abs) crc.update(b, off, (int) Math.min(n, crcEnd - abs));
            if (footer != null && mismatch == null) {
                long from = Math.max(abs, footerStart);
                long to = Math.min(abs + n, footerStart + footer.length);
                for (long p = from; p < to; p++) {
                    if (b[off + (int) (p - abs)] != footer[(int) (p - footerStart)]) {
                        mismatch = "byte " + p + " differs from the footer built from the manifest checksum "
                                + f.checksum();
                        break;
                    }
                }
            }
            seen += n;
            return true;
        }

        /** Null when everything matched; else what did not. */
        String finish() {
            if (mismatch != null) return mismatch;
            if (seen != length) return seen + " bytes, " + length + " expected";
            if (crcEnd >= 0 && crc.getValue() != expectedCrc) {
                return "CRC32 " + Long.toHexString(crc.getValue()) + ", the manifest says " + f.checksum();
            }
            return null;
        }
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
