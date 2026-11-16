package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deletes generations of a shard that no reader can still be using.
 *
 * <p>Kept: the manifest {@code LATEST} points at (always, with everything it references); the
 * manifests of the newest {@code keepGenerations} generations at or below {@code LATEST} (by
 * generation number); and any manifest younger than {@code minAge} (by {@code createdAt} or by
 * store {@code lastModified}). Orphan manifests newer than {@code LATEST} are kept only while
 * young, since an in-flight publish may be about to CAS {@code LATEST}. Every other manifest is
 * deleted; then {@code lucene/}, {@code graph/} and {@code refs/} objects referenced by no kept
 * manifest and last modified more than {@code minAge} before {@code now} are deleted.
 *
 * <p>Safety: GC deletes nothing when {@code LATEST} is missing or unreadable, when any listing or
 * manifest read fails, or when any manifest is unparseable or fails validation (unsupported
 * {@code formatVersion}, {@code generation} or {@code shard} not matching its key, no files, or a
 * file key that is blank or outside this shard). Deletes are best-effort: a failed delete is
 * logged and the rest continue. Manifests are deleted before data files, so a crash mid-GC leaves
 * only unreferenced data for the next run. {@link #collect} never throws.
 *
 * <p>Concurrency: Phase 1 assumes a single writer per shard (leases arrive in Phase 4); GC runs
 * on that writer after its own publish. Decisions are made from one listing snapshot, so a
 * content-addressed data key re-uploaded after the listing (e.g. by a racing publish) would look
 * old. To narrow that window, each data object's {@code lastModified} is re-read
 * ({@code list(exactKey)}) immediately before its delete, and the delete is skipped if the object
 * is gone or now younger than {@code minAge}. A window between that re-read and the delete
 * remains; it is closed only by the Phase 4 writer lease.
 */
public final class GenerationGc {

    private static final Logger log = LoggerFactory.getLogger(GenerationGc.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern MANIFEST_NAME = Pattern.compile("gen-(\\d+)\\.json");
    private static final List<String> DATA_DIRS = List.of("lucene/", "graph/", "refs/");

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final int keepGenerations;
    private final Duration minAge;

    public GenerationGc(ObjectStore store, ObjectStoreLayout layout, int keepGenerations, Duration minAge) {
        if (keepGenerations < 1) throw new IllegalArgumentException("keepGenerations must be at least 1");
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.keepGenerations = keepGenerations;
        this.minAge = Objects.requireNonNull(minAge, "minAge");
    }

    private record Manifest(String key, long generation, Instant lastModified, ShardManifest parsed) {}

    /** Signals that GC for the shard must stop without deleting anything (more). */
    private static final class Abort extends Exception {
        Abort(String message) { super(message, null, false, false); }
    }

    /**
     * @param now the object store's clock (the {@code lastModified} of the {@code LATEST} just
     *            written), never the local clock
     * @return the number of objects deleted; never throws
     */
    public int collect(ShardId shard, Instant now) {
        int[] deleted = {0};
        try {
            run(shard, now, deleted);
        } catch (Abort e) {
            log.warn("GC of shard {} aborted after {} deletes: {}", shard, deleted[0], e.getMessage());
        } catch (RuntimeException e) {
            log.warn("GC of shard {} failed after {} deletes: {}", shard, deleted[0], e.toString());
        }
        return deleted[0];
    }

    private void run(ShardId shard, Instant now, int[] deleted) throws Abort {
        String prefix = layout.shardPrefix(shard);
        String manifestsPrefix = prefix + "manifests/";
        Instant cutoff = now.minus(minAge);

        // 1. LATEST: the generation readers see. Without it, we know nothing safe.
        long latestGen;
        String latestManifestKey;
        Optional<StoredObject> latest;
        try {
            latest = store.get(layout.latestKey(shard));
        } catch (IOException e) {
            throw new Abort("cannot read LATEST: " + e);
        }
        if (latest.isEmpty()) throw new Abort("LATEST is missing");
        try {
            JsonNode node = JSON.readTree(latest.get().data());
            JsonNode g = node == null ? null : node.get("generation");
            JsonNode m = node == null ? null : node.get("manifest");
            if (g == null || !g.canConvertToLong() || g.asLong() <= 0 || m == null || !m.isTextual()) {
                throw new Abort("LATEST is unreadable");
            }
            latestGen = g.asLong();
            latestManifestKey = manifestsPrefix + m.asText();
        } catch (IOException e) {
            throw new Abort("LATEST is unreadable: " + e);
        }

        // 2. Complete listing and every manifest read before any delete.
        List<ObjectInfo> objects;
        Map<Long, Manifest> manifests = new TreeMap<>(Comparator.reverseOrder());
        try {
            objects = store.list(prefix);
            for (ObjectInfo o : objects) {
                if (!o.key().startsWith(manifestsPrefix)) continue;
                Matcher mm = MANIFEST_NAME.matcher(o.key().substring(manifestsPrefix.length()));
                if (!mm.matches()) continue; // not a manifest we know: never touched
                long gen = Long.parseLong(mm.group(1));
                StoredObject obj = store.get(o.key()).orElse(null);
                if (obj == null) throw new Abort("manifest " + o.key() + " vanished during GC");
                ShardManifest parsed;
                try {
                    parsed = ShardManifest.read(obj.data());
                } catch (IOException e) {
                    throw new Abort("unparseable manifest " + o.key() + ": " + e);
                }
                validate(shard, o.key(), gen, parsed);
                manifests.put(gen, new Manifest(o.key(), gen, o.lastModified(), parsed));
            }
        } catch (IOException e) {
            throw new Abort("listing failed: " + e);
        }
        Manifest latestManifest = manifests.values().stream()
                .filter(m -> m.key().equals(latestManifestKey)).findFirst().orElse(null);
        if (latestManifest == null) throw new Abort("LATEST points at missing manifest " + latestManifestKey);

        // 3. Decide which manifests survive.
        List<Manifest> kept = new ArrayList<>();
        List<Manifest> doomed = new ArrayList<>();
        int newestAtOrBelowLatest = 0;
        for (Manifest m : manifests.values()) { // newest generation first
            boolean young = isYoung(m, cutoff);
            boolean keep;
            if (m.key().equals(latestManifestKey)) {
                keep = true;
            } else if (m.generation() > latestGen) {
                keep = young;
            } else {
                keep = newestAtOrBelowLatest < keepGenerations || young;
            }
            if (m.generation() <= latestGen) newestAtOrBelowLatest++;
            (keep ? kept : doomed).add(m);
        }
        Set<String> protectedKeys = new HashSet<>();
        protectedKeys.add(latestManifestKey);
        protectedKeys.add(layout.latestKey(shard));
        for (ManifestFile f : latestManifest.parsed().files()) protectedKeys.add(f.key());
        Set<String> referenced = new HashSet<>(protectedKeys);
        for (Manifest m : kept) {
            referenced.add(m.key());
            for (ManifestFile f : m.parsed().files()) referenced.add(f.key());
        }

        // 4. Delete manifests first, then unreferenced old data (each re-checked just before).
        for (Manifest m : doomed) {
            deleted[0] += delete(shard, m.key(), protectedKeys);
        }
        for (ObjectInfo o : objects) {
            String rel = o.key().substring(prefix.length());
            if (DATA_DIRS.stream().noneMatch(rel::startsWith)) continue;
            if (referenced.contains(o.key())) continue;
            if (!o.lastModified().isBefore(cutoff)) continue;
            if (!stillOld(o.key(), cutoff)) {
                log.info("GC of shard {} kept {}: re-uploaded or removed since listing", shard, o.key());
                continue;
            }
            deleted[0] += delete(shard, o.key(), protectedKeys);
        }
        if (deleted[0] > 0) {
            log.info("GC of shard {}: deleted {} objects ({} manifests), LATEST generation {}",
                    shard, deleted[0], doomed.size(), latestGen);
        }
    }

    /** Rejects manifests that parse leniently but cannot be trusted to list their references. */
    private void validate(ShardId shard, String key, long keyGen, ShardManifest m) throws Abort {
        try {
            ManifestValidation.check(shard, new Generation(keyGen), m, layout, key);
        } catch (ManifestValidation.InvalidManifestException e) {
            throw new Abort(e.getMessage());
        }
    }

    private static boolean isYoung(Manifest m, Instant cutoff) {
        Instant created = m.parsed().createdAt();
        return !m.lastModified().isBefore(cutoff) || (created != null && !created.isBefore(cutoff));
    }

    /** Re-reads one key's lastModified; false if gone, now young, or unreadable. */
    private boolean stillOld(String key, Instant cutoff) throws Abort {
        List<ObjectInfo> current;
        try {
            current = store.list(key);
        } catch (IOException e) {
            throw new Abort("re-check of " + key + " failed: " + e);
        }
        return current.stream().filter(o -> o.key().equals(key)).findFirst()
                .map(o -> o.lastModified().isBefore(cutoff)).orElse(false);
    }

    private int delete(ShardId shard, String key, Set<String> protectedKeys) {
        if (protectedKeys.contains(key)) {
            log.error("GC of shard {} refused to delete {}: referenced by the LATEST generation", shard, key);
            return 0;
        }
        try {
            store.delete(key);
            return 1;
        } catch (IOException | RuntimeException e) {
            log.warn("GC of shard {} could not delete {}: {}", shard, key, e.toString());
            return 0;
        }
    }
}
