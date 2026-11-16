package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * manifest read fails, or when any manifest cannot be parsed. Deletes are best-effort: a failed
 * delete is logged and the rest continue. Manifests are deleted before data files, so a crash
 * mid-GC leaves only unreferenced data for the next run.
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

    /**
     * @param now the object store's clock (the {@code lastModified} of the {@code LATEST} just
     *            written), never the local clock
     * @return the number of objects deleted
     */
    public int collect(ShardId shard, Instant now) {
        String prefix = layout.shardPrefix(shard);
        String manifestsPrefix = prefix + "manifests/";
        Instant cutoff = now.minus(minAge);

        // 1. LATEST: the generation readers see. Without it, we know nothing safe.
        long latestGen;
        String latestManifestKey;
        try {
            Optional<StoredObject> latest = store.get(layout.latestKey(shard));
            if (latest.isEmpty()) {
                log.warn("GC of shard {} skipped: LATEST is missing", shard);
                return 0;
            }
            JsonNode node = JSON.readTree(latest.get().data());
            JsonNode g = node == null ? null : node.get("generation");
            JsonNode m = node == null ? null : node.get("manifest");
            if (g == null || !g.canConvertToLong() || g.asLong() <= 0 || m == null || !m.isTextual()) {
                log.warn("GC of shard {} skipped: LATEST is unreadable", shard);
                return 0;
            }
            latestGen = g.asLong();
            latestManifestKey = manifestsPrefix + m.asText();
        } catch (IOException | RuntimeException e) {
            log.warn("GC of shard {} skipped: cannot read LATEST: {}", shard, e.toString());
            return 0;
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
                StoredObject obj = store.get(o.key()).orElse(null);
                if (obj == null) {
                    log.warn("GC of shard {} aborted: manifest {} vanished during GC", shard, o.key());
                    return 0;
                }
                ShardManifest parsed;
                try {
                    parsed = ShardManifest.read(obj.data());
                } catch (IOException e) {
                    log.warn("GC of shard {} aborted: unparseable manifest {}: {}", shard, o.key(), e.toString());
                    return 0;
                }
                manifests.put(Long.parseLong(mm.group(1)),
                        new Manifest(o.key(), Long.parseLong(mm.group(1)), o.lastModified(), parsed));
            }
        } catch (IOException | RuntimeException e) {
            log.warn("GC of shard {} aborted: listing failed: {}", shard, e.toString());
            return 0;
        }
        Manifest latestManifest = manifests.values().stream()
                .filter(m -> m.key().equals(latestManifestKey)).findFirst().orElse(null);
        if (latestManifest == null) {
            log.warn("GC of shard {} aborted: LATEST points at missing manifest {}", shard, latestManifestKey);
            return 0;
        }

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

        // 4. Delete manifests first, then unreferenced old data.
        int deleted = 0;
        for (Manifest m : doomed) {
            deleted += delete(shard, m.key(), protectedKeys);
        }
        for (ObjectInfo o : objects) {
            String rel = o.key().substring(prefix.length());
            if (DATA_DIRS.stream().noneMatch(rel::startsWith)) continue;
            if (referenced.contains(o.key())) continue;
            if (!o.lastModified().isBefore(cutoff)) continue;
            deleted += delete(shard, o.key(), protectedKeys);
        }
        if (deleted > 0) {
            log.info("GC of shard {}: deleted {} objects ({} manifests), LATEST generation {}",
                    shard, deleted, doomed.size(), latestGen);
        }
        return deleted;
    }

    private static boolean isYoung(Manifest m, Instant cutoff) {
        Instant created = m.parsed().createdAt();
        return !m.lastModified().isBefore(cutoff) || (created != null && !created.isBefore(cutoff));
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
