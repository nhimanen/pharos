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
 *
 * <p>Writer state ({@code writer-state/}) is kept for the kept generations only; see
 * {@link #collectWriterState}.
 */
public final class GenerationGc {

    private static final Logger log = LoggerFactory.getLogger(GenerationGc.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern MANIFEST_NAME = Pattern.compile("gen-(\\d+)\\.json");
    private static final List<String> DATA_DIRS = List.of("lucene/", "graph/", "refs/");
    /** A file of a per-generation writer-state backup, relative to {@code writer-state/}. */
    private static final Pattern WRITER_STATE_GEN = Pattern.compile("gen-(\\d{1,18})/.+");

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
        return collect(shard, now, Set.of());
    }

    /**
     * As {@link #collect(ShardId, Instant)}, also keeping the generations in {@code pinned} (with
     * everything they reference): the generations a version of the shard's project names in the catalog
     * (a staged reshard's, the active pins after a flip, the previous version's), which readers sync or
     * serve whatever {@code LATEST} says.
     */
    public int collect(ShardId shard, Instant now, Set<Long> pinned) {
        int[] deleted = {0};
        try {
            run(shard, now, pinned == null ? Set.of() : pinned, deleted);
        } catch (Abort e) {
            log.warn("GC of shard {} aborted after {} deletes: {}", shard, deleted[0], e.getMessage());
        } catch (RuntimeException e) {
            log.warn("GC of shard {} failed after {} deletes: {}", shard, deleted[0], e.toString());
        }
        return deleted[0];
    }

    private void run(ShardId shard, Instant now, Set<Long> pinned, int[] deleted) throws Abort {
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
            if (m.key().equals(latestManifestKey) || pinned.contains(m.generation())) {
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
        for (Manifest m : manifests.values()) {
            if (!pinned.contains(m.generation())) continue;
            protectedKeys.add(m.key());
            for (ManifestFile f : m.parsed().files()) protectedKeys.add(f.key());
        }
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
        Set<Long> keptGens = new HashSet<>();
        for (Manifest m : kept) keptGens.add(m.generation());
        keptGens.add(latestGen);
        collectWriterState(shard.toString(), layout.writerStatePrefix(shard), objects, keptGens, cutoff,
                protectedKeys, deleted);
        if (deleted[0] > 0) {
            log.info("GC of shard {}: deleted {} objects ({} manifests), LATEST generation {}",
                    shard, deleted[0], doomed.size(), latestGen);
        }
    }

    /**
     * Writer state follows the generations: {@code writer-state/gen-N/} survives only while manifest
     * N does (its {@code index.json} is deleted first), and a {@code writer-state/blobs/} object only
     * while a surviving backup index references it or it is younger than {@code minAge} (each
     * re-checked just before its delete, like data files). When a surviving index cannot be read or
     * parsed, no blob is deleted.
     */
    private void collectWriterState(String shard, String prefix, List<ObjectInfo> objects, Set<Long> keptGens,
                                    Instant cutoff, Set<String> protectedKeys, int[] deleted) throws Abort {
        String blobPrefix = prefix + "blobs/";
        Map<Long, List<ObjectInfo>> byGen = new TreeMap<>();
        List<ObjectInfo> blobs = new ArrayList<>();
        for (ObjectInfo o : objects) {
            if (!o.key().startsWith(prefix)) continue;
            if (o.key().startsWith(blobPrefix)) {
                blobs.add(o);
                continue;
            }
            Matcher m = WRITER_STATE_GEN.matcher(o.key().substring(prefix.length()));
            if (m.matches()) byGen.computeIfAbsent(Long.parseLong(m.group(1)), g -> new ArrayList<>()).add(o);
        }

        // Blobs referenced by the backups that stay, read before anything is deleted.
        Set<String> referenced = new HashSet<>();
        boolean referencesKnown = true;
        for (long gen : byGen.keySet()) {
            if (!keptGens.contains(gen)) continue;
            String indexKey = prefix + "gen-" + gen + "/index.json";
            if (byGen.get(gen).stream().noneMatch(o -> o.key().equals(indexKey))) continue; // backup never completed
            Optional<StoredObject> obj;
            try {
                obj = store.get(indexKey);
            } catch (IOException e) {
                throw new Abort("cannot read " + indexKey + ": " + e);
            }
            if (obj.isEmpty()) {
                referencesKnown = false;
                continue;
            }
            try {
                for (WriterStateIndex.Entry e : WriterStateIndex.read(obj.get().data()).files()) {
                    if (e != null && e.key() != null) referenced.add(e.key());
                }
            } catch (IOException | RuntimeException e) {
                log.warn("GC of shard {}: unreadable writer-state index {}, keeping every blob: {}",
                        shard, indexKey, e.toString());
                referencesKnown = false;
            }
        }

        for (Map.Entry<Long, List<ObjectInfo>> e : byGen.entrySet()) {
            if (keptGens.contains(e.getKey())) continue;
            String indexKey = prefix + "gen-" + e.getKey() + "/index.json";
            List<ObjectInfo> files = new ArrayList<>(e.getValue());
            files.sort(Comparator.comparing((ObjectInfo o) -> !o.key().equals(indexKey))); // index first
            for (ObjectInfo o : files) deleted[0] += delete(shard, o.key(), protectedKeys);
        }

        if (!referencesKnown) return;
        for (ObjectInfo o : blobs) {
            if (referenced.contains(o.key())) continue;
            if (!o.lastModified().isBefore(cutoff)) continue;
            if (!stillOld(o.key(), cutoff)) continue;
            deleted[0] += delete(shard, o.key(), protectedKeys);
        }
    }

    /**
     * GC of a split project's own objects under {@code projects/<project>/} (ruling P6-R2), run after the
     * GC of each of its shards. Kept: every {@code graph/} and {@code refs/} object some manifest of any
     * of {@code shards} still lists in its {@code projectFiles} (the manifests the shard GCs kept, and any
     * newer orphan), and anything younger than {@code minAge}. The project writer state of a run survives
     * while a manifest of that run does, and the newest backup always does. Same safety rules as
     * {@link #collect}: any listing, read or validation failure deletes nothing; never throws.
     *
     * @param now the store's clock; null skips the GC
     * @return the number of objects deleted
     */
    public int collectProject(String project, List<ShardId> shards, Instant now) {
        int[] deleted = {0};
        if (now == null) {
            log.warn("GC of project {} skipped: no store time", project);
            return 0;
        }
        try {
            runProject(project, shards, now, deleted);
        } catch (Abort e) {
            log.warn("GC of project {} aborted after {} deletes: {}", project, deleted[0], e.getMessage());
        } catch (RuntimeException e) {
            log.warn("GC of project {} failed after {} deletes: {}", project, deleted[0], e.toString());
        }
        return deleted[0];
    }

    private void runProject(String project, List<ShardId> shards, Instant now, int[] deleted) throws Abort {
        Instant cutoff = now.minus(minAge);
        String prefix = layout.projectPrefix(project);
        Set<String> referenced = new HashSet<>();
        Set<String> protectedKeys = new HashSet<>();
        Set<Long> keptGens = new HashSet<>();
        for (ShardId shard : shards) {
            String manifestsPrefix = layout.shardPrefix(shard) + "manifests/";
            String latestManifest = null;
            try {
                Optional<StoredObject> latest = store.get(layout.latestKey(shard));
                if (latest.isPresent()) {
                    JsonNode m = JSON.readTree(latest.get().data()).get("manifest");
                    if (m != null && m.isTextual()) latestManifest = manifestsPrefix + m.asText();
                }
                for (ObjectInfo o : store.list(manifestsPrefix)) {
                    Matcher mm = MANIFEST_NAME.matcher(o.key().substring(manifestsPrefix.length()));
                    if (!mm.matches()) continue;
                    StoredObject obj = store.get(o.key()).orElseThrow(() -> new Abort("manifest " + o.key()
                            + " vanished during GC"));
                    ShardManifest parsed;
                    try {
                        parsed = ShardManifest.read(obj.data());
                    } catch (IOException e) {
                        throw new Abort("unparseable manifest " + o.key() + ": " + e);
                    }
                    validate(shard, o.key(), Long.parseLong(mm.group(1)), parsed);
                    if (parsed.projectGeneration() != null) keptGens.add(parsed.projectGeneration());
                    for (ManifestFile f : parsed.projectFileList()) {
                        referenced.add(f.key());
                        if (o.key().equals(latestManifest)) protectedKeys.add(f.key());
                    }
                }
            } catch (IOException e) {
                throw new Abort("listing shard " + shard + " failed: " + e);
            }
        }
        List<ObjectInfo> objects;
        try {
            objects = store.list(prefix);
        } catch (IOException e) {
            throw new Abort("listing failed: " + e);
        }
        for (ObjectInfo o : objects) {
            String rel = o.key().substring(prefix.length());
            if (!rel.startsWith("graph/") && !rel.startsWith("refs/")) continue;
            if (referenced.contains(o.key()) || !o.lastModified().isBefore(cutoff)) continue;
            if (!stillOld(o.key(), cutoff)) continue;
            deleted[0] += delete("project " + project, o.key(), protectedKeys);
        }
        // the newest backup stays even when no manifest names its run (yet)
        String statePrefix = layout.projectWriterStatePrefix(project);
        long newest = -1;
        for (ObjectInfo o : objects) {
            if (!o.key().startsWith(statePrefix)) continue;
            Matcher m = WRITER_STATE_GEN.matcher(o.key().substring(statePrefix.length()));
            if (m.matches()) newest = Math.max(newest, Long.parseLong(m.group(1)));
        }
        if (newest >= 0) keptGens.add(newest);
        collectWriterState("project " + project, statePrefix, objects, keptGens, cutoff, protectedKeys, deleted);
        if (deleted[0] > 0) log.info("GC of project {}: deleted {} objects", project, deleted[0]);
    }

    /**
     * Deletes every object of {@code shard} ({@code shards/<shard>/}): a shard no version of its project
     * names any more, past the retention window (a retired or abandoned reshard's). Manifests and
     * {@code LATEST} go first, so a crash midway leaves only unreferenced data. Never throws.
     *
     * @return the number of objects deleted
     */
    public int purgeShard(ShardId shard) {
        int deleted = 0;
        try {
            List<ObjectInfo> objects = new ArrayList<>(store.list(layout.shardPrefix(shard)));
            String manifests = layout.shardPrefix(shard) + "manifests/";
            String latest = layout.latestKey(shard);
            objects.sort(Comparator.comparing((ObjectInfo o) -> !o.key().equals(latest))
                    .thenComparing(o -> !o.key().startsWith(manifests)));
            for (ObjectInfo o : objects) {
                try {
                    store.delete(o.key());
                    deleted++;
                } catch (IOException | RuntimeException e) {
                    log.warn("Purging shard {}: could not delete {}: {}", shard, o.key(), e.toString());
                }
            }
            if (deleted > 0) log.info("Purged shard {}: deleted {} objects", shard, deleted);
        } catch (IOException | RuntimeException e) {
            log.warn("Purging shard {} failed after {} deletes: {}", shard, deleted, e.toString());
        }
        return deleted;
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

    private int delete(Object shard, String key, Set<String> protectedKeys) {
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
