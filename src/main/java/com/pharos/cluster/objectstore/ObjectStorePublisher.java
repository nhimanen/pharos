package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CompactGraphWriter;
import com.pharos.graph.GraphViews;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Publishes a committed local shard as a new generation in an object store.
 *
 * <p>Protocol: read {@code LATEST} (P and its ETag) → upload Lucene commit files whose
 * content-addressed key is not in manifest P → upload this generation's graph and refs under
 * content-named keys ({@code graph/gen-N@<crc32>.cgf.gz}, {@code refs/gen-N@<crc32>.json.gz}), so
 * a racing run for the same N cannot overwrite bytes a committed manifest references → ship
 * synonyms → register the shard in the catalog (idempotent; readers skip a listed shard without
 * {@code LATEST}) → {@code putIfAbsent} the manifest gen-N → CAS {@code LATEST} from P to N → GC.
 * Registering before the commit point means a catalog failure can never be reported as a failed
 * publish of a generation that is in fact committed.
 *
 * <p>The commit point is the {@code LATEST} CAS alone. If manifest gen-N already exists while
 * {@code LATEST} still carries the ETag read in step 1, nobody committed N: the manifest is an
 * orphan of a run that died before its CAS, and is replaced by an ETag-conditional put (losing
 * that race, or {@code LATEST} having moved, is a {@link PublishConflictException}). Nothing
 * becomes visible to readers until the {@code LATEST} CAS succeeds; every failure before it
 * leaves {@code LATEST} untouched.
 */
public final class ObjectStorePublisher implements IndexPublisher {

    private static final Logger log = LoggerFactory.getLogger(ObjectStorePublisher.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String REFS_FILE = "unresolved-refs.json";

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ObjectStoreCatalog catalog;
    /** Nullable: no GC when absent. */
    private final GenerationGc gc;
    private final String nodeId;
    private final Path synonymsFile;

    /**
     * @param gc           generation GC, run with the shard and the {@code LATEST} last-modified
     *                     time (store clock) after a successful publish; {@code null} disables GC
     * @param synonymsFile shipped to {@code catalog/synonyms.txt} when it exists
     */
    public ObjectStorePublisher(ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                                GenerationGc gc, String nodeId, Path synonymsFile) {
        this.store = store;
        this.layout = layout;
        this.catalog = catalog;
        this.gc = gc;
        this.nodeId = nodeId;
        this.synonymsFile = synonymsFile;
    }

    /** Object keys embed the shard id, so the project name must be a valid one. */
    @Override
    public void checkPublishable(String project) {
        ShardId.ofProject(project);
    }

    private record Latest(Generation generation, String manifest, String etag) {}

    private record LuceneFile(String name, String key, long length, String checksum) {}

    @Override
    public Optional<Generation> publish(LocalSnapshot snapshot) throws IOException {
        // Validated before anything is read or uploaded (IllegalArgumentException).
        ShardId shard = ShardId.ofProject(snapshot.project());
        Path tempDir = null;
        try {
            // 1. Previous generation P and the ETag the final CAS is conditioned on.
            Optional<Latest> previous = readLatest(shard);
            Generation gen = previous.map(p -> p.generation().next()).orElse(Generation.FIRST);
            Set<String> previousKeys = previous.isPresent()
                    ? keysOf(readManifest(shard, previous.get())) : Set.of();

            // 2. Lucene commit files: checksum every file first, then upload the new ones.
            List<ManifestFile> files = new ArrayList<>();
            long docCount;
            List<LuceneFile> lucene = new ArrayList<>();
            Path luceneDir = snapshot.shardDir().resolve("lucene");
            try (Directory dir = FSDirectory.open(luceneDir)) {
                SegmentInfos commit = SegmentInfos.readLatestCommit(dir);
                docCount = 0;
                for (SegmentCommitInfo si : commit) {
                    docCount += si.info.maxDoc() - si.getDelCount() - si.getSoftDelCount();
                }
                for (String name : new TreeSet<>(commit.files(true))) {
                    String checksum = checksumHex(dir, name);
                    lucene.add(new LuceneFile(name, layout.luceneKey(shard, name, checksum),
                            dir.fileLength(name), checksum));
                }
            } catch (IOException e) {
                throw new PublishException("cannot read Lucene commit of shard " + shard + " in " + luceneDir, e);
            }
            int uploaded = 0, reused = 0;
            long bytesUploaded = 0;
            for (LuceneFile f : lucene) {
                if (previousKeys.contains(f.key())) {
                    reused++;
                } else {
                    store.put(f.key(), luceneDir.resolve(f.name()));
                    uploaded++;
                    bytesUploaded += f.length();
                }
                files.add(new ManifestFile(ManifestFile.LUCENE, f.name(), f.key(), f.length(), f.checksum()));
            }

            // 3. Per-generation graph and refs snapshots, content-named (gen-N@crc) so a racing
            //    run for the same N can never overwrite the bytes a committed manifest points at.
            tempDir = Files.createTempDirectory("pharos-publish-");
            //    The writer's live graph is the generation named by callgraph.current (legacy
            //    callgraph.arcadedb without a pointer). It is opened read-only: openExisting never
            //    creates a graph, and an unusable pointer fails the publish instead of silently
            //    shipping a generation without its graph.
            String pointerProblem = CallGraph.pointerProblem(snapshot.shardDir());
            if (pointerProblem != null) {
                throw new PublishException("cannot publish shard " + shard + ": " + pointerProblem
                        + "; re-index the project (pharos index --full)");
            }
            Path arcade = CallGraph.projectGraphDir(snapshot.shardDir());
            if (Files.isDirectory(arcade)) {
                Path graphFile = tempDir.resolve(GraphViews.COMPACT_FILE);
                try (CallGraph graph = CallGraph.openExisting(arcade)) {
                    CompactGraphWriter.export(graph, graphFile);
                }
                String crc = crc32Hex(graphFile);
                ManifestFile mf = upload(ManifestFile.GRAPH, GraphViews.COMPACT_FILE,
                        layout.graphKey(shard, gen, crc), graphFile, crc);
                files.add(mf);
                uploaded++;
                bytesUploaded += mf.length();
            }
            Path refs = snapshot.shardDir().resolve(REFS_FILE);
            if (Files.isRegularFile(refs)) {
                Path refsGz = tempDir.resolve(REFS_FILE + ".gz");
                CRC32 refsCrc = new CRC32();
                try (OutputStream out = new GZIPOutputStream(
                        new CheckedOutputStream(Files.newOutputStream(refsGz), refsCrc))) {
                    Files.copy(refs, out);
                }
                String crc = Long.toHexString(refsCrc.getValue());
                ManifestFile mf = upload(ManifestFile.REFS, REFS_FILE + ".gz",
                        layout.refsKey(shard, gen, crc), refsGz, crc);
                files.add(mf);
                uploaded++;
                bytesUploaded += mf.length();
            }

            // 4. Synonyms are cluster-wide; last writer wins.
            if (synonymsFile != null && Files.isRegularFile(synonymsFile)) {
                store.put(layout.synonymsKey(), synonymsFile);
            }

            // 5. Catalog before the commit point (idempotent).
            catalog.register(shard);

            // 6. Manifest gen-N, create-only; an orphan left by a run that never committed N is replaced.
            long sizeBytes = files.stream().mapToLong(ManifestFile::length).sum();
            ShardManifest manifest = new ShardManifest(ShardManifest.FORMAT_VERSION, shard.value(), gen.value(),
                    previous.map(p -> p.generation().value()).orElse(null), Instant.now(), nodeId,
                    docCount, sizeBytes, snapshot.modelFingerprint(), files,
                    ShardManifest.stripRefs(snapshot.meta()));
            byte[] manifestJson = manifest.toJson();
            String manifestKey = layout.manifestKey(shard, gen);
            if (store.putIfAbsent(manifestKey, manifestJson).isEmpty()) {
                replaceOrphanManifest(shard, gen, manifestKey, manifestJson,
                        previous.map(Latest::etag).orElse(null));
            }

            // 7. Commit point: CAS LATEST from P to N.
            byte[] latest = ("{\"generation\": " + gen.value() + ", \"manifest\": \""
                    + ObjectStoreLayout.manifestName(gen) + "\"}").getBytes(StandardCharsets.UTF_8);
            Optional<String> committed = previous.isPresent()
                    ? store.putIfMatch(layout.latestKey(shard), latest, previous.get().etag())
                    : store.putIfAbsent(layout.latestKey(shard), latest);
            if (committed.isEmpty()) {
                throw new PublishConflictException("LATEST moved; another writer published shard " + shard);
            }

            // 8. Best-effort GC.
            runGc(shard);

            log.info("Published shard {} generation {}: {} files uploaded, {} reused, {} bytes uploaded",
                    shard, gen.value(), uploaded, reused, bytesUploaded);
            return Optional.of(gen);
        } catch (PublishException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new PublishException("publishing shard " + shard + " failed: " + e.getMessage(), e);
        } finally {
            if (tempDir != null) deleteRecursively(tempDir);
        }
    }

    private Optional<Latest> readLatest(ShardId shard) throws IOException {
        String key = layout.latestKey(shard);
        Optional<StoredObject> obj = store.get(key);
        if (obj.isEmpty()) return Optional.empty();
        JsonNode node;
        try {
            node = JSON.readTree(obj.get().data());
        } catch (IOException e) {
            throw new PublishException("unreadable " + key, e);
        }
        JsonNode g = node == null ? null : node.get("generation");
        if (g == null || !g.canConvertToLong() || g.asLong() <= 0) {
            throw new PublishException(key + " has no valid generation: "
                    + new String(obj.get().data(), StandardCharsets.UTF_8));
        }
        Generation gen = new Generation(g.asLong());
        String manifest = node.path("manifest").asText(ObjectStoreLayout.manifestName(gen));
        return Optional.of(new Latest(gen, manifest, obj.get().etag()));
    }

    private ShardManifest readManifest(ShardId shard, Latest latest) throws IOException {
        String key = layout.shardPrefix(shard) + "manifests/" + latest.manifest();
        StoredObject obj = store.get(key).orElseThrow(() -> new PublishException(
                "LATEST of shard " + shard + " points at missing manifest " + key));
        try {
            return ShardManifest.read(obj.data());
        } catch (IOException e) {
            throw new PublishException("unreadable manifest " + key, e);
        }
    }

    private static Set<String> keysOf(ShardManifest manifest) {
        Set<String> keys = new HashSet<>();
        for (ManifestFile f : manifest.files()) keys.add(f.key());
        return keys;
    }

    /** Lucene footer checksum (CRC32) as lower-case hex. */
    private static String checksumHex(Directory dir, String name) throws IOException {
        try (IndexInput in = dir.openInput(name, IOContext.READONCE)) {
            return Long.toHexString(CodecUtil.retrieveChecksum(in));
        }
    }

    /**
     * Manifest N already exists. The commit point is the {@code LATEST} CAS only, so if
     * {@code LATEST} is still exactly what step 1 read (ETag unchanged, or still absent), nobody
     * committed N: the existing manifest is an orphan of a run that crashed (or got an ambiguous
     * error) between writing it and the CAS, and is replaced by ETag-conditional put. Otherwise
     * another writer committed, and this run conflicts.
     */
    private void replaceOrphanManifest(ShardId shard, Generation gen, String manifestKey, byte[] json,
                                       String previousLatestEtag) throws IOException {
        Optional<StoredObject> orphan = store.get(manifestKey);
        String currentLatestEtag = store.get(layout.latestKey(shard)).map(StoredObject::etag).orElse(null);
        if (!Objects.equals(currentLatestEtag, previousLatestEtag)) {
            throw new PublishConflictException("LATEST moved; another writer published shard " + shard);
        }
        boolean replaced = orphan.isPresent()
                ? store.putIfMatch(manifestKey, json, orphan.get().etag()).isPresent()
                : store.putIfAbsent(manifestKey, json).isPresent();
        if (!replaced) {
            throw new PublishConflictException("manifest " + ObjectStoreLayout.manifestName(gen)
                    + " changed while replacing an orphan; another writer is publishing shard " + shard);
        }
        log.warn("Replaced orphan manifest {} of shard {} (LATEST never moved to it)", manifestKey, shard);
    }

    /** CRC32 of a whole file as lower-case hex. */
    private static String crc32Hex(Path file) throws IOException {
        CRC32 crc = new CRC32();
        try (InputStream in = new CheckedInputStream(Files.newInputStream(file), crc)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return Long.toHexString(crc.getValue());
    }

    private ManifestFile upload(String role, String name, String key, Path file, String crcHex) throws IOException {
        store.put(key, file);
        return new ManifestFile(role, name, key, Files.size(file), crcHex);
    }

    private void runGc(ShardId shard) {
        if (gc == null) return;
        try {
            String latestKey = layout.latestKey(shard);
            Instant latestModified = store.list(latestKey).stream()
                    .filter(o -> o.key().equals(latestKey))
                    .map(ObjectInfo::lastModified)
                    .findFirst().orElse(null);
            if (latestModified == null) {
                log.warn("Generation GC for shard {} skipped: LATEST not listed", shard);
                return;
            }
            gc.collect(shard, latestModified);
        } catch (IOException | RuntimeException e) {
            log.warn("Generation GC for shard {} failed (publish succeeded): {}", shard, e.toString());
        }
    }

    private static void deleteRecursively(Path dir) {
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.debug("could not delete temp file {}", p, e);
                }
            });
        } catch (IOException e) {
            log.debug("could not clean temp dir {}", dir, e);
        }
    }
}
