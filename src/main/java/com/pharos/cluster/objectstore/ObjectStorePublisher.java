package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.config.ProjectMeta;
import com.pharos.indexer.LocalShardLayout;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CompactGraphWriter;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.IndexFormat;
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
import java.util.LinkedHashMap;
import java.util.Map;
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
 * {@code LATEST}) → the {@link LeaseGuard} check (work-queue mode; a lost lease is a
 * {@link PublishConflictException}) → {@code putIfAbsent} the manifest gen-N → the lease check again,
 * and the manifest must still carry the ETag this run wrote → CAS {@code LATEST} from P to N → GC.
 * Registering before the commit point means a catalog failure can never be reported as a failed
 * publish of a generation that is in fact committed.
 *
 * <p>The commit point is the {@code LATEST} CAS alone. If manifest gen-N already exists while
 * {@code LATEST} still carries the ETag read in step 1, nobody committed N: the manifest is an
 * orphan of a run that died before its CAS, and is replaced by an ETag-conditional put (losing
 * that race, or {@code LATEST} having moved, is a {@link PublishConflictException}). Nothing
 * becomes visible to readers until the {@code LATEST} CAS succeeds; every failure before it
 * leaves {@code LATEST} untouched.
 *
 * <p>After the commit point the shard's writer state is backed up ({@link WriterStateStore#backup}),
 * then GC runs. Both are best-effort: a failure is a WARN, and the publish still succeeds.
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
    private final LeaseGuard leaseGuard;
    private final WriterStateStore writerState;

    /**
     * @param gc           generation GC, run with the shard and the {@code LATEST} last-modified
     *                     time (store clock) after a successful publish; {@code null} disables GC
     * @param synonymsFile shipped to {@code catalog/synonyms.txt} when it exists
     */
    public ObjectStorePublisher(ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                                GenerationGc gc, String nodeId, Path synonymsFile) {
        this(store, layout, catalog, gc, nodeId, synonymsFile, LeaseGuard.IMPLICIT);
    }

    /**
     * @param leaseGuard checked right before the {@code LATEST} CAS; {@link LeaseGuard#IMPLICIT}
     *                   (the default) always passes
     */
    public ObjectStorePublisher(ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                                GenerationGc gc, String nodeId, Path synonymsFile, LeaseGuard leaseGuard) {
        this(store, layout, catalog, gc, nodeId, synonymsFile, leaseGuard, WriterStateStore.NONE);
    }

    /**
     * @param writerState backs up the writer state of each generation right after its commit, before
     *                    GC; {@link WriterStateStore#NONE} keeps none
     */
    public ObjectStorePublisher(ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                                GenerationGc gc, String nodeId, Path synonymsFile, LeaseGuard leaseGuard,
                                WriterStateStore writerState) {
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.leaseGuard = Objects.requireNonNull(leaseGuard, "leaseGuard");
        this.store = store;
        this.layout = layout;
        this.catalog = catalog;
        this.gc = gc;
        this.nodeId = nodeId;
        this.synonymsFile = synonymsFile;
    }

    /** Attempts of the synonyms CAS before the publish fails. */
    static final int SYNONYMS_CAS_ATTEMPTS = 8;

    /**
     * Merges this writer's synonyms into {@code catalog/synonyms.txt} ({@link SynonymsMerge}): only
     * {@code shard}'s section is replaced, every other shard's rules stay, and the result is the same
     * whichever writer publishes it. A conditional put on the ETag read; on a race it re-reads and
     * merges again. Unchanged bytes are not rewritten. A bucket with the Phase 1–3 single file is
     * read the same way (it has the same layout).
     */
    private void publishSynonyms(String section) throws IOException {
        if (synonymsFile == null || !Files.isRegularFile(synonymsFile)) return;
        String local = new String(Files.readAllBytes(synonymsFile), StandardCharsets.UTF_8);
        String key = layout.synonymsKey();
        for (int attempt = 0; attempt < SYNONYMS_CAS_ATTEMPTS; attempt++) {
            Optional<StoredObject> remote = store.get(key);
            byte[] merged = SynonymsMerge.merge(
                    remote.map(r -> new String(r.data(), StandardCharsets.UTF_8)).orElse(null), local, section)
                    .getBytes(StandardCharsets.UTF_8);
            if (remote.isPresent() && java.util.Arrays.equals(remote.get().data(), merged)) return;
            Optional<String> written = remote.isPresent()
                    ? store.putIfMatch(key, merged, remote.get().etag())
                    : store.putIfAbsent(key, merged);
            if (written.isPresent()) return;
        }
        throw new IOException("synonyms.txt changed under " + SYNONYMS_CAS_ATTEMPTS
                + " concurrent publishes in a row; retry the publish");
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
        if (snapshot.plan().isSplit()) return publishProject(snapshot);
        // Validated before anything is read or uploaded (IllegalArgumentException).
        ShardId shard = ShardId.ofProject(snapshot.project());
        // Defensive, before anything is read from the store or uploaded: PublishCommand refuses
        // these states too, but every publish path (the index hook included) must.
        checkPublishableState(shard, snapshot.shardDir());
        Path tempDir = null;
        try {
            // A project the catalog splits into shards is published only by a split run: a [project]
            // generation under its plain id would be an orphan no reader serves.
            ProjectShards mapping = catalog.projectShards();
            if (!mapping.specOf(snapshot.project()).isDefault()) {
                throw new PublishConflictException("the catalog splits project " + snapshot.project() + " by spec "
                        + mapping.specOf(snapshot.project()) + " into " + mapping.shardsOf(snapshot.project())
                        + "; it is indexed and published by the work queue (cluster.writer.workQueue), not as one shard");
            }
            // 1. Previous generation P and the ETag the final CAS is conditioned on.
            Optional<Latest> previous = readLatest(shard);
            Generation gen = previous.map(p -> p.generation().next()).orElse(Generation.FIRST);
            Set<String> previousKeys = previous.isPresent()
                    ? keysOf(readManifest(shard, previous.get())) : Set.of();

            // 2. Lucene commit files: checksum every file first, then upload the new ones.
            ShardLucene sl = uploadLucene(shard, snapshot.shardDir().resolve("lucene"), previousKeys);
            List<ManifestFile> files = new ArrayList<>(sl.files());
            long docCount = sl.docCount();
            int uploaded = sl.uploaded(), reused = sl.reused();
            long bytesUploaded = sl.bytesUploaded();

            // 3. Per-generation graph and refs snapshots, content-named (gen-N@crc) so a racing
            //    run for the same N can never overwrite the bytes a committed manifest points at.
            tempDir = Files.createTempDirectory("pharos-publish-");
            //    The writer's live graph is the generation named by callgraph.current (legacy
            //    callgraph.arcadedb without a pointer), checked above; openExisting never creates one.
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

            // 4. Synonyms are cluster-wide: replace only this shard's section (a CAS read-modify-write).
            publishSynonyms(shard.value());

            // 5. Catalog before the commit point (idempotent).
            catalog.register(shard);

            // 6. Manifest gen-N, create-only; an orphan left by a run that never committed N is replaced.
            long sizeBytes = files.stream().mapToLong(ManifestFile::length).sum();
            ShardManifest manifest = new ShardManifest(ShardManifest.FORMAT_VERSION, shard.value(), gen.value(),
                    previous.map(p -> p.generation().value()).orElse(null), Instant.now(), nodeId,
                    docCount, sizeBytes, snapshot.modelFingerprint(), files,
                    ShardManifest.stripRefs(snapshot.meta()), formatStamp(snapshot.shardDir()));
            commit(shard, gen, previous, manifest.toJson());

            // 8. Best-effort writer-state backup (before GC, so GC sees this generation's state), then GC.
            backupWriterState(shard, gen, snapshot.shardDir());
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

    /**
     * A broken {@code callgraph.current} would ship a generation without its graph; an index
     * format other than this build's (even an N-1 one it still serves) must be re-indexed first,
     * so readers never receive an old format from this writer.
     */
    private static void checkPublishableState(ShardId shard, Path shardDir) throws PublishException {
        checkPublishableState(shard.value(), shardDir);
    }

    private static void checkPublishableState(String shard, Path shardDir) throws PublishException {
        String pointerProblem = CallGraph.pointerProblem(shardDir);
        if (pointerProblem != null) {
            throw new PublishException("cannot publish shard " + shard + ": " + pointerProblem
                    + "; re-index the project (pharos index --full)");
        }
        IndexFormat.Status status = IndexFormat.status(shardDir);
        if (status != IndexFormat.Status.OK) {
            throw new PublishException("cannot publish shard " + shard + ": its index format is "
                    + IndexFormat.read(shardDir).format() + " (" + status.wireName() + "), this pharos publishes "
                    + com.pharos.indexer.IndexVersions.INDEX_FORMAT + "; re-index the project (pharos index --full)");
        }
    }

    /** The shard's index-format.json as written by the index run; null when it has none (format 0). */
    private static IndexFormat.Stamp formatStamp(Path shardDir) {
        return Files.isRegularFile(shardDir.resolve(IndexFormat.FILE_NAME)) ? IndexFormat.read(shardDir) : null;
    }

    /** One shard's Lucene commit as uploaded: its manifest entries, live docs and upload counts. */
    private record ShardLucene(List<ManifestFile> files, long docCount, int uploaded, int reused,
                               long bytesUploaded) {}

    /**
     * Checksums every file of the latest Lucene commit in {@code luceneDir} first, then uploads those
     * whose content-addressed key is not in {@code previousKeys} (the shard's previous generation).
     */
    private ShardLucene uploadLucene(ShardId shard, Path luceneDir, Set<String> previousKeys) throws IOException {
        List<ManifestFile> files = new ArrayList<>();
        long docCount;
        List<LuceneFile> lucene = new ArrayList<>();
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
        return new ShardLucene(files, docCount, uploaded, reused, bytesUploaded);
    }

    /**
     * Steps 6 and 7 of the protocol for one shard: the manifest gen-N (create-only; an orphan left by a
     * run that never committed N is replaced), then the commit point, the {@code LATEST} CAS from P to N,
     * both under the shard's lease check.
     */
    private void commit(ShardId shard, Generation gen, Optional<Latest> previous, byte[] manifestJson)
            throws IOException {
        String manifestKey = layout.manifestKey(shard, gen);
        //    A writer that lost its lease must not write (or replace as an "orphan") the
        //    manifest a new owner is about to commit: check before writing it.
        leaseGuard.assertHeld(shard);
        Optional<String> manifestEtag = store.putIfAbsent(manifestKey, manifestJson);
        if (manifestEtag.isEmpty()) {
            manifestEtag = Optional.of(replaceOrphanManifest(shard, gen, manifestKey, manifestJson,
                    previous.map(Latest::etag).orElse(null)));
        }

        // 7. Commit point: CAS LATEST from P to N, only while this writer holds the shard's lease,
        //    and only onto the manifest this run wrote: a racing run that replaced it as an
        //    "orphan" in the meantime must never be committed under this run's CAS.
        leaseGuard.assertHeld(shard);
        String ownManifest = manifestEtag.get();
        if (!store.get(manifestKey).map(StoredObject::etag).filter(ownManifest::equals).isPresent()) {
            throw new PublishConflictException("manifest " + ObjectStoreLayout.manifestName(gen)
                    + " was replaced by another writer before the commit of shard " + shard);
        }
        byte[] latest = ("{\"generation\": " + gen.value() + ", \"manifest\": \""
                + ObjectStoreLayout.manifestName(gen) + "\"}").getBytes(StandardCharsets.UTF_8);
        Optional<String> committed = previous.isPresent()
                ? store.putIfMatch(layout.latestKey(shard), latest, previous.get().etag())
                : store.putIfAbsent(layout.latestKey(shard), latest);
        if (committed.isEmpty()) {
            throw new PublishConflictException("LATEST moved; another writer published shard " + shard);
        }
    }

    /**
     * Publishes one index run of a split project (rulings P6-R1, P6-R2): every shard gets a new
     * generation, and the project's call graph and refs are uploaded once, under
     * {@code projects/<project>/} named after the run's {@code projectGeneration} (one above the highest
     * any shard's {@code LATEST} manifest carries), and listed in every shard manifest's
     * {@code projectFiles}.
     *
     * <p>Order: read every shard's {@code LATEST} → check the catalog still maps the project to exactly
     * this run's spec and shards (a reshard since the run started is a {@link PublishConflictException})
     * → upload the project files → ship synonyms → upload every shard's new Lucene files → then, shard
     * after shard in plan order with nothing slow in between, register, write the manifest and CAS
     * {@code LATEST}, each under that shard's lease check → back up the project's writer state once →
     * GC every shard, then the project's files.
     *
     * <p><b>What readers see</b> (Review Focus 3): each shard's {@code LATEST} CAS is its own commit
     * point; there is no cross-shard transaction. Uploads all happen before the first CAS, so the shards
     * flip within one short burst of small writes, but a reader polling in between can see some shards at
     * this run and others at the previous one: a file that moved between two shards is then seen twice or
     * not at all until both have flipped. Every shard manifest of one run carries the same
     * {@code projectGeneration}; a reader that activates a project's shards only as a set with one
     * {@code projectGeneration} never sees the mix (Task 3). A failure after some CAS leaves the project
     * mixed until the next run, which publishes every shard again.
     */
    private Optional<Generation> publishProject(LocalSnapshot snapshot) throws IOException {
        ShardPlan plan = snapshot.plan();
        String project = snapshot.project();
        ShardId.ofProject(project); // validated before anything is read or uploaded
        Path projectDir = snapshot.shardDir();
        checkPublishableState(project, projectDir);
        Path tempDir = null;
        try {
            ProjectShards mapping = catalog.projectShards();
            if (!mapping.specOf(project).equals(plan.spec()) || !mapping.shardsOf(project).equals(plan.shards())) {
                throw new PublishConflictException("the catalog maps project " + project + " to spec "
                        + mapping.specOf(project) + " with shards " + mapping.shardsOf(project) + ", but this run"
                        + " indexed spec " + plan.spec() + " with shards " + plan.shards()
                        + " (resharded since the run started?)");
            }
            // 1. Every shard's previous generation, and the run's project generation.
            Map<ShardId, Optional<Latest>> previous = new LinkedHashMap<>();
            Map<ShardId, Set<String>> previousKeys = new LinkedHashMap<>();
            long projectGen = 1;
            for (ShardId shard : plan.shards()) {
                Optional<Latest> p = readLatest(shard);
                previous.put(shard, p);
                Set<String> keys = Set.of();
                if (p.isPresent()) {
                    ShardManifest m = readManifest(shard, p.get());
                    keys = keysOf(m);
                    if (m.projectGeneration() != null) projectGen = Math.max(projectGen, m.projectGeneration() + 1);
                }
                previousKeys.put(shard, keys);
            }

            // 2. The project's graph and refs, once, content-named (gen-N@crc).
            tempDir = Files.createTempDirectory("pharos-publish-");
            List<ManifestFile> projectFiles = new ArrayList<>();
            int uploaded = 0, reused = 0;
            long bytesUploaded = 0;
            Path arcade = CallGraph.projectGraphDir(projectDir);
            if (Files.isDirectory(arcade)) {
                Path graphFile = tempDir.resolve(GraphViews.COMPACT_FILE);
                try (CallGraph graph = CallGraph.openExisting(arcade)) {
                    CompactGraphWriter.export(graph, graphFile);
                }
                String crc = crc32Hex(graphFile);
                ManifestFile mf = upload(ManifestFile.GRAPH, GraphViews.COMPACT_FILE,
                        layout.projectGraphKey(project, projectGen, crc), graphFile, crc);
                projectFiles.add(mf);
                uploaded++;
                bytesUploaded += mf.length();
            }
            Path refs = projectDir.resolve(REFS_FILE);
            if (Files.isRegularFile(refs)) {
                Path refsGz = tempDir.resolve(REFS_FILE + ".gz");
                CRC32 refsCrc = new CRC32();
                try (OutputStream out = new GZIPOutputStream(
                        new CheckedOutputStream(Files.newOutputStream(refsGz), refsCrc))) {
                    Files.copy(refs, out);
                }
                String crc = Long.toHexString(refsCrc.getValue());
                ManifestFile mf = upload(ManifestFile.REFS, REFS_FILE + ".gz",
                        layout.projectRefsKey(project, projectGen, crc), refsGz, crc);
                projectFiles.add(mf);
                uploaded++;
                bytesUploaded += mf.length();
            }
            long projectBytes = projectFiles.stream().mapToLong(ManifestFile::length).sum();

            // 3. Synonyms: the project's section, as for a [project] shard.
            publishSynonyms(project);

            // 4. Every shard's new Lucene files before the first commit, so the commits follow each other.
            Map<ShardId, ShardLucene> lucene = new LinkedHashMap<>();
            for (ShardId shard : plan.shards()) {
                ShardLucene sl = uploadLucene(shard, LocalShardLayout.luceneDir(projectDir, shard),
                        previousKeys.get(shard));
                lucene.put(shard, sl);
                uploaded += sl.uploaded();
                reused += sl.reused();
                bytesUploaded += sl.bytesUploaded();
            }

            // 5. Commit every shard: register, manifest, LATEST CAS (each under its lease check).
            ProjectMeta meta = ShardManifest.stripRefs(snapshot.meta());
            IndexFormat.Stamp stamp = formatStamp(projectDir);
            Generation firstGen = null;
            for (ShardId shard : plan.shards()) {
                Optional<Latest> p = previous.get(shard);
                Generation gen = p.map(l -> l.generation().next()).orElse(Generation.FIRST);
                if (firstGen == null) firstGen = gen;
                ShardLucene sl = lucene.get(shard);
                catalog.register(shard);
                long sizeBytes = sl.files().stream().mapToLong(ManifestFile::length).sum() + projectBytes;
                ShardManifest manifest = new ShardManifest(ShardManifest.FORMAT_VERSION, shard.value(), gen.value(),
                        p.map(l -> l.generation().value()).orElse(null), Instant.now(), nodeId,
                        sl.docCount(), sizeBytes, snapshot.modelFingerprint(), sl.files(), meta, stamp,
                        projectGen, projectFiles);
                commit(shard, gen, p, manifest.toJson());
            }

            // 6. Best-effort: the project's writer state once, then GC of every shard and the project files.
            try {
                writerState.backupProject(project, projectGen, projectDir);
            } catch (IOException | RuntimeException e) {
                log.warn("Writer-state backup of project {} run {} failed (publish succeeded): {}",
                        project, projectGen, e.toString());
            }
            for (ShardId shard : plan.shards()) runGc(shard);
            if (gc != null) gc.collectProject(project, plan.shards(), storeNow(layout.latestKey(plan.shards().getFirst())));

            log.info("Published project {} run {} as {} shard generations: {} files uploaded, {} reused, {} bytes"
                    + " uploaded", project, projectGen, plan.shards().size(), uploaded, reused, bytesUploaded);
            return Optional.of(firstGen);
        } catch (PublishException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new PublishException("publishing project " + project + " failed: " + e.getMessage(), e);
        } finally {
            if (tempDir != null) deleteRecursively(tempDir);
        }
    }

    /** The store's last-modified time of {@code key} (the store clock); null when not listed. */
    private Instant storeNow(String key) {
        try {
            return store.list(key).stream().filter(o -> o.key().equals(key))
                    .map(ObjectInfo::lastModified).findFirst().orElse(null);
        } catch (IOException | RuntimeException e) {
            log.warn("Listing {} failed: {}", key, e.toString());
            return null;
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
    private String replaceOrphanManifest(ShardId shard, Generation gen, String manifestKey, byte[] json,
                                         String previousLatestEtag) throws IOException {
        Optional<StoredObject> orphan = store.get(manifestKey);
        String currentLatestEtag = store.get(layout.latestKey(shard)).map(StoredObject::etag).orElse(null);
        if (!Objects.equals(currentLatestEtag, previousLatestEtag)) {
            throw new PublishConflictException("LATEST moved; another writer published shard " + shard);
        }
        Optional<String> replaced = orphan.isPresent()
                ? store.putIfMatch(manifestKey, json, orphan.get().etag())
                : store.putIfAbsent(manifestKey, json);
        if (replaced.isEmpty()) {
            throw new PublishConflictException("manifest " + ObjectStoreLayout.manifestName(gen)
                    + " changed while replacing an orphan; another writer is publishing shard " + shard);
        }
        log.warn("Replaced orphan manifest {} of shard {} (LATEST never moved to it)", manifestKey, shard);
        return replaced.get();
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

    /**
     * Never fails the publish: the generation is committed. A missing backup of this generation makes
     * a takeover restore it with an older backup's writer state, whose file state doesn't describe
     * these documents: the new owner then runs a full index (embeddings from the older cache).
     */
    private void backupWriterState(ShardId shard, Generation gen, Path shardDir) {
        try {
            writerState.backup(shard, gen, shardDir);
        } catch (IOException | RuntimeException e) {
            log.warn("Writer-state backup of shard {} generation {} failed (publish succeeded): {}",
                    shard, gen.value(), e.toString());
        }
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
