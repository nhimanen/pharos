package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.GenerationFiles;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.WriterStateStore.Restored;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.indexer.LocalShardLayout;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CompactGraphImport;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Writer state in the object store, under {@code shards/<shard>/writer-state/}:
 * <ul>
 *   <li>{@code gen-N/file-state.json}: the file state of generation N;</li>
 *   <li>{@code blobs/<sha256>}: the embedding cache files ({@code embed-cache*.{bin,meta}}), named
 *       by the SHA-256 of their bytes, so an unchanged cache is uploaded once and shared by every
 *       generation that has it;</li>
 *   <li>{@code gen-N/index.json} ({@link WriterStateIndex}), written last: names, keys, lengths and
 *       SHA-256 of the files of backup N.</li>
 * </ul>
 * {@link GenerationGc} keeps the writer state of the generations it retains, and the blobs those
 * reference.
 *
 * <p>{@link #restore} materialises {@code LATEST} through the reader's download path
 * ({@link ReplicaSource} with {@link ManifestValidation}, and {@link GenerationFiles} checks of every
 * length and checksum) into a staging dir next to the index dir, rebuilds the writer's ArcadeDB
 * graph from the published compact graph ({@link CompactGraphImport}), overlays the backup, and only
 * then swaps the staging dir in. A backup that is missing or fails its checks, and a {@code LATEST}
 * generation that cannot be used as a writer base, are WARNs and an empty result (the caller runs a
 * full index); a failure to reach the store is thrown.
 *
 * <p><b>Split projects</b> (ruling P6-R2): the writer state belongs to the project, so it is backed up
 * once per index run under {@code projects/<project>/writer-state/} with the same layout, numbered by
 * the run's {@code projectGeneration} ({@link #backupProject}). {@link #restoreProject} materialises
 * every shard's {@code LATEST} into the project's local shard dirs, imports the project's graph once, and
 * overlays that backup. A {@code [project]} project keeps the Phase 4 path {@code shards/<project>/}.
 */
public final class ObjectStoreWriterStateStore implements WriterStateStore {

    private static final Logger log = LoggerFactory.getLogger(ObjectStoreWriterStateStore.class);

    static final String FILE_STATE = "file-state.json";
    /** The per-model embedding caches and their meta files; temp files ({@code .tmp}) never match. */
    public static final Pattern EMBED_CACHE = Pattern.compile("embed-cache(\\.[A-Za-z0-9._-]+)?\\.(bin|meta)");
    private static final Pattern INDEX_KEY = Pattern.compile("gen-(\\d{1,18})/index\\.json");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final String GRAPH_TMP = ".restore-graph.cgf.gz";
    private static final String REFS_TMP = ".restore-refs.json.gz";

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ReplicaSource source;
    private final String nodeId;

    public ObjectStoreWriterStateStore(ObjectStore store, ObjectStoreLayout layout, ReplicaSource source,
                                       String nodeId) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.source = Objects.requireNonNull(source, "source");
        this.nodeId = nodeId;
    }

    /**
     * Where one writer state lives: a shard's ({@code shards/<shard>/writer-state/}, Phase 4 and every
     * {@code [project]} shard) or a split project's ({@code projects/<project>/writer-state/}, ruling
     * P6-R2), with the same layout below it. {@code name} is what a backup index records as its owner.
     */
    private record Scope(String label, String name, String prefix, boolean project) {
        static Scope shard(ObjectStoreLayout layout, ShardId shard) {
            return new Scope("Shard " + shard, shard.value(), layout.writerStatePrefix(shard), false);
        }

        static Scope project(ObjectStoreLayout layout, String project) {
            return new Scope("Project " + project, project, layout.projectWriterStatePrefix(project), true);
        }

        /**
         * A file this scope backs up besides {@code file-state.json}: the embedding caches, and for a split
         * project its {@code shard-layout.json} (it records the build-module dirs the file state was built
         * under).
         */
        boolean backsUp(String name) {
            return EMBED_CACHE.matcher(name).matches() || (project && LocalShardLayout.FILE_NAME.equals(name));
        }

        String genPrefix(long gen) { return prefix + "gen-" + gen + "/"; }

        String indexKey(long gen) { return genPrefix(gen) + "index.json"; }

        String blobKey(String sha256Hex) { return prefix + "blobs/" + sha256Hex; }
    }

    /** The backup or the generation cannot be used; the caller falls back to a full index. */
    private static final class Unusable extends Exception {
        Unusable(String message) { super(message, null, false, false); }
    }

    // ---- backup -----------------------------------------------------------------------------

    @Override
    public void backup(ShardId shard, Generation gen, Path indexDir) throws IOException {
        backup(Scope.shard(layout, shard), gen.value(), indexDir);
    }

    @Override
    public void backupProject(String project, long projectGeneration, Path indexDir) throws IOException {
        if (projectGeneration < 1) throw new IllegalArgumentException("project generation " + projectGeneration);
        backup(Scope.project(layout, project), projectGeneration, indexDir);
    }

    private void backup(Scope scope, long gen, Path indexDir) throws IOException {
        Path fileState = indexDir.resolve(FILE_STATE);
        if (!Files.isRegularFile(fileState)) {
            throw new IOException("no " + FILE_STATE + " in " + indexDir + ": nothing to back up");
        }
        List<String> names = new ArrayList<>();
        names.add(FILE_STATE);
        try (Stream<Path> s = Files.list(indexDir)) {
            s.map(p -> p.getFileName().toString())
                    .filter(scope::backsUp)
                    .filter(n -> Files.isRegularFile(indexDir.resolve(n)))
                    .sorted()
                    .forEach(names::add);
        }
        // A snapshot first (hard links: both files are only ever replaced by an atomic move), so the
        // bytes hashed are the bytes uploaded even if a later run replaces a file meanwhile.
        Path snapshot = Files.createTempDirectory(parentOf(indexDir), "." + indexDir.getFileName() + ".backup-");
        try {
            List<WriterStateIndex.Entry> entries = new ArrayList<>();
            int uploaded = 0, unchanged = 0;
            long bytesUploaded = 0;
            for (String name : names) {
                Path copy = snapshot.resolve(name);
                linkOrCopy(indexDir.resolve(name), copy);
                String sha = sha256Hex(copy);
                long length = Files.size(copy);
                String key;
                if (FILE_STATE.equals(name)) {
                    key = scope.genPrefix(gen) + FILE_STATE;
                    store.put(key, copy);
                    uploaded++;
                    bytesUploaded += length;
                } else {
                    key = scope.blobKey(sha);
                    Optional<ObjectInfo> existing = store.stat(key);
                    if (existing.isPresent() && existing.get().size() == length) {
                        unchanged++;
                    } else {
                        store.put(key, copy);
                        uploaded++;
                        bytesUploaded += length;
                    }
                }
                entries.add(new WriterStateIndex.Entry(name, key, length, sha));
            }
            WriterStateIndex index = new WriterStateIndex(WriterStateIndex.FORMAT_VERSION, scope.name(),
                    gen, Instant.now(), nodeId, IndexVersions.CHUNKING_VERSION, entries);
            store.put(scope.indexKey(gen), index.toJson());
            log.info("Backed up writer state of {} generation {}: {} files uploaded ({} bytes), {} unchanged",
                    scope.label().toLowerCase(java.util.Locale.ROOT), gen, uploaded, bytesUploaded, unchanged);
        } finally {
            deleteQuietly(snapshot);
        }
    }

    private static void linkOrCopy(Path from, Path to) throws IOException {
        try {
            Files.createLink(to, from);
        } catch (IOException | UnsupportedOperationException e) {
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    // ---- restore ----------------------------------------------------------------------------

    @Override
    public Optional<Restored> restore(ShardId shard, Path indexDir) throws IOException {
        Optional<Generation> latestOpt = source.latest(shard);
        if (latestOpt.isEmpty()) {
            log.info("Shard {}: nothing published yet, nothing to restore", shard);
            return Optional.empty();
        }
        Generation latest = latestOpt.get();
        Scope scope = Scope.shard(layout, shard);
        Optional<Generation> backup = newestBackup(scope, latest);
        if (backup.isEmpty()) {
            log.warn("Shard {}: no writer-state backup at or below generation {}; falling back to a full index",
                    shard, latest.value());
            return Optional.empty();
        }
        Path dir = indexDir.toAbsolutePath().normalize();
        Path root = parentOf(dir);
        Files.createDirectories(root);
        Path staging = root.resolve("." + dir.getFileName() + ".restoring");
        deleteRecursively(staging); // left by a crashed restore
        deleteRecursively(root.resolve("." + dir.getFileName() + ".replaced")); // ditto, crashed mid-swap
        Files.createDirectories(staging);
        try {
            WriterStateIndex index = readIndex(scope, backup.get());
            List<Path> state = downloadState(index, staging);
            ShardManifest manifest = manifest(shard, latest);
            materialise(shard, latest, manifest, dir, staging);
            swap(staging, dir);
            if (backup.get().compareTo(latest) < 0) {
                log.warn("Shard {}: restored generation {} with the writer state of generation {}: its file state"
                        + " doesn't describe the restored documents, so the next run is a full index (embeddings"
                        + " from the restored cache)", shard, latest.value(), backup.get().value());
            } else {
                log.info("Shard {}: restored generation {} with its writer state ({} files)", shard,
                        latest.value(), state.size());
            }
            return Optional.of(new Restored(latest, backup.get()));
        } catch (Unusable e) {
            log.warn("Shard {}: cannot restore from writer-state backup {} on generation {} ({}); falling back to"
                    + " a full index", shard, backup.get().value(), latest.value(), e.getMessage());
            return Optional.empty();
        } finally {
            deleteQuietly(staging);
        }
    }

    /** The newest generation at or below {@code latest} with a backup index. */
    private Optional<Generation> newestBackup(Scope scope, Generation latest) throws IOException {
        String prefix = scope.prefix();
        long best = -1;
        for (ObjectInfo o : store.list(prefix)) {
            Matcher m = INDEX_KEY.matcher(o.key().substring(prefix.length()));
            if (!m.matches()) continue;
            long g = Long.parseLong(m.group(1));
            if (g >= 1 && g <= latest.value() && g > best) best = g;
        }
        return best < 0 ? Optional.empty() : Optional.of(new Generation(best));
    }

    private WriterStateIndex readIndex(Scope scope, Generation gen) throws IOException, Unusable {
        String key = scope.indexKey(gen.value());
        StoredObject obj = store.get(key).orElseThrow(() -> new Unusable(key + " vanished"));
        WriterStateIndex index;
        try {
            index = WriterStateIndex.read(obj.data());
        } catch (IOException e) {
            throw new Unusable("unparseable " + key + ": " + e.getMessage());
        }
        if (index == null || index.formatVersion() != WriterStateIndex.FORMAT_VERSION) {
            throw new Unusable(key + " has an unsupported format");
        }
        if (index.generation() != gen.value() || !scope.name().equals(index.shard())) {
            throw new Unusable(key + " describes " + index.shard() + " generation " + index.generation());
        }
        String genPrefix = scope.genPrefix(gen.value());
        String blobPrefix = scope.blobKey("");
        Set<String> names = new HashSet<>();
        for (WriterStateIndex.Entry e : index.files()) {
            if (e == null || e.name() == null || e.key() == null || e.sha256() == null || e.length() < 0
                    || !SHA256.matcher(e.sha256()).matches() || !names.add(e.name())) {
                throw new Unusable(key + " has an invalid entry " + e);
            }
            boolean fileState = FILE_STATE.equals(e.name());
            if (!fileState && !scope.backsUp(e.name())) {
                throw new Unusable(key + " lists an unexpected file " + e.name());
            }
            if (fileState ? !e.key().equals(genPrefix + FILE_STATE) : !e.key().equals(blobPrefix + e.sha256())) {
                throw new Unusable(key + " has a foreign key for " + e.name() + ": " + e.key());
            }
        }
        if (!names.contains(FILE_STATE)) throw new Unusable(key + " lists no " + FILE_STATE);
        return index;
    }

    /** Downloads every file of the backup into {@code staging} and checks its length and SHA-256. */
    private List<Path> downloadState(WriterStateIndex index, Path staging)
            throws IOException, Unusable {
        List<Path> out = new ArrayList<>();
        for (WriterStateIndex.Entry e : index.files()) {
            Path target = staging.resolve(e.name());
            try {
                store.download(e.key(), target);
            } catch (ObjectMissingException missing) {
                throw new Unusable("backup file " + e.key() + " is missing");
            }
            long length = Files.size(target);
            String sha = sha256Hex(target);
            if (length != e.length() || !sha.equals(e.sha256())) {
                if (!FILE_STATE.equals(e.name())) {
                    // a blob whose bytes do not hash to its name is garbage: the next backup re-uploads it
                    try {
                        store.delete(e.key());
                    } catch (IOException | RuntimeException ex) {
                        log.debug("could not delete corrupt blob {}", e.key(), ex);
                    }
                }
                throw new Unusable("checksum mismatch for " + e.key() + ": " + length + " bytes, SHA-256 "
                        + sha + "; the index says " + e.length() + " bytes, SHA-256 " + e.sha256());
            }
            out.add(target);
        }
        return out;
    }

    private ShardManifest manifest(ShardId shard, Generation gen) throws IOException, Unusable {
        ShardManifest manifest;
        try {
            manifest = source.manifest(shard, gen);
        } catch (ManifestValidation.InvalidManifestException e) {
            throw new Unusable(e.getMessage());
        }
        IndexFormat.Status status = IndexFormat.statusOf(manifest.indexFormatVersion());
        if (status != IndexFormat.Status.OK) {
            throw new Unusable("generation " + gen.value() + " has index format " + manifest.indexFormatVersion()
                    + " (" + status.wireName() + "), this writer indexes format " + IndexVersions.INDEX_FORMAT);
        }
        try {
            GenerationFiles.checkFiles(manifest);
        } catch (GenerationFiles.IntegrityException e) {
            throw new Unusable(e.getMessage());
        }
        if (manifest.graphFile().isEmpty()) {
            throw new Unusable("generation " + gen.value() + " was published without a call graph");
        }
        return manifest;
    }

    /**
     * The published generation into {@code staging}: Lucene files (hard-linked from the current
     * index dir when the same file is there, else downloaded; all checksummed end to end), the call
     * graph as a fresh writer graph generation plus its pointer, refs and the format stamp.
     */
    private void materialise(ShardId shard, Generation gen, ShardManifest manifest, Path indexDir, Path staging)
            throws IOException, Unusable {
        long start = System.nanoTime();
        int[] counts = materialiseLucene(gen, manifest, indexDir.resolve("lucene"),
                Files.createDirectories(staging.resolve("lucene")));
        for (ManifestFile f : manifest.files()) {
            if (!ManifestFile.LUCENE.equals(f.role()) && materialiseProjectFile(gen, f, staging)) counts[1]++;
        }
        if (manifest.indexFormat() != null) IndexFormat.write(staging, manifest.indexFormat());
        // a project new to this node: the cross-project graph picks it up on its next rebuild
        CrossProjectLinker.touchStamp(staging.resolve("graph.stamp"));
        log.info("Shard {}: materialised generation {} for the writer ({} files hard-linked, {} downloaded) in {} ms",
                shard, gen.value(), counts[0], counts[1], (System.nanoTime() - start) / 1_000_000);
    }

    /**
     * The Lucene files of {@code manifest} into {@code lucene}: hard-linked from {@code existingLucene}
     * when the same file is there, else downloaded; all checksummed end to end.
     *
     * @return {@code {hard-linked, downloaded}}
     */
    private int[] materialiseLucene(Generation gen, ShardManifest manifest, Path existingLucene, Path lucene)
            throws IOException, Unusable {
        Set<String> local = new HashSet<>();
        int linked = 0, downloaded = 0;
        try {
            for (ManifestFile f : manifest.files()) {
                if (!ManifestFile.LUCENE.equals(f.role())) continue;
                Path target = lucene.resolve(f.name());
                if (reuse(existingLucene.resolve(f.name()), target, f)) {
                    linked++;
                } else {
                    source.download(f, target);
                    downloaded++;
                }
                GenerationFiles.checkLength(target, f);
                local.add(f.name());
            }
            // every local file is checked end to end: hard-linked ones were never verified downloads
            GenerationFiles.checkLuceneChecksums(lucene, manifest, local, local);
        } catch (GenerationFiles.IntegrityException e) {
            throw new Unusable(e.getMessage());
        } catch (ObjectMissingException e) {
            throw new Unusable("file " + e.key() + " of generation " + gen.value() + " is missing");
        }
        return new int[]{linked, downloaded};
    }

    /**
     * A graph file becomes a fresh writer graph generation plus its pointer in {@code staging}; a refs
     * file becomes {@code unresolved-refs.json}. Other roles are not part of a writer's index.
     *
     * @return true when the file was downloaded
     */
    private boolean materialiseProjectFile(Generation gen, ManifestFile f, Path staging) throws IOException, Unusable {
        try {
            switch (f.role()) {
                case ManifestFile.GRAPH -> {
                    Path compact = staging.resolve(GRAPH_TMP);
                    source.download(f, compact);
                    GenerationFiles.checkLength(compact, f);
                    GenerationFiles.checkCrc32(compact, f);
                    Path graphDir = CallGraph.newGenerationDir(staging);
                    try {
                        CompactGraphImport.importInto(compact, graphDir);
                    } catch (IOException e) {
                        throw new Unusable("call graph of generation " + gen.value() + " cannot be imported: "
                                + e.getMessage());
                    }
                    CallGraph.publish(staging, graphDir);
                    Files.delete(compact);
                    return true;
                }
                case ManifestFile.REFS -> {
                    Path gz = staging.resolve(REFS_TMP);
                    source.download(f, gz);
                    GenerationFiles.checkLength(gz, f);
                    GenerationFiles.checkCrc32(gz, f);
                    try (InputStream in = new GZIPInputStream(Files.newInputStream(gz))) {
                        Files.copy(in, staging.resolve(ObjectStorePublisher.REFS_FILE));
                    } catch (java.util.zip.ZipException e) {
                        throw new Unusable("refs of generation " + gen.value() + " are not gzip: " + e.getMessage());
                    }
                    Files.delete(gz);
                    return true;
                }
                default -> {
                    return false; // unknown roles are not part of a writer's index
                }
            }
        } catch (GenerationFiles.IntegrityException e) {
            throw new Unusable(e.getMessage());
        } catch (ObjectMissingException e) {
            throw new Unusable("file " + e.key() + " of generation " + gen.value() + " is missing");
        }
    }

    // ---- restore of a split project (ruling P6-R2) --------------------------------------------

    @Override
    public Optional<Restored> restoreProject(ShardPlan plan, Path indexDir) throws IOException {
        if (!plan.isSplit()) return restore(ShardId.ofProject(plan.project()), indexDir);
        String project = plan.project();
        Map<ShardId, Generation> latest = new LinkedHashMap<>();
        List<ShardId> unpublished = new ArrayList<>();
        for (ShardId shard : plan.shards()) {
            Optional<Generation> g = source.latest(shard);
            if (g.isPresent()) latest.put(shard, g.get()); else unpublished.add(shard);
        }
        if (latest.isEmpty()) {
            log.info("Project {}: nothing published yet, nothing to restore", project);
            return Optional.empty();
        }
        if (!unpublished.isEmpty()) {
            log.warn("Project {}: shards {} have no published generation; falling back to a full index",
                    project, unpublished);
            return Optional.empty();
        }
        Scope scope = Scope.project(layout, project);
        Path dir = indexDir.toAbsolutePath().normalize();
        Path root = parentOf(dir);
        Files.createDirectories(root);
        Path staging = root.resolve("." + dir.getFileName() + ".restoring");
        Long run = null;
        Optional<Generation> backup = Optional.empty();
        try {
            Map<ShardId, ShardManifest> manifests = new LinkedHashMap<>();
            long minRun = Long.MAX_VALUE, maxRun = 0;
            ShardManifest newest = null;
            for (Map.Entry<ShardId, Generation> e : latest.entrySet()) {
                ShardManifest m = manifest(e.getKey(), e.getValue());
                if (!m.isProjectShard()) {
                    throw new Unusable("shard " + e.getKey() + " generation " + e.getValue().value()
                            + " was not published as a shard of a split project");
                }
                manifests.put(e.getKey(), m);
                minRun = Math.min(minRun, m.projectGeneration());
                if (m.projectGeneration() > maxRun) {
                    maxRun = m.projectGeneration();
                    newest = m;
                }
            }
            run = maxRun;
            backup = newestBackup(scope, new Generation(maxRun));
            if (backup.isEmpty()) {
                log.warn("Project {}: no writer-state backup at or below run {}; falling back to a full index",
                        project, maxRun);
                return Optional.empty();
            }
            deleteRecursively(staging); // left by a crashed restore
            deleteRecursively(root.resolve("." + dir.getFileName() + ".replaced")); // ditto, crashed mid-swap
            Files.createDirectories(staging);
            long start = System.nanoTime();
            WriterStateIndex index = readIndex(scope, backup.get());
            downloadState(index, staging);
            int linked = 0, downloaded = 0;
            for (Map.Entry<ShardId, ShardManifest> e : manifests.entrySet()) {
                int[] c = materialiseLucene(latest.get(e.getKey()), e.getValue(),
                        LocalShardLayout.luceneDir(dir, e.getKey()),
                        Files.createDirectories(LocalShardLayout.luceneDir(staging, e.getKey())));
                linked += c[0];
                downloaded += c[1];
            }
            Generation runGen = new Generation(maxRun);
            for (ManifestFile f : newest.projectFileList()) {
                if (materialiseProjectFile(runGen, f, staging)) downloaded++;
            }
            if (newest.indexFormat() != null) IndexFormat.write(staging, newest.indexFormat());
            // the backed-up layout's module dirs describe the restored file state; the shards are the plan's
            List<String> modules = LocalShardLayout.read(staging).filter(l -> l.matches(plan))
                    .map(LocalShardLayout.Layout::modules).orElse(List.of());
            LocalShardLayout.write(staging, plan, modules);
            CrossProjectLinker.touchStamp(staging.resolve("graph.stamp"));
            swap(staging, dir);
            // the shards are not all at one run: the local index mixes runs, which no file state describes
            long stateRun = minRun == maxRun ? backup.get().value() : Math.min(backup.get().value(), minRun);
            log.info("Project {}: restored run {} ({} shards, {} files hard-linked, {} downloaded) in {} ms", project,
                    maxRun, manifests.size(), linked, downloaded, (System.nanoTime() - start) / 1_000_000);
            if (stateRun < maxRun) {
                log.warn("Project {}: restored run {} with the writer state of run {}{}: the next run is a full"
                        + " index (embeddings from the restored cache)", project, maxRun, stateRun,
                        minRun < maxRun ? " (its shards were published by runs " + minRun + " to " + maxRun + ")" : "");
            }
            return Optional.of(new Restored(runGen, new Generation(stateRun)));
        } catch (Unusable e) {
            log.warn("Project {}: cannot restore from writer-state backup {} on run {} ({}); falling back to a full"
                    + " index", project, backup.map(Generation::value).orElse(null), run, e.getMessage());
            return Optional.empty();
        } finally {
            deleteQuietly(staging);
        }
    }

    /**
     * Hard-links {@code existing} when it is the manifest's file: same length and the same footer
     * checksum (Lucene reuses file names across indexes, so a stale local file of the same name and
     * length is common). The end-to-end check after materialising still covers its body.
     */
    private static boolean reuse(Path existing, Path target, ManifestFile f) {
        try {
            if (!Files.isRegularFile(existing) || Files.size(existing) != f.length() || f.length() < 8) return false;
            java.nio.ByteBuffer footer = java.nio.ByteBuffer.allocate(8);
            try (var ch = java.nio.channels.FileChannel.open(existing, java.nio.file.StandardOpenOption.READ)) {
                while (footer.hasRemaining()) {
                    if (ch.read(footer, f.length() - 8 + footer.position()) < 0) return false;
                }
            }
            if (footer.flip().getLong() != GenerationFiles.parseChecksum(f)) return false;
            Files.createLink(target, existing);
            return true;
        } catch (IOException | UnsupportedOperationException | SecurityException e) {
            try {
                Files.deleteIfExists(target);
            } catch (IOException ignored) {
                // the download overwrites it
            }
            return false;
        }
    }

    /** Moves the complete {@code staging} dir into place of {@code indexDir} (which may not exist). */
    private static void swap(Path staging, Path indexDir) throws IOException {
        Path old = null;
        if (Files.exists(indexDir)) {
            old = parentOf(indexDir).resolve("." + indexDir.getFileName() + ".replaced");
            deleteRecursively(old);
            Files.move(indexDir, old, StandardCopyOption.ATOMIC_MOVE);
        }
        try {
            Files.move(staging, indexDir, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (old != null) Files.move(old, indexDir, StandardCopyOption.ATOMIC_MOVE);
            throw e;
        }
        GraphViews.evict(indexDir);
        if (old != null) deleteQuietly(old);
    }

    // ---- helpers ----------------------------------------------------------------------------

    static String sha256Hex(Path file) throws IOException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), md)) {
            in.transferTo(java.io.OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(md.digest());
    }

    private static Path parentOf(Path dir) {
        Path parent = dir.toAbsolutePath().normalize().getParent();
        if (parent == null) throw new IllegalArgumentException("index dir " + dir + " has no parent");
        return parent;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir, java.nio.file.LinkOption.NOFOLLOW_LINKS)) return;
        List<Path> paths;
        try (Stream<Path> s = Files.walk(dir)) {
            paths = s.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path p : paths) Files.deleteIfExists(p);
    }

    private static void deleteQuietly(Path dir) {
        try {
            deleteRecursively(dir);
        } catch (IOException | RuntimeException e) {
            log.debug("could not delete {}", dir, e);
        }
    }
}
