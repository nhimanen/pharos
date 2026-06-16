package com.pharos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;

/**
 * Reads and writes the global project registry at ~/.pharos/registry.json.
 * Tracks all indexed projects and their metadata.
 *
 * <h3>Performance design</h3>
 * <ul>
 *   <li><b>In-memory cache with mtime check</b> — the registry map is kept in memory
 *       after the first read.  Each read compares {@code registry.json}'s last-modified
 *       time to the value recorded at load; a newer file triggers an automatic reload.
 *       This keeps the long-running daemon in sync when {@code pharos index} (a separate
 *       JVM) writes new entries to disk, and also prevents data loss when daemon and
 *       CLI register projects concurrently.  The stat call is cheap; a reload only
 *       happens when the file actually changed.</li>
 *   <li><b>Separate refs storage</b> — {@link ProjectMeta#getUnresolvedRefs()} can be
 *       thousands of entries.  They are persisted to
 *       {@code <indexPath>/unresolved-refs.json} so {@code registry.json} stays small.
 *       Refs are hydrated lazily the first time {@link #find(String)} is called for a
 *       project within a process run.</li>
 * </ul>
 */
public class ProjectRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProjectRegistry.class);

    private final Path registryFile;
    private final ObjectMapper mapper;

    /**
     * In-memory cache of the registry.  {@code null} means "not yet loaded from disk".
     * All entries carry their full {@link ProjectMeta#getUnresolvedRefs()} once hydrated.
     * Protected by {@code synchronized} on {@code this}.
     */
    private Map<String, ProjectMeta> cache = null;

    /**
     * Last-modified time of {@code registry.json} at the moment {@link #cache} was
     * populated (or last refreshed via our own {@link #save}).  Used by {@link #isStale}
     * to detect external writes — typically a fresh-JVM {@code pharos index} updating
     * the file while a long-running daemon holds an older cache.  {@code null} means
     * the file did not exist at the time of the load.
     */
    private FileTime lastLoadedMtime = null;

    public ProjectRegistry(IndexConfig config) {
        this(IndexConfig.DEFAULT_BASE.resolve("registry.json"));
    }

    /** Package-private: explicit registry path, used by tests that need an isolated location. */
    ProjectRegistry(Path registryFile) {
        this.registryFile = registryFile;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public synchronized void register(ProjectMeta meta) {
        // Persist unresolved refs to a separate file — keeps registry.json lean
        if (meta.getIndexPath() != null
                && meta.getUnresolvedRefs() != null
                && !meta.getUnresolvedRefs().isEmpty()) {
            saveRefs(meta.getName(), meta.getIndexPath(), meta.getUnresolvedRefs());
        }

        // Update cache with the full (refs-bearing) meta
        ensureCache().put(meta.getName(), meta);

        // Write slim registry to disk (refs excluded)
        flushToDisk();
        log.debug("Registered project '{}' in registry", meta.getName());
    }

    public synchronized Optional<ProjectMeta> find(String name) {
        ProjectMeta meta = ensureCache().get(name);
        if (meta == null) return Optional.empty();

        // Hydrate refs lazily — only on first access per process run
        if (meta.getUnresolvedRefs() == null || meta.getUnresolvedRefs().isEmpty()) {
            List<ProjectMeta.UnresolvedRef> refs = loadRefs(name, meta.getIndexPath());
            if (!refs.isEmpty()) {
                meta.setUnresolvedRefs(refs);  // mutate in-cache copy — hydrated once, reused thereafter
            }
        }
        return Optional.of(meta);
    }

    public synchronized List<ProjectMeta> listAll() {
        return new ArrayList<>(ensureCache().values());
    }

    public synchronized void link(String project1, String project2) {
        Map<String, ProjectMeta> c = ensureCache();
        ProjectMeta p1 = c.get(project1);
        ProjectMeta p2 = c.get(project2);
        if (p1 == null) throw new IllegalArgumentException("Project not found: " + project1);
        if (p2 == null) throw new IllegalArgumentException("Project not found: " + project2);

        if (!p1.getLinkedProjects().contains(project2)) p1.getLinkedProjects().add(project2);
        if (!p2.getLinkedProjects().contains(project1)) p2.getLinkedProjects().add(project1);

        flushToDisk();
        log.info("Linked projects '{}' and '{}'", project1, project2);
    }

    public synchronized void unregister(String name) {
        ensureCache().remove(name);
        flushToDisk();
    }

    /** Remove {@code name} from the linkedProjects list of every other project. */
    public synchronized void unlinkAll(String name) {
        boolean changed = false;
        for (ProjectMeta meta : ensureCache().values()) {
            if (meta.getLinkedProjects().remove(name)) changed = true;
        }
        if (changed) flushToDisk();
    }

    // -------------------------------------------------------------------------
    // Separate unresolved-refs persistence (#3)
    // -------------------------------------------------------------------------

    /**
     * Saves the unresolved call refs for {@code projectName} to
     * {@code <indexPath>/unresolved-refs.json}.  Atomic write via temp + rename.
     */
    public void saveRefs(String projectName, String indexPath,
                         List<ProjectMeta.UnresolvedRef> refs) {
        if (indexPath == null || refs == null || refs.isEmpty()) return;
        Path refsFile = Path.of(indexPath).resolve("unresolved-refs.json");
        try {
            Files.createDirectories(Path.of(indexPath));
            Path tmp = refsFile.resolveSibling("unresolved-refs.json.tmp");
            mapper.writeValue(tmp.toFile(), refs);
            Files.move(tmp, refsFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn("Could not save unresolved refs for '{}': {}", projectName, e.getMessage());
        }
    }

    /**
     * Loads unresolved refs from {@code <indexPath>/unresolved-refs.json}.
     * Falls back to the inline list in the cached meta when the separate file is absent
     * (supports indexes built before this optimization was introduced).
     */
    public List<ProjectMeta.UnresolvedRef> loadRefs(String projectName, String indexPath) {
        if (indexPath == null) return List.of();
        Path refsFile = Path.of(indexPath).resolve("unresolved-refs.json");
        if (!Files.exists(refsFile)) return List.of();
        try {
            List<ProjectMeta.UnresolvedRef> refs = mapper.readValue(refsFile.toFile(),
                    mapper.getTypeFactory().constructCollectionType(
                            List.class, ProjectMeta.UnresolvedRef.class));
            return refs != null ? refs : List.of();
        } catch (IOException e) {
            log.warn("Could not load unresolved refs for '{}': {}", projectName, e.getMessage());
            return List.of();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the in-memory cache, loading from disk on first call and reloading
     * whenever {@code registry.json} has been modified since the last load (e.g. by
     * a fresh-JVM {@code pharos index} writing while a daemon is running).
     */
    private Map<String, ProjectMeta> ensureCache() {
        if (cache == null || isStale()) {
            cache = loadFromDisk();
        }
        return cache;
    }

    /**
     * @return {@code true} if {@code registry.json}'s mtime is newer than the value
     *         recorded at last load, or if the file existence changed.  When we wrote
     *         the file ourselves {@link #save} updates {@link #lastLoadedMtime} so this
     *         returns {@code false} until the next external write.
     */
    private boolean isStale() {
        try {
            boolean exists = Files.exists(registryFile);
            if (!exists) {
                return lastLoadedMtime != null;  // file disappeared since load
            }
            FileTime current = Files.getLastModifiedTime(registryFile);
            return lastLoadedMtime == null || current.compareTo(lastLoadedMtime) > 0;
        } catch (IOException e) {
            log.warn("Could not stat registry {}: {}", registryFile, e.getMessage());
            return false;
        }
    }

    /**
     * Writes a slim (refs-excluded) snapshot of the cache to {@code registry.json}.
     * Unresolved refs are stored separately in per-project files — keeping them out
     * of the registry avoids serializing megabytes of data on every write.
     */
    private void flushToDisk() {
        Map<String, ProjectMeta> diskView = new LinkedHashMap<>();
        for (Map.Entry<String, ProjectMeta> e : cache.entrySet()) {
            diskView.put(e.getKey(), buildSlim(e.getValue()));
        }
        save(diskView);
    }

    /**
     * Builds a shallow copy of {@code meta} with {@link ProjectMeta#getUnresolvedRefs()}
     * cleared — safe to serialize into the shared {@code registry.json}.
     */
    private static ProjectMeta buildSlim(ProjectMeta meta) {
        ProjectMeta slim = new ProjectMeta(meta.getName(), meta.getRootPath(), meta.getIndexPath());
        slim.setLastIndexed(meta.getLastIndexed());
        slim.setMethodCount(meta.getMethodCount());
        slim.setClassCount(meta.getClassCount());
        slim.setFileCount(meta.getFileCount());
        slim.setKnownPackages(meta.getKnownPackages() != null
                ? new ArrayList<>(meta.getKnownPackages()) : new ArrayList<>());
        slim.setLinkedProjects(meta.getLinkedProjects() != null
                ? new ArrayList<>(meta.getLinkedProjects()) : new ArrayList<>());
        slim.setEmbeddedModels(meta.getEmbeddedModels() != null
                ? new ArrayList<>(meta.getEmbeddedModels()) : new ArrayList<>());
        slim.setGroupId(meta.getGroupId());
        slim.setArtifactId(meta.getArtifactId());
        slim.setMavenVersion(meta.getMavenVersion());
        slim.setUnresolvedRefsHash(meta.getUnresolvedRefsHash());
        // unresolvedRefs intentionally omitted — persisted in unresolved-refs.json
        return slim;
    }

    @SuppressWarnings("unchecked")
    private Map<String, ProjectMeta> loadFromDisk() {
        if (!Files.exists(registryFile)) {
            lastLoadedMtime = null;
            return new LinkedHashMap<>();
        }
        try {
            Map<String, ProjectMeta> result = mapper.readValue(registryFile.toFile(),
                    mapper.getTypeFactory().constructMapType(
                            LinkedHashMap.class, String.class, ProjectMeta.class));
            lastLoadedMtime = Files.getLastModifiedTime(registryFile);
            return result != null ? result : new LinkedHashMap<>();
        } catch (IOException e) {
            log.warn("Could not read registry {}: {}", registryFile, e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    private void save(Map<String, ProjectMeta> registry) {
        try {
            Files.createDirectories(registryFile.getParent());
            Path tmp = registryFile.resolveSibling(registryFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), registry);
            Files.move(tmp, registryFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            // Update mtime watermark so isStale() doesn't reload our own writes.
            lastLoadedMtime = Files.getLastModifiedTime(registryFile);
        } catch (IOException e) {
            log.error("Failed to save registry: {}", e.getMessage());
        }
    }
}
