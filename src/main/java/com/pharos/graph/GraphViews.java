package com.pharos.graph;

import com.pharos.config.ProjectMeta;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Opens the read-only {@link GraphView} for a project's shard directory.
 *
 * <p>Compact graphs (reader generation dirs) are immutable and parsed once: the parsed
 * {@link CompactGraph} is cached per shard directory and shared by every caller (its
 * {@code close()} is a no-op), so a graph query does not re-read every project's graph file. An
 * entry is re-read when the file's size or modification time changes, and dropped by
 * {@link #evict} when the reader activates another generation or prunes the directory.
 */
public final class GraphViews {
    /** Legacy ArcadeDB dir; the live one is resolved through {@link CallGraph#POINTER_FILE}. */
    public static final String ARCADE_DIR = CallGraph.LEGACY_DIR;
    public static final String COMPACT_FILE = "graph.cgf.gz";

    private record Cached(long size, FileTime modified, CompactGraph graph) {}

    /** Generation dir (absolute, normalised) → parsed compact graph. */
    private static final Map<Path, Cached> COMPACT_CACHE = new ConcurrentHashMap<>();
    /** Number of compact graph files parsed; for tests. */
    static final AtomicLong compactReads = new AtomicLong();

    private GraphViews() {}

    public static GraphView open(Path shardDir) {
        // the writer's live generation (callgraph.current), or the legacy dir without a pointer
        Path arcade = CallGraph.projectGraphDir(shardDir);
        if (Files.isDirectory(arcade)) {
            return CallGraph.openExisting(arcade);
        }
        Path compact = shardDir.resolve(COMPACT_FILE);
        if (Files.exists(compact)) {
            try {
                return compact(key(shardDir), compact);
            } catch (IOException e) {
                // surfaced, not swallowed: an unreadable graph must not look like "no graph"
                throw new UncheckedIOException("Cannot read compact graph " + compact, e);
            }
        }
        return EmptyGraphView.INSTANCE;
    }

    public static GraphView open(ProjectMeta meta) {
        return open(Path.of(meta.getIndexPath()));
    }

    /** Drops the cached graph of {@code shardDir}, if any (generation deactivated or deleted). */
    public static void evict(Path shardDir) {
        if (shardDir != null) COMPACT_CACHE.remove(key(shardDir));
    }

    /** Number of cached compact graphs; for tests. */
    static int cachedCount() {
        return COMPACT_CACHE.size();
    }

    private static CompactGraph compact(Path key, Path file) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        Cached c = COMPACT_CACHE.get(key);
        if (c != null && c.size() == attrs.size() && c.modified().equals(attrs.lastModifiedTime())) {
            return c.graph();
        }
        synchronized (GraphViews.class) {
            c = COMPACT_CACHE.get(key);
            if (c != null && c.size() == attrs.size() && c.modified().equals(attrs.lastModifiedTime())) {
                return c.graph();
            }
            CompactGraph graph = CompactGraph.read(file);
            compactReads.incrementAndGet();
            COMPACT_CACHE.put(key, new Cached(attrs.size(), attrs.lastModifiedTime(), graph));
            return graph;
        }
    }

    private static Path key(Path shardDir) {
        return shardDir.toAbsolutePath().normalize();
    }
}
