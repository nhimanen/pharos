package com.pharos.cluster;

import com.pharos.graph.GraphViews;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reader cache: each shard is served from its active generation directory
 * ({@code <cacheDir>/<shard>/gen-N}, the shard id URL-safe encoded: {@link ShardId#pathSegment()}), switched by {@link #activate}. {@code LuceneIndexer} notices
 * the changed path on its next {@code openReader} and swaps readers. Activating another generation
 * evicts the previous one's cached compact graph ({@link GraphViews#evict}).
 */
public class ReaderLocalCache implements LocalCache {

    private final Path cacheDir;
    private final Map<String, Path> active = new ConcurrentHashMap<>();

    public ReaderLocalCache(Path cacheDir) {
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
    }

    public void activate(String project, Path generationDir) {
        Path previous = active.put(project, Objects.requireNonNull(generationDir, "generationDir"));
        if (previous != null && !previous.equals(generationDir)) GraphViews.evict(previous);
    }

    /** Stops serving {@code project}; its generation dirs stay until the caller removes them. */
    public void deactivate(String project) {
        Path previous = active.remove(project);
        if (previous != null) GraphViews.evict(previous);
    }

    /** Projects with an active generation. */
    public java.util.Set<String> activeProjects() {
        return java.util.Set.copyOf(active.keySet());
    }

    /** The active generation dir, or {@code <cacheDir>/<project>/none} (never created) before the first. */
    @Override
    public Path shardDir(String project) {
        Path dir = active.get(project);
        return dir != null ? dir : cacheDir.resolve(ShardId.pathSegmentOf(project)).resolve("none");
    }

    public Optional<Path> active(String project) {
        return Optional.ofNullable(active.get(project));
    }
}
