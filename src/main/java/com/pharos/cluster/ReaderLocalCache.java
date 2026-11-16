package com.pharos.cluster;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reader cache: each shard is served from its active generation directory
 * ({@code <cacheDir>/<shard>/gen-N}), switched by {@link #activate}. {@code LuceneIndexer} notices
 * the changed path on its next {@code openReader} and swaps readers.
 */
public class ReaderLocalCache implements LocalCache {

    private final Path cacheDir;
    private final Map<String, Path> active = new ConcurrentHashMap<>();

    public ReaderLocalCache(Path cacheDir) {
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
    }

    public void activate(String project, Path generationDir) {
        active.put(project, Objects.requireNonNull(generationDir, "generationDir"));
    }

    /** The active generation dir, or {@code <cacheDir>/<project>/none} (never created) before the first. */
    @Override
    public Path shardDir(String project) {
        Path dir = active.get(project);
        return dir != null ? dir : cacheDir.resolve(project).resolve("none");
    }

    public Optional<Path> active(String project) {
        return Optional.ofNullable(active.get(project));
    }
}
