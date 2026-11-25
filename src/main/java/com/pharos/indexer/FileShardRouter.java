package com.pharos.indexer;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardPlan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * The shard of each file of a split project's index run ({@link ShardPlan#assign}), from the file's
 * {@link DocKeys}:
 * <ul>
 *   <li>{@code relativePath}: the file relative to the project root, by {@link DocKeys#normalizePath}
 *       (ruling P6-R7: the ring hashes this);</li>
 *   <li>{@code module}: the directory (relative to the root) of the nearest ancestor below the root that
 *       holds a build file ({@link #BUILD_FILES}); none for files outside such a directory;</li>
 *   <li>{@code language}: the file extension, lower case ({@code java}, {@code py}, {@code md});</li>
 *   <li>{@code fileKind}: {@code doc} for files the document chunker handles, else {@code code}.</li>
 * </ul>
 * Every document of a file has the file's keys, so one file never spans shards. Thread-safe; results
 * are cached per file and per directory for the run.
 */
final class FileShardRouter {

    /** Files that make their directory a build module. */
    static final List<String> BUILD_FILES = List.of("pom.xml", "build.gradle", "build.gradle.kts",
            "package.json", "go.mod", "pyproject.toml", "setup.py", "Cargo.toml", "CMakeLists.txt");

    private final ShardPlan plan;
    private final Path root;
    private final Predicate<Path> isDocument;
    private final Map<String, ShardId> byFile = new ConcurrentHashMap<>();
    private final Map<Path, Optional<String>> moduleOfDir = new ConcurrentHashMap<>();

    /** Key values per file (the document key fields every plan writes, P6-R12). */
    private final Map<String, DocKeys> keysByFile = new ConcurrentHashMap<>();

    /**
     * @param plan a split plan routes files to shards ({@link #shardOf}); any plan computes the files'
     *             key values ({@link #keysOf(String)}), which every document carries for key filters
     */
    FileShardRouter(ShardPlan plan, Path projectRoot, Predicate<Path> isDocument) {
        this.plan = plan;
        this.root = projectRoot.toAbsolutePath().normalize();
        this.isDocument = isDocument;
    }

    /**
     * The shard of the file at {@code filePath} (as stored in the documents' {@code filePath} field).
     *
     * @throws IllegalArgumentException for a file outside the project root, or one the catalog lists no
     *         shard for ({@link ShardPlan#assign})
     */
    ShardId shardOf(String filePath) {
        if (!plan.isSplit()) throw new IllegalStateException("a [project] plan has no shard routing");
        return byFile.computeIfAbsent(filePath, p -> plan.assign(keysOf(p)));
    }

    /** The key values of the file at {@code filePath} (as stored in the documents), cached for the run. */
    DocKeys keysOf(String filePath) {
        return keysByFile.computeIfAbsent(filePath, p -> keysOf(Path.of(p)));
    }

    DocKeys keysOf(Path file) {
        Path abs = file.toAbsolutePath().normalize();
        if (!abs.startsWith(root)) {
            throw new IllegalArgumentException("file " + file + " is not under the project root " + root);
        }
        String rel = DocKeys.normalizePath(root, abs);
        String name = abs.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String language = dot > 0 && dot < name.length() - 1 ? name.substring(dot + 1).toLowerCase(Locale.ROOT) : null;
        String kind = isDocument.test(abs) ? "doc" : "code";
        return new DocKeys(plan.project(), rel, moduleOf(abs.getParent()), language, kind);
    }

    /** The nearest module directory at or above {@code dir} and below the root, relative to it. */
    private String moduleOf(Path dir) {
        if (dir == null || !dir.startsWith(root) || dir.equals(root)) return null;
        Optional<String> cached = moduleOfDir.get(dir);
        if (cached != null) return cached.orElse(null);
        String module = null;
        for (String b : BUILD_FILES) {
            if (Files.isRegularFile(dir.resolve(b))) {
                module = DocKeys.normalizePath(root, dir);
                break;
            }
        }
        if (module == null) module = moduleOf(dir.getParent());
        moduleOfDir.put(dir, Optional.ofNullable(module));
        return module;
    }
}
