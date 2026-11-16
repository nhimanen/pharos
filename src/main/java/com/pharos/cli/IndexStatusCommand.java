package com.pharos.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pharos.PharosVersion;
import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.NodeRole;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import com.pharos.indexer.ProjectNames;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Reports each registered project's index format status. Strictly read-only: reads the registry
 * and the per-project format stamps, and sizes the index dirs by walking them. It opens no
 * Lucene writers or graphs, creates nothing and never starts a daemon.
 */
@Command(
        name = "index-status",
        description = "Show each project's index format status (ok, upgrade-available, outdated, newer, missing, invalid)",
        mixinStandardHelpOptions = true
)
public class IndexStatusCommand implements Callable<Integer> {

    @Option(names = "--json", description = "Machine-readable output")
    private boolean json;

    private final ProjectRegistry registry;
    private final NodeRole role;
    private final Supplier<Map<String, Object>> readerCache;

    public IndexStatusCommand(ProjectRegistry registry) {
        this(registry, NodeRole.ALL);
    }

    /**
     * @param role this node's cluster role, reported as {@code role} in {@code --json} so consumers
     *             (setup.sh) do not advise {@code upgrade-indexes} on a reader, which refuses it
     */
    public IndexStatusCommand(ProjectRegistry registry, NodeRole role) {
        this(registry, role, null);
    }

    /**
     * @param readerCache this process's block cache stats ({@code ClusterRuntime#readerCacheStats}),
     *                    reported as {@code readerCache}; null, or returning null, on anything but a lazy reader
     */
    public IndexStatusCommand(ProjectRegistry registry, NodeRole role, Supplier<Map<String, Object>> readerCache) {
        this.registry = registry;
        this.role = role;
        this.readerCache = readerCache;
    }

    @Override
    public Integer call() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ProjectMeta p : registry.listAll()) {
            boolean invalid = !ProjectNames.isValid(p.getName());
            // An invalid name may point at the whole index root: never read or size it.
            Path dir = invalid ? null : IndexFormat.dir(p.getIndexPath());
            IndexFormat.Stamp stamp = IndexFormat.read(dir);
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("name", p.getName());
            r.put("status", (invalid ? IndexFormat.Status.INVALID : IndexFormat.status(dir)).wireName());
            r.put("format", stamp.format());
            r.put("pharosVersion", stamp.pharosVersion());
            r.put("rootPath", p.getRootPath());
            r.put("rootExists", p.getRootPath() != null && Files.isDirectory(Path.of(p.getRootPath())));
            r.put("indexBytes", sizeOf(dir));
            r.put("lastIndexed", p.getLastIndexed() == null ? null : p.getLastIndexed().toString());
            rows.add(r);
        }

        Map<String, Object> cache = readerCache == null ? null : readerCache.get();
        if (json) {
            Map<String, Object> top = new LinkedHashMap<>();
            top.put("requiredFormat", IndexVersions.INDEX_FORMAT);
            top.put("minReadableFormat", IndexFormat.MIN_READABLE_FORMAT);
            top.put("pharosVersion", PharosVersion.get());
            top.put("role", role.json());
            if (cache != null) top.put("readerCache", cache);
            top.put("projects", rows);
            System.out.println(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                    .writeValueAsString(top));
            return 0;
        }

        System.out.printf("pharos %s  (index format %d; reads %d-%d)%n", PharosVersion.get(),
                IndexVersions.INDEX_FORMAT, IndexFormat.MIN_READABLE_FORMAT, IndexVersions.INDEX_FORMAT);
        if (cache != null) System.out.println(ClusterRuntime.readerCacheLine(cache));
        if (rows.isEmpty()) {
            System.out.println("No projects indexed.");
            return 0;
        }
        System.out.printf("%-25s  %-18s  %6s  %-10s  %10s  %-6s  %s%n",
                "PROJECT", "STATUS", "FORMAT", "VERSION", "SIZE", "ROOT", "LAST INDEXED");
        for (Map<String, Object> r : rows) {
            System.out.printf("%-25s  %-18s  %6s  %-10s  %10s  %-6s  %s%n",
                    r.get("name"), r.get("status"), r.get("format"), r.get("pharosVersion"),
                    humanBytes((Long) r.get("indexBytes")),
                    Boolean.TRUE.equals(r.get("rootExists")) ? "ok" : "gone",
                    r.get("lastIndexed") == null ? "never" : r.get("lastIndexed"));
        }
        return 0;
    }

    /** Total size of regular files under {@code dir}; 0 if it does not exist. */
    static long sizeOf(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) return 0;
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).mapToLong(f -> {
                try { return Files.size(f); } catch (IOException e) { return 0; }
            }).sum();
        } catch (IOException e) {
            return 0;
        }
    }

    static String humanBytes(long b) {
        if (b < 1024) return b + " B";
        double v = b;
        String[] u = {"KB", "MB", "GB", "TB"};
        int i = -1;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format("%.1f %s", v, u[i]);
    }
}
