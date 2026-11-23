package com.pharos.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pharos.PharosVersion;
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
    private final boolean embeddingsConfigured;

    /** For callers that do not know the config: reports {@code needsReembed} as if embeddings were on. */
    public IndexStatusCommand(ProjectRegistry registry) {
        this(registry, true);
    }

    /**
     * @param embeddingsConfigured whether an embedding provider is configured; without one, nothing
     *                             re-embeds, so {@code needsReembed} is always false
     */
    public IndexStatusCommand(ProjectRegistry registry, boolean embeddingsConfigured) {
        this.registry = registry;
        this.embeddingsConfigured = embeddingsConfigured;
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
            // Status values stay as-is; this flag marks projects whose vectors pharos 1.4.0-1.4.1 stored
            // under vec.legacy (never searched) and that the next 'pharos index' re-embeds.
            r.put("needsReembed", embeddingsConfigured && dir != null && com.pharos.indexer.LegacyVectorFields
                    .hasLiveMisnamedVectors(dir.resolve("lucene")));
            r.put("lastIndexed", p.getLastIndexed() == null ? null : p.getLastIndexed().toString());
            rows.add(r);
        }

        if (json) {
            Map<String, Object> top = new LinkedHashMap<>();
            top.put("requiredFormat", IndexVersions.INDEX_FORMAT);
            top.put("minReadableFormat", IndexFormat.MIN_READABLE_FORMAT);
            top.put("pharosVersion", PharosVersion.get());
            top.put("projects", rows);
            System.out.println(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                    .writeValueAsString(top));
            return 0;
        }

        System.out.printf("pharos %s  (index format %d; reads %d-%d)%n", PharosVersion.get(),
                IndexVersions.INDEX_FORMAT, IndexFormat.MIN_READABLE_FORMAT, IndexVersions.INDEX_FORMAT);
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
        for (Map<String, Object> r : rows) {
            if (Boolean.TRUE.equals(r.get("needsReembed"))) {
                System.out.printf("note: %s: vectors need re-embed (run: %s)%n", r.get("name"),
                        com.pharos.indexer.ReindexHint.fullIndexCommand((String) r.get("name"), (String) r.get("rootPath")));
            }
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
