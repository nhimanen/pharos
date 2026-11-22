package com.pharos.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.cluster.sharding.ShardSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The local index layout of a split project (ruling P6-R1). A {@code [project]} project has none of it:
 * its Lucene index is {@code <project>/lucene/}, exactly as in Phases 1–5. A split project keeps its
 * project-level state where it always was ({@code <project>/}: the call graph, {@code file-state.json},
 * the embedding caches, refs and the format stamp) and one Lucene index per shard:
 * <pre>
 *   &lt;project&gt;/shard-layout.json             {"spec": "[project, ring(path, shards=2, vnodes=512)]",
 *                                           "shards": ["p~ring=s01", "p~ring=s02"]}
 *   &lt;project&gt;/shards/&lt;encoded shard id&gt;/lucene/
 * </pre>
 * {@code shard-layout.json} is written last, after every shard's commit and the file state: it names the
 * shard set the local index is complete for. While the shards are being committed, {@link #COMMITTING}
 * marks the dir; a run that finds it (a crash between two shard commits) is turned into a full one.
 */
public final class LocalShardLayout {

    public static final String FILE_NAME = "shard-layout.json";
    /** The per-shard Lucene dirs live under {@code <project>/shards/}. */
    public static final String SHARDS_DIR = "shards";
    /** Present while a run commits its shards one after another; removed once all are committed. */
    public static final String COMMITTING = ".shards-committing";

    private static final ObjectMapper JSON = new ObjectMapper();

    private LocalShardLayout() {}

    /** A recorded layout: the spec and the shards the local index holds. */
    public record Layout(ShardSpec spec, List<ShardId> shards) {
        public Layout {
            shards = List.copyOf(shards);
        }

        /** True when this is exactly the layout {@code plan} indexes into. */
        public boolean matches(ShardPlan plan) {
            return plan.isSplit() && plan.spec().equals(spec) && plan.shards().equals(shards);
        }
    }

    /** The Lucene dir of {@code shard} in the project dir {@code projectDir}. */
    public static Path luceneDir(Path projectDir, ShardId shard) {
        return projectDir.resolve(SHARDS_DIR).resolve(shard.pathSegment()).resolve("lucene");
    }

    /** The recorded layout of {@code projectDir}; empty for a {@code [project]} index (or none). */
    public static Optional<Layout> read(Path projectDir) {
        Path file = projectDir.resolve(FILE_NAME);
        if (!Files.isRegularFile(file)) return Optional.empty();
        try {
            JsonNode root = JSON.readTree(file.toFile());
            ShardSpec spec = ShardSpec.parse(root.path("spec").asText(null));
            List<ShardId> shards = new ArrayList<>();
            for (JsonNode s : root.path("shards")) shards.add(new ShardId(s.asText()));
            if (spec.isDefault() || shards.isEmpty()) return Optional.empty();
            return Optional.of(new Layout(spec, shards));
        } catch (IOException | RuntimeException e) {
            // an unreadable layout matches no plan: the next run is a full one, which rewrites it
            return Optional.of(new Layout(ShardSpec.PROJECT, List.of()));
        }
    }

    /** Records {@code plan}'s layout (atomically replaced). */
    public static void write(Path projectDir, ShardPlan plan) throws IOException {
        ObjectNode root = JSON.createObjectNode();
        root.put("spec", plan.spec().toString());
        var arr = root.putArray("shards");
        plan.shards().forEach(s -> arr.add(s.value()));
        Path tmp = projectDir.resolve(FILE_NAME + ".tmp");
        Files.write(tmp, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root));
        Files.move(tmp, projectDir.resolve(FILE_NAME), StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }
}
