package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.MultiModelEmbedder;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Adds (or refreshes) one embedding model's vectors against an existing Lucene
 * index without re-running parsing or the call-graph build. Used to A/B
 * compare embedding models cheaply: index once with model A, drop in model B
 * via this command instead of paying the full parse + graph cost again.
 *
 * <p>Idempotent: re-running with the same {@code --model} for a project that
 * already carries the field is a no-op. Use {@code --force} to recompute and
 * overwrite the existing per-model vectors.
 *
 * <p>Currently single-chunk-only — multi-chunk docs are skipped with a summary
 * count. For those, {@code pharos index --full} is the supported path.
 */
@Command(
        name = "embed",
        description = "Add vectors for one model against an existing index, no re-parse",
        mixinStandardHelpOptions = true
)
public class EmbedCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1",
            description = "Project name (as shown in 'pharos projects'); omit when --all is set")
    private String projectName;

    @Option(names = "--model", required = true,
            description = "modelId of the embedding provider (must match config.embeddingProviders[].modelId)")
    private String modelId;

    @Option(names = "--all", description = "Embed every registered project")
    private boolean all;

    @Option(names = "--force",
            description = "Overwrite the vector even when it's already present on a doc")
    private boolean force;

    private final MultiModelEmbedder embedder;
    private final ProjectRegistry registry;

    public EmbedCommand(MultiModelEmbedder embedder, ProjectRegistry registry) {
        this.embedder = embedder;
        this.registry = registry;
    }

    @Override
    public Integer call() throws Exception {
        if (all && projectName != null) {
            System.err.println("Specify either a project name or --all, not both.");
            return 2;
        }
        if (!all && projectName == null) {
            System.err.println("Missing project name. Pass a project or use --all.");
            return 2;
        }

        List<String> targets = all
                ? registry.listAll().stream().map(ProjectMeta::getName).toList()
                : List.of(projectName);
        if (targets.isEmpty()) {
            System.err.println("No projects to embed.");
            return 1;
        }

        int totalUpdated = 0;
        int failed = 0;
        for (String name : targets) {
            try {
                if (targets.size() > 1) {
                    System.out.printf("%n=== Embedding '%s' with '%s' ===%n", name, modelId);
                }
                totalUpdated += embedder.embed(name, modelId, force);
            } catch (Exception e) {
                System.err.printf("Failed for '%s': %s%n", name, e.getMessage());
                failed++;
            }
        }
        if (targets.size() > 1) {
            System.out.printf("%nDone: %d project(s)%s, %d total document(s) updated.%n",
                    targets.size(), failed > 0 ? " (" + failed + " failed)" : "", totalUpdated);
        }
        return failed > 0 ? 1 : 0;
    }
}
