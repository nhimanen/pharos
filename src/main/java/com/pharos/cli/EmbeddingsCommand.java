package com.pharos.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import com.pharos.indexer.LegacyVectorFields;
import com.pharos.indexer.ProjectNames;
import com.pharos.indexer.ReindexHint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Explicit opt-in to semantic (vector) search. Embeddings are off until {@code enable};
 * this command only edits {@code config.json} and never downloads a model or embeds anything.
 */
@Command(
        name = "embeddings",
        description = "Turn semantic (vector) search on or off, or show its state",
        mixinStandardHelpOptions = true,
        subcommands = {
                EmbeddingsCommand.Enable.class,
                EmbeddingsCommand.Disable.class,
                EmbeddingsCommand.Status.class
        }
)
public class EmbeddingsCommand implements Runnable {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final String[] EMBEDDING_KEYS = {
            "embeddingProviders", "searchEmbeddingModel", "searchEmbeddingProvider",
            "embeddingModelUrl", "embeddingDimensions", "embeddingMaxTokens"};

    private final Path home;
    private final ProjectRegistry registry;

    public EmbeddingsCommand(Path home, ProjectRegistry registry) {
        this.home = home;
        this.registry = registry;
    }

    @Override
    public void run() {
        new picocli.CommandLine(this).usage(System.out);
    }

    // The subcommands are instantiated by picocli through this factory-less path, so they
    // reach the parent via @ParentCommand.
    @Command abstract static class Sub implements Callable<Integer> {
        @picocli.CommandLine.ParentCommand EmbeddingsCommand parent;
    }

    /** Test hook: runs after the temp file is fully written, before it replaces config.json. */
    static java.util.function.Consumer<Path> beforeMove = p -> { };

    private ObjectNode readConfig() throws java.io.IOException {
        Path f = home.resolve("config.json");
        if (!Files.exists(f)) return MAPPER.createObjectNode();
        var node = MAPPER.readTree(f.toFile());
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("config.json is not a JSON object: " + f);
        }
        return (ObjectNode) node;
    }

    /** Atomic: temp file in the same directory, then move over config.json (same as ProjectRegistry). */
    private void writeConfig(ObjectNode cfg) throws java.io.IOException {
        Files.createDirectories(home);
        Path target = home.resolve("config.json");
        Path tmp = home.resolve("config.json.tmp");
        try {
            MAPPER.writeValue(tmp.toFile(), cfg);
            beforeMove.accept(tmp);
            Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
    }

    /** How to get out of a config.json that pharos refuses to load. */
    public static String fixConfigHint(Path home) {
        return "Fix " + home.resolve("config.json") + " by hand, or run `pharos embeddings disable` to remove"
                + " all embedding settings from it (other keys are kept).";
    }

    private ObjectNode readConfigOrReport() throws java.io.IOException {
        try {
            return readConfig();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    @Command(name = "enable", mixinStandardHelpOptions = true,
            description = "Write an embedding provider to config.json (does not download or embed)")
    static class Enable extends Sub {
        @Option(names = "--model", defaultValue = "granite-r2",
                description = "granite-r2 (recommended, default) or jina-v2-code")
        String model;

        @Option(names = "--force", description = "Replace a different, already configured provider")
        boolean force;

        @Override
        public Integer call() throws Exception {
            EmbeddingProviderConfig preset = IndexConfig.presetProvider(model);
            if (preset == null) {
                System.err.println("Unknown model '" + model + "' (use granite-r2 or jina-v2-code).");
                return 2;
            }
            ObjectNode cfg = parent.readConfigOrReport();
            if (cfg == null) return 1;
            ObjectNode target = MAPPER.valueToTree(preset);

            boolean hasProviders = cfg.has("embeddingProviders") && cfg.get("embeddingProviders").size() > 0;
            boolean hasLegacy = !hasProviders && cfg.hasNonNull("embeddingModelUrl")
                    && !cfg.get("embeddingModelUrl").asText().isBlank();
            // Same weights under another id, an old single-key config or a pinned/unpinned URL:
            // replacing it would only force a pointless re-embed.
            String currentUrl = hasProviders && cfg.get("embeddingProviders").size() == 1
                    ? cfg.get("embeddingProviders").get(0).path("url").asText(null)
                    : hasLegacy ? cfg.get("embeddingModelUrl").asText() : null;
            if (currentUrl != null && sameModel(currentUrl, preset.getUrl())) {
                String currentId = hasProviders
                        ? cfg.get("embeddingProviders").get(0).path("modelId").asText("?")
                        : IndexConfig.LEGACY_MODEL_ID;
                System.out.println("Embeddings are already configured with this model (" + currentId + ", "
                        + currentUrl + "); nothing to do.");
                return 0;
            }
            if ((hasProviders || hasLegacy) && !force) {
                String current = hasProviders
                        ? cfg.get("embeddingProviders").get(0).path("modelId").asText("?")
                        : "legacy (" + cfg.get("embeddingModelUrl").asText() + ")";
                System.err.println("Embeddings are already configured with a different model: " + current + ".");
                System.err.println("Re-run with --force to replace it. Vectors already in your indexes stay,"
                        + " but they were built with the old model: re-index each project afterwards with"
                        + " `pharos index --full <root path>` (`pharos embeddings status` prints the command"
                        + " per project).");
                return 1;
            }

            System.out.println("Enabling semantic search with " + preset.getModelId() + " ("
                    + preset.getUrl() + ").");
            System.out.println("Cost: about a 600 MB model download on first use and a CPU-heavy first embed"
                    + " (hours for a large project on CPU; peak memory about 2.8 GB). Nothing is downloaded"
                    + " or embedded now.");

            if (hasProviders) {
                System.out.println("Replacing configured provider(s):");
                for (var old : cfg.get("embeddingProviders")) {
                    System.out.println("  - " + old.path("modelId").asText("?") + "  " + old.path("url").asText("?"));
                }
            } else if (hasLegacy) {
                System.out.println("Replacing legacy embeddingModelUrl: " + cfg.get("embeddingModelUrl").asText());
            }
            for (String k : EMBEDDING_KEYS) cfg.remove(k);
            ArrayNode providers = cfg.putArray("embeddingProviders");
            providers.add(target);
            cfg.put("searchEmbeddingModel", preset.getModelId());
            parent.writeConfig(cfg);

            System.out.println("Wrote " + parent.home.resolve("config.json") + ".");
            System.out.println("Next: restart the daemon (`pharos daemon restart`), then re-index each project with"
                    + " `pharos index --full <root path>` to embed it (`pharos embeddings status` prints the"
                    + " exact command per project).");
            return 0;
        }

        /** Same model repo: URLs equal once a {@code @<sha>} revision pin is dropped. */
        static boolean sameModel(String a, String b) {
            return a != null && b != null && stripRevision(a).equals(stripRevision(b));
        }

        private static String stripRevision(String url) {
            String u = url.trim();
            int at = u.lastIndexOf('@');
            return at > u.indexOf("://") + 2 ? u.substring(0, at) : u;
        }
    }

    @Command(name = "disable", mixinStandardHelpOptions = true,
            description = "Remove all embedding settings from config.json (vectors already indexed are left in place)")
    static class Disable extends Sub {
        @Override
        public Integer call() throws Exception {
            if (!Files.exists(parent.home.resolve("config.json"))) {
                System.out.println("Embeddings are already off.");
                return 0;
            }
            ObjectNode cfg = parent.readConfigOrReport();
            if (cfg == null) return 1;
            boolean changed = false;
            for (String k : EMBEDDING_KEYS) changed |= cfg.remove(k) != null;
            if (changed) parent.writeConfig(cfg);
            System.out.println(changed
                    ? "Embeddings disabled: search is keyword-only. Restart the daemon to apply."
                    : "Embeddings are already off.");
            return 0;
        }
    }

    @Command(name = "status", mixinStandardHelpOptions = true,
            description = "Show the configured embedding model and which projects have its vectors")
    static class Status extends Sub {
        @Override
        public Integer call() {
            IndexConfig cfg;
            try {
                cfg = IndexConfig.loadFrom(parent.home);
            } catch (RuntimeException e) {
                System.err.println("Invalid embedding config: " + e.getMessage());
                System.err.println(fixConfigHint(parent.home));
                return 1;
            }
            if (cfg.getEmbeddingProviders().isEmpty()) {
                System.out.println("Semantic search: off (keyword-only). Enable with `pharos embeddings enable`.");
                return 0;
            }
            System.out.println("Semantic search: on");
            for (EmbeddingProviderConfig p : cfg.getEmbeddingProviders()) {
                boolean search = cfg.resolveSearchProvider().map(s -> s.getModelId().equals(p.getModelId())).orElse(false);
                System.out.println("  model:       " + p.getModelId() + (search ? "  (used for search)" : ""));
                System.out.println("  type/url:    " + p.getType() + "  " + p.getUrl());
                System.out.println("  dimensions:  " + p.getDimensions() + "  pooling: " + p.getPooling().json());
                System.out.println("  fingerprint: " + IndexVersions.modelFingerprint(p));
            }
            System.out.println("Projects with vectors:");
            var projects = parent.registry.listAll();
            if (projects.isEmpty()) System.out.println("  (no projects indexed)");
            for (ProjectMeta m : projects) {
                if (!ProjectNames.isValid(m.getName())) {
                    System.out.printf("  %-24s invalid name (see UPGRADING.md)%n", m.getName());
                    continue;
                }
                // Evidence from the index itself, not the registry's embeddedModels or the
                // search-side legacy allowance: both said "yes" for keyword-only projects.
                Path lucene = IndexFormat.dir(m.getIndexPath()) == null ? null
                        : IndexFormat.dir(m.getIndexPath()).resolve("lucene");
                StringBuilder has = new StringBuilder();
                for (EmbeddingProviderConfig p : cfg.getEmbeddingProviders()) {
                    if (LegacyVectorFields.hasLiveVectors(lucene, DocumentMapper.vectorFieldFor(p.getModelId()))) {
                        has.append(has.length() > 0 ? "," : "").append(p.getModelId());
                    }
                }
                System.out.printf("  %-24s %s%n", m.getName(), has.length() > 0 ? "yes (" + has + ")"
                        : "no   (run `" + ReindexHint.fullIndexCommand(m.getName(), m.getRootPath()) + "`)");
            }
            return 0;
        }
    }
}
