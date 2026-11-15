package com.pharos;

import com.pharos.cli.*;
import com.pharos.web.WebServer;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.cluster.Backends;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.CodeParser;
import com.pharos.parser.GenericFileParser;
import com.pharos.parser.GoCodeParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.JsCodeParser;
import com.pharos.parser.LanguageProfile;
import com.pharos.parser.PythonCodeParser;
import com.pharos.parser.RegexCodeParser;
import com.pharos.parser.TerraformCodeParser;
import com.pharos.embedding.CrossEncoderProvider;
import com.pharos.search.SearchEngine;
import com.pharos.search.pipeline.CrossEncoder;
import com.pharos.search.pipeline.CrossEncoderProviderAdapter;
import com.pharos.search.pipeline.LazyCrossEncoder;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.List;

/**
 * CLI entrypoint with manual dependency wiring (no Spring/DI framework).
 *
 * Design: picocli with CommandLine.IFactory for constructor injection.
 * All dependencies are created once here and injected into subcommands.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    static void main(String[] args) {
        IndexConfig config = IndexConfig.load();

        // Core components
        ProjectRegistry registry = new ProjectRegistry(config);
        // Build all configured embedding providers — indexing writes one vector
        // field per model; search picks one provider via config.searchEmbeddingModel.
        List<EmbeddingProvider> embedders = EmbeddingProvider.createAll(config);
        // Same instance as the matching indexing provider — not a second copy of the model.
        EmbeddingProvider searchEmbedder = EmbeddingProvider.searchProvider(config, embedders);
        ReplicationBackend backend = Backends.create(config);
        LuceneIndexer luceneIndexer = new LuceneIndexer(config, backend.localCache());
        ModuleGraphBuilder moduleGraphBuilder = new ModuleGraphBuilder(registry);
        int parseThreads = config.resolvedParseThreads();
        // Tier-2 regex-based parsers — no external runtime needed
        List<CodeParser> regexParsers = LanguageProfile.ALL.stream()
                .<CodeParser>map(RegexCodeParser::new)
                .toList();

        List<CodeParser> parsers = new java.util.ArrayList<>();
        parsers.add(new JavaCodeParser(List.of(), List.of(), parseThreads));
        parsers.add(new PythonCodeParser(parseThreads));
        parsers.add(new JsCodeParser(parseThreads));
        parsers.add(new TerraformCodeParser(parseThreads));
        parsers.add(new GoCodeParser(parseThreads));
        parsers.addAll(regexParsers);
        parsers.add(new GenericFileParser(parseThreads, config.maxFileSizeBytes(true))); // must be last (catch-all)
        ProjectIndexManager indexManager = new ProjectIndexManager(
                config, luceneIndexer, registry, embedders, moduleGraphBuilder, parsers, backend.publisher());
        CrossEncoder crossEncoder = buildCrossEncoder(config);
        SearchEngine searchEngine = new SearchEngine(luceneIndexer, searchEmbedder, registry, crossEncoder);
        ModuleBoundaryAnalyzer boundaryAnalyzer = new ModuleBoundaryAnalyzer(registry, searchEngine);
        CrossProjectLinker crossProjectLinker = new CrossProjectLinker(config, registry);

        // Build picocli CommandLine with injected dependencies via factory
        CommandLine cmd = new CommandLine(new CodeSearchCommand(),
                new DependencyFactory(config, registry, luceneIndexer, indexManager, searchEngine,
                        moduleGraphBuilder, boundaryAnalyzer, crossProjectLinker, embedders));

        int exitCode = cmd.execute(args);
        // Let background model calls (e.g. a server's warm-up) finish before exit runs ORT's
        // shutdown hook, which frees the native environment under any call still running.
        com.pharos.embedding.NativeInferenceGuard.beginShutdown();
        System.exit(com.pharos.embedding.NativeInferenceGuard.exitCode(exitCode));
    }

    private static CrossEncoder buildCrossEncoder(IndexConfig config) {
        if (!config.isCrossEncoderEnabled()) return new NoOpCrossEncoder();
        // Loaded on first rerank, not at startup — most commands never rerank.
        return new LazyCrossEncoder(() -> {
            try {
                return new CrossEncoderProviderAdapter(new CrossEncoderProvider());
            } catch (Throwable e) {   // incl. UnsatisfiedLinkError: memoized as no-op, not retried per search
                log.warn("Cross-encoder unavailable ({}), falling back to no-op.", e.getMessage());
                return new NoOpCrossEncoder();
            }
        });
    }

    /**
     * Custom picocli IFactory that injects dependencies into subcommands.
     * This is the DI mechanism for picocli without using Spring or Guice.
     */
    static class DependencyFactory implements CommandLine.IFactory {

        private final IndexConfig config;
        private final ProjectRegistry registry;
        private final LuceneIndexer luceneIndexer;
        private final ProjectIndexManager indexManager;
        private final SearchEngine searchEngine;
        private final ModuleGraphBuilder moduleGraphBuilder;
        private final ModuleBoundaryAnalyzer boundaryAnalyzer;
        private final CrossProjectLinker crossProjectLinker;
        /** The process's embedding providers — shared so no command loads a model twice. */
        private final List<EmbeddingProvider> embedders;

        DependencyFactory(IndexConfig config, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer, ProjectIndexManager indexManager,
                          SearchEngine searchEngine, ModuleGraphBuilder moduleGraphBuilder,
                          ModuleBoundaryAnalyzer boundaryAnalyzer,
                          CrossProjectLinker crossProjectLinker,
                          List<EmbeddingProvider> embedders) {
            this.embedders = embedders;
            this.config = config;
            this.registry = registry;
            this.luceneIndexer = luceneIndexer;
            this.indexManager = indexManager;
            this.searchEngine = searchEngine;
            this.moduleGraphBuilder = moduleGraphBuilder;
            this.boundaryAnalyzer = boundaryAnalyzer;
            this.crossProjectLinker = crossProjectLinker;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K> K create(Class<K> cls) throws Exception {
            if (cls == IndexCommand.class)
                return (K) new IndexCommand(indexManager, crossProjectLinker, registry, config);
            if (cls == SearchCommand.class)
                return (K) new SearchCommand(searchEngine);
            if (cls == GetMethodCommand.class)
                return (K) new GetMethodCommand(searchEngine);
            if (cls == CallersCommand.class)
                return (K) new CallersCommand(searchEngine, registry, config);
            if (cls == CalleesCommand.class)
                return (K) new CalleesCommand(searchEngine, registry, config);
            if (cls == GetClassCommand.class)
                return (K) new GetClassCommand(searchEngine);
            if (cls == UsagesCommand.class)
                return (K) new UsagesCommand(searchEngine);
            if (cls == TraceCallChainCommand.class)
                return (K) new TraceCallChainCommand(searchEngine);
            if (cls == TransitiveCallersCommand.class)
                return (K) new TransitiveCallersCommand(searchEngine);
            if (cls == PathCommand.class)
                return (K) new PathCommand(registry, config);
            if (cls == ProjectsCommand.class)
                return (K) new ProjectsCommand(registry);
            if (cls == LinkCommand.class)
                return (K) new LinkCommand(registry, config);
            if (cls == StatsCommand.class)
                return (K) new StatsCommand(registry, luceneIndexer);
            if (cls == McpServerCommand.class)
                return (K) new McpServerCommand(searchEngine, registry,
                        moduleGraphBuilder, boundaryAnalyzer);
            if (cls == ModulesCommand.class)
                return (K) new ModulesCommand(moduleGraphBuilder);
            if (cls == ModuleDepsCommand.class)
                return (K) new ModuleDepsCommand(moduleGraphBuilder);
            if (cls == ModulePathCommand.class)
                return (K) new ModulePathCommand(moduleGraphBuilder);
            if (cls == WebCommand.class)
                return (K) new WebCommand(new WebServer(searchEngine, registry,
                        moduleGraphBuilder, luceneIndexer, boundaryAnalyzer), searchEngine);
            if (cls == RemoveIndexCommand.class)
                return (K) new RemoveIndexCommand(registry, indexManager);
            if (cls == BackfillEmbeddingCacheCommand.class)
                return (K) new BackfillEmbeddingCacheCommand(
                        new com.pharos.indexer.EmbeddingCacheBackfiller(config, luceneIndexer, registry));
            if (cls == IndexStatusCommand.class)
                return (K) new IndexStatusCommand(registry);
            if (cls == UpgradeIndexesCommand.class)
                return (K) new UpgradeIndexesCommand(indexManager, registry, config);
            if (cls == MineCommand.class)
                return (K) new MineCommand(registry, luceneIndexer);
            if (cls == EmbedCommand.class)
                return (K) new EmbedCommand(
                        new com.pharos.indexer.MultiModelEmbedder(config, luceneIndexer, registry, embedders),
                        registry);
            // Fall back to default picocli factory for all other classes
            return CommandLine.defaultFactory().create(cls);
        }
    }
}
