package com.pharos;

import com.pharos.cli.*;
import com.pharos.web.WebServer;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.CodeParser;
import com.pharos.parser.GenericFileParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.JsCodeParser;
import com.pharos.parser.LanguageProfile;
import com.pharos.parser.PythonCodeParser;
import com.pharos.parser.RegexCodeParser;
import com.pharos.embedding.CrossEncoderProvider;
import com.pharos.search.SearchEngine;
import com.pharos.search.pipeline.CrossEncoder;
import com.pharos.search.pipeline.CrossEncoderProviderAdapter;
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
        EmbeddingProvider embedder = EmbeddingProvider.create(config);
        LuceneIndexer luceneIndexer = new LuceneIndexer(config);
        ModuleGraphBuilder moduleGraphBuilder = new ModuleGraphBuilder(registry);
        int parseThreads = config.resolvedParseThreads();
        // Tier-2 regex-based parsers — no external runtime needed
        List<CodeParser> regexParsers = LanguageProfile.ALL.stream()
                .<CodeParser>map(RegexCodeParser::new)
                .toList();

        List<CodeParser> parsers = new java.util.ArrayList<>();
        parsers.add(new JavaCodeParser(List.of(), List.of(), parseThreads));
        parsers.add(new PythonCodeParser());
        parsers.add(new JsCodeParser());
        parsers.addAll(regexParsers);
        parsers.add(new GenericFileParser(parseThreads)); // must be last (catch-all)
        ProjectIndexManager indexManager = new ProjectIndexManager(
                config, luceneIndexer, registry, embedder, moduleGraphBuilder, parsers);
        CrossEncoder crossEncoder = buildCrossEncoder(config);
        SearchEngine searchEngine = new SearchEngine(luceneIndexer, embedder, registry, crossEncoder);
        ModuleBoundaryAnalyzer boundaryAnalyzer = new ModuleBoundaryAnalyzer(registry, searchEngine);
        CrossProjectLinker crossProjectLinker = new CrossProjectLinker(config, registry);

        // Build picocli CommandLine with injected dependencies via factory
        CommandLine cmd = new CommandLine(new CodeSearchCommand(),
                new DependencyFactory(config, registry, luceneIndexer, indexManager, searchEngine,
                        moduleGraphBuilder, boundaryAnalyzer, crossProjectLinker));

        int exitCode = cmd.execute(args);
        System.exit(exitCode);
    }

    private static CrossEncoder buildCrossEncoder(IndexConfig config) {
        if (!config.isCrossEncoderEnabled()) return new NoOpCrossEncoder();
        try {
            return new CrossEncoderProviderAdapter(new CrossEncoderProvider());
        } catch (Exception e) {
            log.warn("Cross-encoder unavailable ({}), falling back to no-op.", e.getMessage());
            return new NoOpCrossEncoder();
        }
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

        DependencyFactory(IndexConfig config, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer, ProjectIndexManager indexManager,
                          SearchEngine searchEngine, ModuleGraphBuilder moduleGraphBuilder,
                          ModuleBoundaryAnalyzer boundaryAnalyzer,
                          CrossProjectLinker crossProjectLinker) {
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
                        moduleGraphBuilder, luceneIndexer, boundaryAnalyzer));
            if (cls == RemoveIndexCommand.class)
                return (K) new RemoveIndexCommand(registry, indexManager);
            // Fall back to default picocli factory for all other classes
            return CommandLine.defaultFactory().create(cls);
        }
    }
}
