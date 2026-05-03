package com.pharos.cli;

import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.mcp.McpServer;
import com.pharos.mcp.McpToolRegistry;
import com.pharos.search.SearchEngine;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "mcp-server",
        description = "Start the MCP server (stdio JSON-RPC) for Claude Code integration",
        mixinStandardHelpOptions = true
)
public class McpServerCommand implements Callable<Integer> {

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;
    private final ModuleGraphBuilder moduleGraphBuilder;
    private final ModuleBoundaryAnalyzer boundaryAnalyzer;

    public McpServerCommand(SearchEngine searchEngine, ProjectRegistry registry,
                             ModuleGraphBuilder moduleGraphBuilder,
                             ModuleBoundaryAnalyzer boundaryAnalyzer) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.boundaryAnalyzer = boundaryAnalyzer;
    }

    @Override
    public Integer call() {
        McpToolRegistry toolRegistry = new McpToolRegistry(
                searchEngine, registry, moduleGraphBuilder, boundaryAnalyzer);
        McpServer server = new McpServer(toolRegistry);
        server.start();
        return 0;
    }
}
