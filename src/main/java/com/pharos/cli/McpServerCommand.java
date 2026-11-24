package com.pharos.cli;

import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.NodeRole;
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
    private final ClusterRuntime clusterRuntime;
    private final NodeRole role;

    public McpServerCommand(SearchEngine searchEngine, ProjectRegistry registry,
                             ModuleGraphBuilder moduleGraphBuilder,
                             ModuleBoundaryAnalyzer boundaryAnalyzer,
                             ClusterRuntime clusterRuntime, NodeRole role) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.boundaryAnalyzer = boundaryAnalyzer;
        this.clusterRuntime = clusterRuntime;
        this.role = role;
    }

    @Override
    public Integer call() {
        com.pharos.ShutdownSequence.install(); // a signal stops it in order (drain, model calls, hooks)
        try {
            // Readers: wait for the first sync pass (failures are logged and retried). No membership:
            // this process serves no /internal/*, so it never advertises itself (nor drains) and
            // answers from the shards this reader holds only.
            clusterRuntime.startServing(false);
        } catch (Exception e) {
            // stdout is the JSON-RPC channel: report on stderr only
            System.err.println("Cannot start serving: " + e.getMessage());
            return 1;
        }
        McpToolRegistry toolRegistry = new McpToolRegistry(
                searchEngine, registry, moduleGraphBuilder, boundaryAnalyzer, role);
        toolRegistry.warmUp();
        searchEngine.warmUpInBackground();
        McpServer server = new McpServer(toolRegistry);
        server.start();
        return 0;
    }
}
