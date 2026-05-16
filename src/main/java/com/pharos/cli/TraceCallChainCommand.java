package com.pharos.cli;

import com.pharos.search.CallChainResult;
import com.pharos.search.SearchEngine;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "trace",
        description = "Traverse the call graph from a method and show bodies of visited nodes",
        mixinStandardHelpOptions = true
)
public class TraceCallChainCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Starting method FQN: com.example.MyClass#myMethod(String)")
    private String fqn;

    @Option(names = {"--depth", "-d"}, description = "Hops to traverse (default: 2, max: 4)", defaultValue = "2")
    private int depth;

    @Option(names = {"--direction"}, description = "callees | callers | both (default: callees)", defaultValue = "callees")
    private String direction;

    @Option(names = {"--max-body"}, description = "Truncate each body to this many chars (default: 500, 0 = no truncation)", defaultValue = "500")
    private int maxBodyChars;

    private final SearchEngine searchEngine;

    public TraceCallChainCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            int clampedDepth = Math.min(4, Math.max(1, depth));
            CallChainResult chain = searchEngine.traceCallChain(fqn, clampedDepth, direction, maxBodyChars);

            if (chain.totalNodes() == 0) {
                System.out.println("No nodes found for: " + fqn);
                return 0;
            }

            System.out.printf("Call chain from: %s%n", chain.root());
            System.out.printf("Direction: %s | Depth: %d | Nodes: %d%s%n%n",
                    chain.direction(), chain.maxDepth(), chain.totalNodes(),
                    chain.truncated() ? " (truncated at 50)" : "");

            for (CallChainResult.ChainNode node : chain.nodes()) {
                String indent = "  ".repeat(node.depth());
                String prefix = node.depth() == 0 ? "" : "↳ ";
                System.out.printf("%s%s[%d] %s%n", indent, prefix, node.depth(), node.fqn());
                if (node.filePath() != null) {
                    System.out.printf("%s    %s:%d-%d%n", indent, node.filePath(), node.startLine(), node.endLine());
                }
                if (node.body() != null && !node.body().isBlank()) {
                    for (String line : node.body().split("\n")) {
                        System.out.printf("%s    %s%n", indent, line);
                    }
                }
                System.out.println();
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
