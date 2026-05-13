package com.pharos.cli;

import picocli.CommandLine;
import picocli.CommandLine.*;

/**
 * Root picocli command. Prints help if invoked with no subcommand.
 */
@Command(
        name = "pharos",
        description = "Code-aware search tool with BM25 + vector search and call graph analysis",
        subcommands = {
                IndexCommand.class,
                SearchCommand.class,
                GetMethodCommand.class,
                CallersCommand.class,
                CalleesCommand.class,
                GetClassCommand.class,
                PathCommand.class,
                ProjectsCommand.class,
                LinkCommand.class,
                StatsCommand.class,
                ModulesCommand.class,
                ModuleDepsCommand.class,
                ModulePathCommand.class,
                McpServerCommand.class,
                WebCommand.class,
                RemoveIndexCommand.class,
                CommandLine.HelpCommand.class
        },
        mixinStandardHelpOptions = true,
        version = "pharos 1.0.0"
)
public class CodeSearchCommand implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
