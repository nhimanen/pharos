package com.pharos.cli;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.pharos.config.IndexConfig;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * {@code pharos config}: commands on a config.json file. They run on the given file only: the
 * node's own config is not loaded first ({@code Main} dispatches {@code config} before it), so a
 * broken {@code ~/.pharos/config.json} can still be checked, and nothing (object store, index,
 * daemon) is touched.
 */
@Command(
        name = "config",
        description = "Commands on a pharos config.json file",
        mixinStandardHelpOptions = true,
        versionProvider = PharosVersionProvider.class,
        subcommands = {ConfigCommand.Validate.class}
)
public class ConfigCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    /**
     * {@code pharos config validate <file>}: reads the file as {@code config.json} and runs the same
     * validation a node runs at startup (with {@code cluster.tokenEnv} read from this process's
     * environment). Prints {@code ok} and exits {@value #EXIT_OK}, or prints the problem to stderr and
     * exits {@value #EXIT_INVALID}. Messages never echo a value from the file: a JSON syntax error is
     * reported by its position only, since the text near it may be the token.
     */
    @Command(
            name = "validate",
            description = "Validate a config.json file as a node would at startup; prints \"ok\" or the problem",
            mixinStandardHelpOptions = true,
            versionProvider = PharosVersionProvider.class,
            exitCodeListHeading = "%nExit codes:%n",
            exitCodeList = {
                    "0:the config is valid (prints \"ok\")",
                    "2:the config is invalid, unreadable or not JSON (the problem goes to stderr)"}
    )
    public static class Validate implements Callable<Integer> {

        static final int EXIT_OK = 0;
        static final int EXIT_INVALID = 2;

        @Spec
        CommandSpec spec;

        @Parameters(index = "0", paramLabel = "<file>", description = "The config.json file to check")
        Path file;

        @Override
        public Integer call() {
            String problem = check(file);
            if (problem == null) {
                spec.commandLine().getOut().println("ok");
                spec.commandLine().getOut().flush();
                return EXIT_OK;
            }
            spec.commandLine().getErr().println("invalid config " + file + ": " + problem);
            spec.commandLine().getErr().flush();
            return EXIT_INVALID;
        }

        /** The problem with {@code file}, or null when it is a valid config. */
        static String check(Path file) {
            if (!Files.isRegularFile(file)) return "no such file";
            try {
                IndexConfig.load(file);
                return null;
            } catch (JsonMappingException e) {
                if (e.getCause() instanceof com.fasterxml.jackson.core.JsonParseException syntax) {
                    return "not valid JSON" + at(syntax.getLocation()); // wrapped while binding a field
                }
                // a value of the wrong type (or a setter that refused it): name the field, not the value
                String path = e.getPath().stream()
                        .map(r -> r.getFieldName() != null ? r.getFieldName() : "[" + r.getIndex() + "]")
                        .collect(Collectors.joining(".")).replace(".[", "[");
                return (path.isEmpty() ? "the file is not a config object" : path + " has an invalid value")
                        + at(e.getLocation());
            } catch (JsonProcessingException e) {
                return "not valid JSON" + at(e.getLocation());
            } catch (NoSuchFileException e) {
                return "no such file";
            } catch (IOException e) {
                return "cannot read it (" + e.getClass().getSimpleName() + ")";
            } catch (IllegalStateException | IllegalArgumentException e) {
                return e.getMessage();
            }
        }

        private static String at(JsonLocation loc) {
            if (loc == null || loc.getLineNr() < 1) return "";
            return " at line " + loc.getLineNr() + ", column " + loc.getColumnNr();
        }
    }

    /** True when {@code args} run a {@code config} command, which needs no node set up (see the class doc). */
    public static boolean handles(String[] args) {
        return args.length > 0 && "config".equals(args[0]);
    }

    /**
     * Runs a {@code config ...} command line (as {@code Main} receives it, {@code config} first) and
     * returns its exit code. The root {@code pharos} command cannot be built here: its other
     * subcommands need the wired dependencies, so this runs {@code config} as its own root.
     */
    public static int execute(String[] args) {
        return execute(args, null, null);
    }

    /** {@link #execute(String[])} writing to {@code out} and {@code err} (null: the console). */
    static int execute(String[] args, java.io.PrintWriter out, java.io.PrintWriter err) {
        CommandLine cmd = new CommandLine(new ConfigCommand()).setCommandName("pharos config");
        if (out != null) cmd.setOut(out);
        if (err != null) cmd.setErr(err);
        return cmd.execute(java.util.Arrays.copyOfRange(args, handles(args) ? 1 : 0, args.length));
    }
}
