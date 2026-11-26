package com.pharos.cluster;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Ruling CPU-R1: every launcher starts a reader with {@link ArithmeticProfile#DETERMINISTIC_FLAGS} and without the
 * vector incubator module, and everything else with the module (Lucene's SIMD code). Each launcher is run for
 * real, with a {@code java} on PATH that records its arguments: the image entrypoint, the {@code ./pharos} Python
 * client and {@code setup.sh}'s daemon start. The operator's pods go through the entrypoint (WorkloadRendererTest
 * checks the env it renders).
 */
class ArithmeticLaunchFlagsTest {

    private static final String MODULE = ArithmeticProfile.VECTOR_MODULE;

    @TempDir
    static Path tmp;
    static Path stubDir;

    @BeforeAll
    static void stubJava() throws IOException {
        assumeTrue(Files.isExecutable(Path.of("/bin/sh")), "needs a POSIX shell");
        stubDir = Files.createDirectories(tmp.resolve("stubbin"));
        Path stub = stubDir.resolve("java");
        // one line per invocation, one argument per tab-separated field
        Files.writeString(stub, "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\t' \"$a\"; done >> \"$JAVA_LOG\"\n"
                + "printf '\\n' >> \"$JAVA_LOG\"\nexit 0\n");
        Files.setPosixFilePermissions(stub, PosixFilePermissions.fromString("rwxr-xr-x"));
    }

    // ── docker/pharos/entrypoint.sh ──────────────────────────────────────────

    @Test
    void entrypointPinsReadersAndLeavesTheOtherRolesNative() throws Exception {
        assertDeterministic(entrypoint("reader", null));
        assertNative(entrypoint("writer", null));
        assertNative(entrypoint("router", null));
        assertNative(entrypoint("all", null));
    }

    @Test
    void entrypointHonoursPharosArithmetic() throws Exception {
        assertNative(entrypoint("reader", "native"));
        assertDeterministic(entrypoint("writer", "deterministic"));
        assertDeterministic(entrypoint("all", "deterministic"));
    }

    @Test
    void entrypointRefusesAnUnknownArithmetic() throws Exception {
        Path home = Files.createTempDirectory(tmp, "ep");
        Result r = runEntrypoint(home, "reader", "fast");
        assertThat(r.exit).isEqualTo(64);
        assertThat(r.javaCalls).isEmpty();
    }

    /** The {@code web} launch of the entrypoint (its last java call; a cluster role validates the config first). */
    private List<String> entrypoint(String role, String arithmetic) throws Exception {
        Path home = Files.createTempDirectory(tmp, "ep");
        Result r = runEntrypoint(home, role, arithmetic);
        assertThat(r.exit).as("entrypoint exit").isZero();
        List<String> last = r.javaCalls.getLast();
        assertThat(last).contains(role.equals("router") ? "router" : "web");
        for (List<String> call : r.javaCalls) assertThat(call).isEqualTo(withoutCommand(last, call));
        return last;
    }

    /** {@code call}'s JVM part must be the web launch's: every java the entrypoint runs gets the same flags. */
    private static List<String> withoutCommand(List<String> web, List<String> call) {
        int jar = web.indexOf("-jar");
        List<String> expected = new ArrayList<>(web.subList(0, jar + 2));
        expected.addAll(call.subList(Math.min(jar + 2, call.size()), call.size()));
        return expected;
    }

    private Result runEntrypoint(Path home, String role, String arithmetic) throws Exception {
        Path config = home.resolve("config.mounted.json");
        Files.writeString(config, "{}");
        Path log = home.resolve("java.log");
        List<String> env = new ArrayList<>(List.of("PATH=" + stubDir + ":/usr/bin:/bin", "JAVA_LOG=" + log,
                "PHAROS_ROLE=" + role, "PHAROS_HOME=" + home.resolve("data"), "PHAROS_CONFIG=" + config));
        if (arithmetic != null) env.add("PHAROS_ARITHMETIC=" + arithmetic);
        List<String> cmd = new ArrayList<>(List.of("/usr/bin/env", "-i"));
        cmd.addAll(env);
        cmd.addAll(List.of("/bin/sh", "docker/pharos/entrypoint.sh"));
        int exit = exec(cmd, Map.of());
        return new Result(exit, calls(log));
    }

    // ── ./pharos (the Python client) ─────────────────────────────────────────

    @Test
    void pythonClientPinsReadersAndLeavesTheOtherRolesNative() throws Exception {
        assumeTrue(onPath("python3"), "needs python3");
        assertDeterministic(pythonFlags("{\"cluster\": {\"role\": \"reader\"}}", null));
        assertNative(pythonFlags("{\"cluster\": {\"role\": \"writer\"}}", null));
        assertNative(pythonFlags("{}", null));
        assertNative(pythonFlags(null, null)); // no config at all
        assertNative(pythonFlags("{\"cluster\": {\"role\": \"reader\"}}", "native"));
        assertDeterministic(pythonFlags("{}", "deterministic"));
    }

    private List<String> pythonFlags(String configJson, String arithmetic) throws Exception {
        Path home = Files.createTempDirectory(tmp, "py");
        Files.createDirectories(home.resolve(".pharos"));
        if (configJson != null) Files.writeString(home.resolve(".pharos/config.json"), configJson);
        Path out = home.resolve("flags.txt");
        String script = String.join("\n",
                "import importlib.machinery, importlib.util, sys",
                "loader = importlib.machinery.SourceFileLoader('pharos_cli', 'pharos')",
                "spec = importlib.util.spec_from_loader('pharos_cli', loader)",
                "m = importlib.util.module_from_spec(spec); loader.exec_module(m)",
                "open(sys.argv[1], 'w').write('\\t'.join(m._jvm_flags()))");
        List<String> cmd = new ArrayList<>(List.of("/usr/bin/env", "-i", "PATH=" + scrubbedPath(), "HOME=" + home));
        if (arithmetic != null) cmd.add("PHAROS_ARITHMETIC=" + arithmetic);
        cmd.addAll(List.of("python3", "-c", script, out.toString()));
        assertThat(exec(cmd, Map.of())).isZero();
        List<String> flags = Arrays.asList(Files.readString(out).split("\t"));
        assertThat(flags).contains("-Dpharos.home=" + home.resolve(".pharos"));
        return flags;
    }

    // ── setup.sh (the daemon it restarts) ────────────────────────────────────

    @Test
    void setupShDaemonStartPinsReadersAndLeavesTheOtherRolesNative() throws Exception {
        assumeTrue(onPath("python3") && Files.isExecutable(Path.of("/bin/bash")), "needs bash and python3");
        assertDeterministic(setupArgs("{\"cluster\": {\"role\": \"reader\"}}", null));
        assertNative(setupArgs("{\"cluster\": {\"role\": \"writer\"}}", null));
        assertNative(setupArgs(null, null));
        assertNative(setupArgs("{\"cluster\": {\"role\": \"reader\"}}", "native"));
        assertDeterministic(setupArgs("{}", "deterministic"));
        // and _restart_daemon really appends them to the daemon's java command line
        String setup = Files.readString(Path.of("setup.sh"));
        assertThat(setup).contains("done < <(_arithmetic_args)");
        String jvmArgs = setup.substring(setup.indexOf("JVM_ARGS=("), setup.indexOf("nohup java"));
        assertThat(jvmArgs).doesNotContain(MODULE); // only through _arithmetic_args
    }

    /** setup.sh's own {@code _arithmetic_args} (and the {@code _cluster_role} it reads), run in a scratch HOME. */
    private List<String> setupArgs(String configJson, String arithmetic) throws Exception {
        Path home = Files.createTempDirectory(tmp, "setup");
        Files.createDirectories(home.resolve(".pharos"));
        if (configJson != null) Files.writeString(home.resolve(".pharos/config.json"), configJson);
        Path out = home.resolve("args.txt");
        String script = "eval \"$(sed -n -e '/^_cluster_role() {/,/^}/p' -e '/^_arithmetic_args() {/,/^}/p' setup.sh)\""
                + " && _arithmetic_args > \"$1\"";
        List<String> cmd = new ArrayList<>(List.of("/usr/bin/env", "-i", "PATH=" + scrubbedPath(), "HOME=" + home));
        if (arithmetic != null) cmd.add("PHAROS_ARITHMETIC=" + arithmetic);
        cmd.addAll(List.of("/bin/bash", "-c", script, "setup-args", out.toString()));
        assertThat(exec(cmd, Map.of())).isZero();
        return Files.readAllLines(out).stream().filter(l -> !l.isBlank()).toList();
    }

    // ── shared ───────────────────────────────────────────────────────────────

    private static void assertDeterministic(List<String> args) {
        assertThat(args).containsAll(ArithmeticProfile.DETERMINISTIC_FLAGS);
        assertThat(args).doesNotContain(MODULE, "-Dpharos.arithmetic=native");
        assertThat(args).noneMatch(a -> a.startsWith("-Dlucene.use") && a.endsWith("=true"));
    }

    private static void assertNative(List<String> args) {
        int i = args.indexOf("--add-modules");
        assertThat(i).as("--add-modules in %s", args).isNotNegative();
        assertThat(args.get(i + 1)).isEqualTo(MODULE);
        assertThat(args).contains("-Dpharos.arithmetic=native");
        assertThat(args).noneMatch(a -> a.startsWith("-Dlucene.use"));
    }

    private record Result(int exit, List<List<String>> javaCalls) {}

    private static List<List<String>> calls(Path log) throws IOException {
        if (!Files.exists(log)) return List.of();
        List<List<String>> out = new ArrayList<>();
        for (String line : Files.readAllLines(log)) {
            if (line.isEmpty()) continue;
            out.add(List.of(line.split("\t")));
        }
        return out;
    }

    private static boolean onPath(String exe) {
        return which(exe) != null;
    }

    /** The directory holding {@code exe} on this JVM's PATH, or null. */
    private static String which(String exe) {
        for (String dir : System.getenv().getOrDefault("PATH", "").split(":")) {
            if (!dir.isEmpty() && Files.isExecutable(Path.of(dir, exe))) return dir;
        }
        return null;
    }

    /** A PATH for the scrubbed environment: python3's directory, then the system's. */
    private static String scrubbedPath() {
        String py = which("python3");
        return (py == null ? "" : py + ":") + "/usr/bin:/bin";
    }

    private static int exec(List<String> cmd, Map<String, String> env) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().putAll(env);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(p.waitFor(60, TimeUnit.SECONDS)).as("finished: %s", out).isTrue();
        if (p.exitValue() != 0) System.err.println(String.join(" ", cmd) + " -> " + p.exitValue() + "\n" + out);
        return p.exitValue();
    }
}
