package com.pharos.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code pharos config validate <file>}: exit 0 and "ok", or exit 2 with the validation message. */
class ConfigValidateCommandTest {

    @TempDir
    Path dir;

    private record Run(int exit, String out, String err) {}

    /** Runs as {@code java -jar pharos.jar config validate <file>} does ({@code Main} hands it over first). */
    private static Run run(String... args) {
        assertThat(ConfigCommand.handles(args)).isTrue();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int exit = ConfigCommand.execute(args, new PrintWriter(out, true), new PrintWriter(err, true));
        return new Run(exit, out.toString(), err.toString());
    }

    @Test
    void registeredUnderTheRootCommand() {
        assertThat(CodeSearchCommand.class.getAnnotation(CommandLine.Command.class).subcommands())
                .contains(ConfigCommand.class);
        assertThat(ConfigCommand.handles(new String[]{"index", "config"})).isFalse();
        assertThat(ConfigCommand.handles(new String[]{})).isFalse();
    }

    private Path file(String name, String json) throws Exception {
        Path f = dir.resolve(name);
        Files.writeString(f, json);
        return f;
    }

    @Test
    void configValidateReportsErrorsWithExit2() throws Exception {
        // a valid reader config (the token from env): exit 0, "ok"
        Path ok = file("ok.json", "{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"},\"advertiseUrl\":\"http://r:7171\",\"tokenEnv\":\"PATH\","
                + "\"status\":{\"removeReadersLoad\":0.3,\"writers\":{\"backlogShards\":2}}}}");
        Run r = run("config", "validate", ok.toString());
        assertThat(r.exit()).isZero();
        assertThat(r.out().strip()).isEqualTo("ok");

        // an empty object is the single-node default: valid
        assertThat(run("config", "validate", file("empty.json", "{}").toString()).exit()).isZero();

        // token and tokenEnv together: exit 2, the validation message, never the token
        Path both = file("both.json", "{\"cluster\":{\"token\":\"s3cret-value\",\"tokenEnv\":\"PATH\"}}");
        r = run("config", "validate", both.toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("cluster.token", "cluster.tokenEnv", "mutually exclusive")
                .doesNotContain("s3cret-value");
        assertThat(r.out()).doesNotContain("ok");

        // a bad threshold
        r = run("config", "validate", file("bad.json", "{\"cluster\":{\"status\":{\"removeReadersLoad\":2}}}").toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("cluster.status.removeReadersLoad");

        // a reader without a token
        r = run("config", "validate", file("notoken.json", "{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"},\"advertiseUrl\":\"http://r:7171\"}}").toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("cluster.token is required");

        // not JSON: exit 2 with the position only (the text near the error may be a secret)
        r = run("config", "validate", file("broken.json", "{\"cluster\":{\"token\": s3cret-value}}").toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("not valid JSON", "line 1").doesNotContain("s3cret-value");

        // a type mismatch is a config error too
        r = run("config", "validate", file("type.json", "{\"cluster\":{\"pollIntervalSeconds\":\"often\"}}").toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("pollIntervalSeconds");

        // a missing file
        r = run("config", "validate", dir.resolve("absent.json").toString());
        assertThat(r.exit()).isEqualTo(2);
        assertThat(r.err()).contains("absent.json");
    }
}
