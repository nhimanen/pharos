package com.pharos;

import com.pharos.config.IndexConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A config.json that IndexConfig.load() rejects must not lock the user out of `pharos embeddings`. */
class MainConfigErrorTest {

    @TempDir Path home;
    private final PrintStream origOut = System.out;
    private final PrintStream origErr = System.err;
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;

    private static final String BAD = "{\"verbose\":true,\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"x\","
            + "\"url\":\"hf://a/b\",\"dimensions\":-1}]}";

    @BeforeEach
    void setUp() throws Exception {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(err, true));
        Files.writeString(home.resolve("config.json"), BAD);
        assertThatThrownBy(() -> IndexConfig.loadFrom(home)).isInstanceOf(IllegalArgumentException.class);
    }

    @AfterEach
    void restore() { System.setOut(origOut); System.setErr(origErr); }

    @Test
    void embeddingsDisableRunsDespiteAnInvalidConfig() throws Exception {
        int rc = Main.runWithInvalidConfig(new String[]{"embeddings", "disable"},
                new IllegalArgumentException("boom"), home);
        assertThat(rc).isZero();
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).isEmpty();
        assertThat(Files.readString(home.resolve("config.json"))).contains("verbose");
    }

    @Test
    void embeddingsStatusReportsTheProblemAndTheFix() {
        int rc = Main.runWithInvalidConfig(new String[]{"embeddings", "status"},
                new IllegalArgumentException("boom"), home);
        assertThat(rc).isNotZero();
        assertThat(err.toString()).contains("config.json").contains("pharos embeddings disable");
    }

    @Test
    void otherCommandsPrintHowToFixTheFile() {
        int rc = Main.runWithInvalidConfig(new String[]{"search", "x"},
                new IllegalArgumentException("dimensions must be > 0"), home);
        assertThat(rc).isNotZero();
        assertThat(err.toString()).contains("dimensions must be > 0")
                .contains(home.resolve("config.json").toString())
                .contains("pharos embeddings disable");
    }

    @Test
    void djlTelemetryIsOptedOut() {
        Main.optOutOfDjlTracking();
        assertThat(System.getProperty("OPT_OUT_TRACKING")).isEqualTo("true");
    }
}
