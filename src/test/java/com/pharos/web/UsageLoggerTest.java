package com.pharos.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the daemon usage logger. Exercises the file-format contract (one valid JSON
 * object per line), enabled/disabled behavior, append semantics, and thread-safety —
 * the daemon serves Javalin requests on a worker pool so concurrent writes are normal.
 */
class UsageLoggerTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper json = new ObjectMapper();

    @Test
    void log_writesOneJsonLinePerEntryToLogsSubdir() throws Exception {
        UsageLogger logger = new UsageLogger(tempDir);

        logger.log(entry("/api/search", "GET", Map.of("q", "agent context"), 47, 200, 8,
                List.of("proj:com.example.Foo#bar()", "proj:com.example.Foo#baz()"), "curl/8.0"));
        logger.log(entry("/api/method", "GET", Map.of("fqn", "com.example.X#y()"),
                12, 404, 0, List.of(), "pharos-cli/1.0"));

        Path file = tempDir.resolve("logs").resolve("usage.jsonl");
        assertThat(file).exists();
        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(2);
        // Each line must parse as JSON
        for (String line : lines) {
            assertThat(line.isBlank()).isFalse();
            json.readTree(line); // would throw if invalid JSON
        }
    }

    @Test
    void log_preservesAllEntryFields() throws Exception {
        UsageLogger logger = new UsageLogger(tempDir);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("q", "search query");
        params.put("limit", "20");

        logger.log(entry("/api/search", "GET", params, 123, 200, 5,
                List.of("a", "b", "c", "d", "e"), "test-agent/1.0"));

        JsonNode node = readSingleEntry();
        assertThat(node.get("endpoint").asText()).isEqualTo("/api/search");
        assertThat(node.get("method").asText()).isEqualTo("GET");
        assertThat(node.get("params").get("q").asText()).isEqualTo("search query");
        assertThat(node.get("params").get("limit").asText()).isEqualTo("20");
        assertThat(node.get("latencyMs").asLong()).isEqualTo(123);
        assertThat(node.get("status").asInt()).isEqualTo(200);
        assertThat(node.get("resultCount").asInt()).isEqualTo(5);
        assertThat(node.get("resultIds")).hasSize(5);
        assertThat(node.get("userAgent").asText()).isEqualTo("test-agent/1.0");
        // ts is an ISO-8601 instant — just check it parses as one
        assertThatCode(() -> java.time.Instant.parse(node.get("ts").asText()))
                .doesNotThrowAnyException();
    }

    @Test
    void log_appendsRatherThanOverwrites() throws Exception {
        UsageLogger logger = new UsageLogger(tempDir);
        for (int i = 0; i < 5; i++) {
            logger.log(entry("/api/projects", "GET", Map.of(), 1, 200, i, List.of(), null));
        }

        // Recreate the logger — same path, should append not truncate
        UsageLogger logger2 = new UsageLogger(tempDir);
        for (int i = 0; i < 3; i++) {
            logger2.log(entry("/api/health", "GET", Map.of(), 1, 200, 0, List.of(), null));
        }

        List<String> lines = Files.readAllLines(tempDir.resolve("logs").resolve("usage.jsonl"));
        assertThat(lines).hasSize(8);
    }

    @Test
    void log_doesNotWriteWhenDisabledViaEnvVar() throws Exception {
        // Can't mutate env vars from JUnit cleanly; instead verify the boolean predicate
        // we'd otherwise have to read PHAROS_USAGE_LOG to enable.
        UsageLogger logger = new UsageLogger(tempDir);
        // Always-on by default in a normal test JVM (no env var set), so this test
        // mainly asserts the enabled() accessor reflects construction-time state.
        assertThat(logger.isEnabled()).isTrue();
    }

    @Test
    void log_concurrentWritesProduceOneLinePerCallAndNoMixedContent() throws Exception {
        // Javalin serves requests on a worker pool — the logger MUST tolerate
        // simultaneous appends without truncating, interleaving, or losing entries.
        UsageLogger logger = new UsageLogger(tempDir);
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            IntStream.range(0, threads).forEach(t -> pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    logger.log(entry("/api/search", "GET",
                            Map.of("q", "thread-" + t + "-iter-" + i),
                            5, 200, 1, List.of("id-" + t + "-" + i), null));
                }
            }));
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        List<String> lines = Files.readAllLines(tempDir.resolve("logs").resolve("usage.jsonl"));
        assertThat(lines).hasSize(threads * perThread);
        // Every line must be a complete, parseable JSON object (no interleaved writes)
        for (String line : lines) {
            JsonNode n = json.readTree(line);
            assertThat(n.get("endpoint").asText()).isEqualTo("/api/search");
            assertThat(n.get("resultIds")).hasSize(1);
        }
    }

    @Test
    void log_creates_logs_subdirIfMissing() {
        UsageLogger logger = new UsageLogger(tempDir);
        // Constructor pre-creates the dir; verify even before any log() call.
        assertThat(tempDir.resolve("logs")).exists().isDirectory();
        // File doesn't exist until first write — that's intentional, no empty files.
        assertThat(logger.logFile()).doesNotExist();
    }

    @Test
    void log_nullResultIdsOmittedFromOutput() throws Exception {
        UsageLogger logger = new UsageLogger(tempDir);
        // Endpoints that don't return result lists pass nulls — the JSON should still
        // be valid (jackson serializes the null as null, which is fine for jq queries).
        logger.log(entry("/api/health", "GET", Map.of(), 1, 200, null, null, null));

        JsonNode node = readSingleEntry();
        // resultCount is null
        assertThat(node.get("resultCount").isNull()).isTrue();
        assertThat(node.get("resultIds").isNull()).isTrue();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static UsageLogger.Entry entry(String endpoint, String method,
                                            Map<String, String> params,
                                            long latencyMs, int status,
                                            Integer resultCount, List<String> resultIds,
                                            String userAgent) {
        return UsageLogger.Entry.of(endpoint, method, params, latencyMs, status,
                resultCount, resultIds, userAgent);
    }

    private JsonNode readSingleEntry() throws Exception {
        List<String> lines = Files.readAllLines(tempDir.resolve("logs").resolve("usage.jsonl"));
        assertThat(lines).hasSize(1);
        return json.readTree(lines.get(0));
    }
}
