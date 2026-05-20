package com.pharos.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Appends one JSON line per daemon API request to {@code ~/.pharos/logs/usage.jsonl}.
 *
 * <p>The goal is to capture how pharos is actually used (which queries, which endpoints,
 * latency distribution, what results came back) so usage patterns from AI agents vs
 * humans can be compared and the search pipeline tuned against real traffic.
 *
 * <p>Default: <b>ON</b>. Disable by setting {@code PHAROS_USAGE_LOG=off} (also accepts
 * {@code 0}, {@code false}, {@code no}) in the daemon's environment.
 *
 * <p>Output format: one JSON object per line ({@code .jsonl}), grep/jq/pandas friendly.
 *
 * <p>Writes are synchronized on a per-instance lock — Javalin handlers run on multiple
 * threads, and append failures must never propagate back into the request path, so any
 * I/O exception is logged via SLF4J and swallowed.
 */
public class UsageLogger {

    private static final Logger log = LoggerFactory.getLogger(UsageLogger.class);

    private final Path logFile;
    private final boolean enabled;
    private final ObjectMapper json;
    private final Object writeLock = new Object();

    public UsageLogger(Path pharosDir) {
        this.enabled = isEnabledFromEnv();
        Path logDir = pharosDir.resolve("logs");
        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
            log.warn("Could not create usage log dir {}: {}", logDir, e.getMessage());
        }
        this.logFile = logDir.resolve("usage.jsonl");
        this.json = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        if (enabled) {
            log.info("Usage logging enabled → {}", logFile);
        } else {
            log.info("Usage logging disabled (PHAROS_USAGE_LOG is off)");
        }
    }

    private static boolean isEnabledFromEnv() {
        String v = System.getenv("PHAROS_USAGE_LOG");
        if (v == null) return true;            // default ON
        v = v.trim().toLowerCase(Locale.ROOT);
        return !(v.equals("off") || v.equals("0") || v.equals("false") || v.equals("no"));
    }

    public boolean isEnabled() {
        return enabled;
    }

    Path logFile() {  // exposed for tests
        return logFile;
    }

    public void log(Entry entry) {
        if (!enabled) return;
        try {
            String line = json.writeValueAsString(entry) + "\n";
            synchronized (writeLock) {
                Files.writeString(logFile, line,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (Exception e) {
            // Usage logging must never break the request — log via slf4j and continue.
            log.warn("Usage log write failed: {}", e.getMessage());
        }
    }

    /**
     * One request line. {@code resultCount} and {@code resultIds} are nullable —
     * endpoints that don't return result lists leave them out so the JSON stays clean.
     */
    public record Entry(
            String ts,
            String endpoint,
            String method,
            Map<String, String> params,
            long latencyMs,
            int status,
            Integer resultCount,
            List<String> resultIds,
            String userAgent
    ) {
        public static Entry of(String endpoint, String method, Map<String, String> params,
                                long latencyMs, int status, Integer resultCount,
                                List<String> resultIds, String userAgent) {
            return new Entry(Instant.now().toString(), endpoint, method, params,
                    latencyMs, status, resultCount, resultIds, userAgent);
        }
    }
}
