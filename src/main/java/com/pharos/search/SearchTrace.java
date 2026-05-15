package com.pharos.search;

import java.util.ArrayList;
import java.util.List;

/**
 * Timing breakdown for a single search operation.
 *
 * Populated by {@link SearchEngine#searchWithTrace} and printed by
 * {@link com.pharos.cli.SearchCommand} when {@code --debug} is passed.
 */
public class SearchTrace {

    /** A named timing span within the search pipeline. */
    public record Span(String name, long startMs, long endMs) {
        public long durationMs() { return endMs - startMs; }
    }

    private final List<Span> spans = new ArrayList<>();
    private long overallStart;

    void start() {
        overallStart = System.currentTimeMillis();
    }

    public void record(String name, long spanStart) {
        spans.add(new Span(name, spanStart, System.currentTimeMillis()));
    }

    public long totalMs() {
        return spans.isEmpty() ? 0
                : spans.getLast().endMs() - overallStart;
    }

    public List<Span> spans() { return List.copyOf(spans); }

    /**
     * Renders a human-readable breakdown, e.g.:
     * <pre>
     *   [trace] keyword search     12ms
     *   [trace] embed query        45ms
     *   [trace] vector search      18ms
     *   [trace] rrf fusion          1ms
     *   [trace] total              76ms
     * </pre>
     */
    public String format() {
        if (spans.isEmpty()) return "[trace] (no spans recorded)";
        StringBuilder sb = new StringBuilder();
        for (Span s : spans) {
            sb.append(String.format("  [trace] %-26s %dms%n", s.name(), s.durationMs()));
        }
        sb.append(String.format("  [trace] %-26s %dms", "total", totalMs()));
        return sb.toString();
    }
}
