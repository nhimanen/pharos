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
    /** Routing notes of a cluster query: which shards a filter pruned, and why it could not prune. */
    private final List<String> notes = new ArrayList<>();
    private long overallStart;
    private QueryClassification classification;

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

    /** Adds a note (a routing decision of a cluster query), once. */
    public void note(String note) {
        if (note != null && !notes.contains(note)) notes.add(note);
    }

    public List<String> notes() { return List.copyOf(notes); }

    /** Per stage type, each result's raw score before the graph boost and the other multipliers (P6-R20). */
    private final java.util.Map<String, java.util.Map<String, Float>> rawScores = new java.util.LinkedHashMap<>();

    /** Records {@code id}'s raw score in stage type {@code type} (the first one recorded is kept). */
    public synchronized void rawScore(String type, String id, float raw) {
        if (type == null || id == null) return;
        rawScores.computeIfAbsent(type, k -> new java.util.LinkedHashMap<>()).putIfAbsent(id, raw);
    }

    /**
     * Ruling P6-R20: the raw (pre-graph-boost) score of every result each retrieval stage shaped, by stage type
     * ({@code keyword}, {@code vector}, {@code unified}, ...) then result id: what parity checks compare when
     * the page-normalised graph boost moves the final scores.
     */
    public synchronized java.util.Map<String, java.util.Map<String, Float>> rawScores() {
        java.util.Map<String, java.util.Map<String, Float>> out = new java.util.LinkedHashMap<>();
        rawScores.forEach((t, m) -> out.put(t, java.util.Map.copyOf(m)));
        return out;
    }

    public void setClassification(QueryClassification c) { this.classification = c; }
    public QueryClassification getClassification() { return classification; }

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
        for (String n : notes) sb.append(String.format("%n  [trace] routing: %s", n));
        return sb.toString();
    }
}
