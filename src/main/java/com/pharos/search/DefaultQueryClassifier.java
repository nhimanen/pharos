package com.pharos.search;

import java.util.Arrays;
import java.util.Set;

/**
 * Heuristic query classifier that selects KEYWORD vs HYBRID based on query shape.
 *
 * <p>Classification rules (evaluated in order):
 * <ol>
 *   <li>No spaces → single identifier or qualified name → KEYWORD</li>
 *   <li>Contains {@code #} → FQN format → KEYWORD</li>
 *   <li>Starts with {@code @} → annotation pattern → KEYWORD</li>
 *   <li>Contains a structural Java keyword ({@code throws}, {@code implements}, etc.) → KEYWORD</li>
 *   <li>Contains a stop word or command word → natural-language query → HYBRID</li>
 *   <li>Four or more tokens → likely a sentence or phrase → HYBRID</li>
 *   <li>Two or three tokens and ALL contain at least one uppercase letter → multi-part
 *       identifier (e.g. {@code "ConnectionPool Manager"}) → KEYWORD</li>
 *   <li>Default → HYBRID (safe for short ambiguous queries)</li>
 * </ol>
 */
public class DefaultQueryClassifier implements QueryRouter {

    /**
     * Words that signal natural-language or command intent rather than code identifiers.
     * If any token in the query matches one of these, the query is treated as natural language.
     */
    private static final Set<String> STOP_WORDS = Set.of(
            // question / wh-words
            "how", "where", "what", "when", "why", "which", "who",
            // articles / prepositions / conjunctions
            "the", "a", "an", "is", "are", "was", "were", "be",
            "all", "for", "to", "of", "in", "with", "that", "this", "by",
            "and", "or", "not",
            // command words — intent markers, not code terms
            "find", "get", "list", "show", "give", "return", "fetch"
    );

    /**
     * Substrings whose presence signals a structural query (Java keyword patterns).
     * Checked on the lowercased query.
     */
    private static final String[] STRUCTURAL_MARKERS = {
            "throws ", "implements ", "extends ", "@override", "@deprecated"
    };

    @Override
    public QueryClassification classify(String query) {
        if (query == null || query.isBlank()) return QueryClassification.of(SearchRequest.SearchType.HYBRID);

        String q = query.trim();

        if (!q.contains(" ")) return kw();
        if (q.contains("#"))  return kw();
        if (q.startsWith("@")) return kw();

        String lower = q.toLowerCase();
        for (String marker : STRUCTURAL_MARKERS) {
            if (lower.contains(marker)) return kw();
        }

        String[] tokens = lower.split("\\s+");
        for (String token : tokens) {
            if (STOP_WORDS.contains(token)) return hy();
        }

        if (tokens.length >= 4) return hy();

        String[] originalTokens = q.split("\\s+");
        boolean allIdentifiers = Arrays.stream(originalTokens)
                .allMatch(t -> t.chars().anyMatch(Character::isUpperCase));
        if (allIdentifiers) return kw();

        return hy();
    }

    private static QueryClassification kw() { return QueryClassification.of(SearchRequest.SearchType.KEYWORD, null, "KEYWORD"); }
    private static QueryClassification hy() { return QueryClassification.of(SearchRequest.SearchType.HYBRID, null, "HYBRID"); }
}
