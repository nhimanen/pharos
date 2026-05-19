package com.pharos.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Two-layer query classifier for {@code type=auto} routing.
 *
 * <h3>Layer 1 — FST phrase tagger</h3>
 * Matches explicit intent phrases from {@code /intent-code-search.csv} using a
 * Lucene FST compiled at startup. Each phrase maps to an intent category and an
 * optional docType filter:
 * <ul>
 *   <li>{@code BEHAVIORAL} → HYBRID (behavioral/intent queries need vector recall)</li>
 *   <li>{@code INTERFACE}  → HYBRID + {@code docType=class} (interfaces are abstract types)</li>
 *   <li>{@code JAVADOC}    → KEYWORD + optional {@code docType} (exact javadoc term match)</li>
 *   <li>{@code CONFIG}     → KEYWORD (config setter/getter names, strong BM25 signal)</li>
 *   <li>{@code LIFECYCLE}  → KEYWORD (exception/lifecycle class names are exact identifiers)</li>
 * </ul>
 *
 * <h3>Layer 2 — Heuristic fallback (no FST match)</h3>
 * Applied when no intent phrase is detected:
 * <ol>
 *   <li>No spaces → single identifier → KEYWORD</li>
 *   <li>Contains {@code #} → FQN → KEYWORD</li>
 *   <li>Any CamelCase or ACRONYM token → technical name → KEYWORD</li>
 *   <li>≥4 tokens AND no stop words → technical phrase → KEYWORD</li>
 *   <li>≥4 tokens AND stop words present → natural language → HYBRID</li>
 *   <li>2–3 tokens, all contain uppercase → multi-part identifier → KEYWORD</li>
 *   <li>Default → HYBRID</li>
 * </ol>
 */
public class FstQueryClassifier implements QueryClassifier {

    private static final Logger log = LoggerFactory.getLogger(FstQueryClassifier.class);

    private static final Set<String> STOP_WORDS = Set.of(
            "how", "where", "what", "when", "why", "which", "who",
            "the", "a", "an", "is", "are", "was", "were", "be",
            "all", "for", "to", "of", "in", "with", "that", "this", "by",
            "and", "or", "not",
            "find", "get", "show", "give", "fetch"
    );

    private final IntentTagger tagger;

    /** Constructs the classifier, compiling the FST from the classpath CSV. */
    public FstQueryClassifier() {
        IntentTagger t = null;
        try {
            t = IntentTagger.fromCsv("/intent-code-search.csv");
        } catch (IOException e) {
            log.warn("FstQueryClassifier: failed to load intent CSV, falling back to heuristic only. {}", e.getMessage());
        }
        this.tagger = t;
    }

    @Override
    public QueryClassification classify(String query) {
        if (query == null || query.isBlank()) return QueryClassification.of(SearchRequest.SearchType.HYBRID);

        // ── Layer 1: FST phrase match ─────────────────────────────────────────
        if (tagger != null) {
            try {
                String[] hit = tagger.tag(query.toLowerCase());
                if (hit != null) {
                    return mapIntent(hit[0], hit[1]);
                }
            } catch (IOException e) {
                log.debug("IntentTagger.tag() failed: {}", e.getMessage());
            }
        }

        // ── Layer 2: Heuristic fallback ───────────────────────────────────────
        return heuristic(query);
    }

    private static QueryClassification mapIntent(String intent, String docType) {
        return switch (intent) {
            case "BEHAVIORAL" -> QueryClassification.of(SearchRequest.SearchType.HYBRID, docType);
            case "INTERFACE"  -> QueryClassification.of(SearchRequest.SearchType.HYBRID, docType);
            case "JAVADOC"    -> QueryClassification.of(SearchRequest.SearchType.KEYWORD, docType);
            case "CONFIG"     -> QueryClassification.of(SearchRequest.SearchType.KEYWORD, docType);
            case "LIFECYCLE"  -> QueryClassification.of(SearchRequest.SearchType.KEYWORD, docType);
            default           -> QueryClassification.of(SearchRequest.SearchType.HYBRID, docType);
        };
    }

    private static QueryClassification heuristic(String query) {
        String q = query.trim();

        // Rule 1: single token
        if (!q.contains(" ")) return kw();

        // Rule 2: FQN
        if (q.contains("#")) return kw();

        // Rule 3: annotation
        if (q.startsWith("@")) return kw();

        // Rule 4: structural Java keyword
        String lower = q.toLowerCase();
        for (String m : new String[]{"throws ", "implements ", "extends ", "@override", "@deprecated"}) {
            if (lower.contains(m)) return kw();
        }

        // Rule 5: CamelCase or ACRONYM token → technical identifier in a multi-word query
        for (String t : q.split("\\s+")) {
            if (isTechnicalToken(t)) return kw();
        }

        // Rule 6: classify by stop-word presence + length
        String[] tokens  = lower.split("\\s+");
        boolean hasStop  = false;
        for (String t : tokens) {
            if (STOP_WORDS.contains(t)) { hasStop = true; break; }
        }

        if (!hasStop && tokens.length >= 4) return kw();   // technical phrase
        if ( hasStop && tokens.length >= 4) return hy();   // natural-language sentence

        // Rule 7: 2–3 tokens where every token has uppercase → multi-part identifier
        String[] orig = q.split("\\s+");
        if (orig.length <= 3 && allHaveUppercase(orig)) return kw();

        return hy();
    }

    private static boolean isTechnicalToken(String t) {
        // PascalCase: starts upper, has lower, then upper again (e.g. HnswGraphBuilder)
        if (t.matches("[A-Z][a-z]+[A-Z].*")) return true;
        // camelCase: lowercase then uppercase (e.g. indexWriter)
        if (t.matches(".*[a-z][A-Z].*")) return true;
        // ACRONYM ≥3 chars (e.g. HNSW, ONNX, BM25, YQL)
        if (t.matches("[A-Z0-9]{3,}")) return true;
        return false;
    }

    private static boolean allHaveUppercase(String[] tokens) {
        for (String t : tokens) {
            if (t.chars().noneMatch(Character::isUpperCase)) return false;
        }
        return true;
    }

    private static QueryClassification kw() {
        return QueryClassification.of(SearchRequest.SearchType.KEYWORD);
    }

    private static QueryClassification hy() {
        return QueryClassification.of(SearchRequest.SearchType.HYBRID);
    }
}
