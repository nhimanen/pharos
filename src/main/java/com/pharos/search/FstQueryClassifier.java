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
public class FstQueryClassifier implements QueryRouter {

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
        SearchRequest.SearchType type = switch (intent) {
            case "BEHAVIORAL",
                 "INTERFACE", "ABSTRACT",
                 "ENUM", "RECORD", "ANNOTATION",
                 "IMPLEMENTATION"              -> SearchRequest.SearchType.HYBRID;
            case "CONFIG"                      -> SearchRequest.SearchType.HYBRID_RERANKED;
            default                            -> SearchRequest.SearchType.KEYWORD;
        };
        return QueryClassification.of(type, docType, intent);
    }

    private static QueryClassification heuristic(String query) {
        String q = query.trim();

        // Rule 1: single token → KEYWORD (exact identifier)
        if (!q.contains(" ")) return kw("KEYWORD");

        // Rule 2: FQN
        if (q.contains("#")) return kw("KEYWORD");

        // Rule 3: annotation
        if (q.startsWith("@")) return kw("KEYWORD");

        // Rule 4: structural Java keyword
        String lower = q.toLowerCase();
        for (String m : new String[]{"throws ", "implements ", "extends ", "@override", "@deprecated"}) {
            if (lower.contains(m)) return kw("KEYWORD");
        }

        // Rule 5: CamelCase or ACRONYM → multi-word query containing a technical identifier
        for (String t : q.split("\\s+")) {
            if (isTechnicalToken(t)) return kw("KEYWORD");
        }

        // Rule 6: classify by stop-word presence + length
        String[] tokens = lower.split("\\s+");
        boolean hasStop = false;
        for (String t : tokens) {
            if (STOP_WORDS.contains(t)) { hasStop = true; break; }
        }

        if (!hasStop && tokens.length >= 4) return kw("KEYWORD_TECHNICAL");  // multi-word tech phrase
        if ( hasStop && tokens.length >= 4) return hy("HYBRID");             // natural-language sentence

        // Rule 7: 2–3 tokens where every token has uppercase → multi-part identifier
        String[] orig = q.split("\\s+");
        if (orig.length <= 3 && allHaveUppercase(orig)) return kw("KEYWORD");

        return hy("HYBRID");
    }

    private static boolean isTechnicalToken(String t) {
        if (t.matches("[A-Z][a-z]+[A-Z].*")) return true;  // PascalCase
        if (t.matches(".*[a-z][A-Z].*"))     return true;  // camelCase
        if (t.matches("[A-Z0-9]{3,}"))        return true;  // ACRONYM
        return false;
    }

    private static boolean allHaveUppercase(String[] tokens) {
        for (String t : tokens) {
            if (t.chars().noneMatch(Character::isUpperCase)) return false;
        }
        return true;
    }

    private static QueryClassification kw(String intent) {
        return QueryClassification.of(SearchRequest.SearchType.KEYWORD, null, intent);
    }

    private static QueryClassification hy(String intent) {
        return QueryClassification.of(SearchRequest.SearchType.HYBRID, null, intent);
    }
}
