package com.pharos.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * Hapax density scanner for q-log IDF estimation.
 *
 * Computes the fraction of unique terms that appear in exactly one document
 * (hapax legomena) for identifier-heavy fields, then estimates the Tsallis q
 * parameter via the closed-form formula from Radha & Goktas (2026):
 *   q = 1 − 7.28 × htok   clamped to [0.05, 0.95]
 *
 * htok is the average hapax density across methodName and className fields.
 * At q=1 the q-log reduces to standard log (no change). For q<1 — typical for
 * code corpora — rare identifiers get amplified beyond what standard log gives.
 *
 * Results are stored per-project at <indexDir>/<project>/search-params.json
 * and loaded at search time by KeywordSearchStrategy.
 */
public final class HapaxStats {

    private static final Logger log = LoggerFactory.getLogger(HapaxStats.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PARAMS_FILE = "search-params.json";

    private static final String[] HAPAX_FIELDS = {"methodName", "className"};

    // From Radha & Goktas (2026): q = 1 − 7.28 × htok
    private static final double Q_COEFF = 7.28;
    private static final double Q_MIN   = 0.05;
    private static final double Q_MAX   = 0.95;

    private HapaxStats() {}

    /**
     * Fraction of unique terms in {@code field} that appear in exactly one document.
     */
    public static double hapaxDensity(IndexReader reader, String field) throws IOException {
        Terms terms = MultiTerms.getTerms(reader, field);
        if (terms == null) return 0.0;
        TermsEnum te = terms.iterator();
        long hapax = 0, total = 0;
        while (te.next() != null) {
            total++;
            if (te.docFreq() == 1) hapax++;
        }
        return total == 0 ? 0.0 : (double) hapax / total;
    }

    /** q = 1 − 7.28 × htok, clamped to [0.05, 0.95]. */
    public static double estimateQ(double htok) {
        return Math.max(Q_MIN, Math.min(Q_MAX, 1.0 - Q_COEFF * htok));
    }

    /**
     * Scans identifier fields, logs per-field stats, and saves results to
     * {@code <indexDir>/<project>/search-params.json}. Returns the estimated q.
     *
     * This is meant to be called once after indexing, before running searches.
     */
    public static double computeAndSave(IndexReader reader, Path indexDir, String project)
            throws IOException {
        double totalHtok = 0.0;
        int measured = 0;
        for (String field : HAPAX_FIELDS) {
            double htok = hapaxDensity(reader, field);
            System.out.printf("  [hapax] project='%s' field='%s'  htok=%.4f  q=%.4f%n",
                    project, field, htok, estimateQ(htok));
            if (htok > 0.0) { totalHtok += htok; measured++; }
        }
        double avgHtok = measured == 0 ? 0.0 : totalHtok / measured;
        double q = estimateQ(avgHtok);

        Path dir = indexDir.resolve(project);
        Files.createDirectories(dir);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("qIdf", q);
        params.put("hapaxDensity", avgHtok);
        params.put("note", "q = 1 - 7.28 * htok (Radha & Goktas 2026)");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(dir.resolve(PARAMS_FILE).toFile(), params);

        System.out.printf("  [hapax] project='%s'  avgHtok=%.4f  q=%.4f  → saved to %s%n",
                project, avgHtok, q, dir.resolve(PARAMS_FILE));
        log.info("q-IDF stats saved for '{}': htok={} q={}", project, avgHtok, q);
        return q;
    }

    /**
     * Loads q from {@code <indexDir>/<project>/search-params.json}.
     * Returns empty if the file is absent or unreadable (search falls back to standard IDF).
     */
    public static OptionalDouble load(Path indexDir, String project) {
        if (project == null || project.isBlank()) return OptionalDouble.empty();
        Path file = indexDir.resolve(project).resolve(PARAMS_FILE);
        if (!Files.exists(file)) return OptionalDouble.empty();
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = MAPPER.readValue(file.toFile(), Map.class);
            Object qVal = params.get("qIdf");
            if (qVal instanceof Number n) return OptionalDouble.of(n.doubleValue());
        } catch (IOException e) {
            log.warn("Could not read search-params for '{}': {}", project, e.getMessage());
        }
        return OptionalDouble.empty();
    }
}
