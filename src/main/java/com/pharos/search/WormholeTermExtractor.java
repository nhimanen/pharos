package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Extracts the characteristic terms of a "foreground" document set relative to the
 * full index ("background") — the wormhole-vectors technique by Trey Grainger.
 *
 * <p>Given an ordered list of Lucene doc IDs (e.g. the top-k hits of a
 * {@link org.apache.lucene.search.KnnFloatVectorQuery}), this class computes which
 * terms are statistically over-represented in those documents compared to the corpus
 * at large.  The result describes <em>what semantic concept the foreground occupies</em>.
 *
 * <p>Scoring pipeline:
 * <ol>
 *   <li><b>Statistical significance (PPMI)</b> — {@code max(0, log(P(term|fg) / P(term|bg)))},
 *       using {@link IndexReader#docFreq(Term)} for the background count.  Terms over-represented
 *       in the foreground get positive scores; terms equally or less common than in the background
 *       are clamped to 0 (positive PMI only).  The log scale compresses extreme ratios and gives
 *       a natural zero-point: a score of 0 means "no foreground signal, positional weight only".
 *       Consistent with the PPMI scoring already used by
 *       {@link com.pharos.analysis.ConceptMiner}.</li>
 *   <li><b>Positional score</b> — {@code Σ 1/√(rank+1)} over the top-
 *       {@code positionalWindow} foreground documents that contain the term.  Terms
 *       concentrated near rank 0 (the best vector matches) are preferred over terms
 *       that only appear in lower-ranked foreground docs.</li>
 *   <li><b>Combination</b> — min-max normalise both scores then blend:
 *       {@code combined = 0.4 × normStat + 0.6 × normPos}.</li>
 * </ol>
 *
 * <p>Typical use-cases in Pharos:
 * <ul>
 *   <li>Feed extracted terms into a secondary BM25 pass to bridge the vocabulary gap
 *       between user language and identifier naming.</li>
 *   <li>Surface "concept tags" alongside vector search results to explain why they matched.</li>
 *   <li>Label semantic clusters for offline {@link com.pharos.analysis.ConceptMiner} synonym
 *       generation.</li>
 * </ul>
 */
public class WormholeTermExtractor {

    /** Stored text fields analysed by default (highest signal first). */
    public static final String[] DEFAULT_FIELDS = {
            DocumentMapper.F_METHOD_NAME,
            DocumentMapper.F_CLASS_NAME,
            DocumentMapper.F_JAVADOC,
            DocumentMapper.F_BODY
    };

    /**
     * Fields for per-class ConceptMiner extraction: excludes {@code className} so the
     * class name itself never shows up as its own top characteristic term.
     * Body is excluded here because it produces Java code-expression noise
     * (e.g. {@code this.field}, {@code graph.flush}) that is unhelpful for synonym mining.
     */
    public static final String[] METHOD_ONLY_FIELDS = {
            DocumentMapper.F_METHOD_NAME,
            DocumentMapper.F_JAVADOC
    };

    public static final int DEFAULT_TOP_N             = 10;
    public static final int DEFAULT_POSITIONAL_WINDOW = 5;

    /** Minimum analysed token length — removes residual short tokens ("is", "to"). */
    private static final int MIN_TERM_LENGTH = 3;

    /**
     * Stop words applied at tokenisation time.  Combines English function words with
     * Java/Javadoc boilerplate so that noise terms like "the", "returns", "method"
     * don't compete with genuine domain vocabulary in the scored output.
     */
    private static final CharArraySet CODE_STOP_WORDS;
    static {
        CODE_STOP_WORDS = new CharArraySet(List.of(
            // English function words
            "a","an","and","are","as","at","be","but","by","for","if","in","into",
            "is","it","no","not","of","on","or","such","that","the","their","then",
            "there","these","they","this","to","was","will","with","from","about",
            "also","just","only","very","over","under","after","before","between",
            "during","through","without","within","against","which","who","what",
            "when","where","how","than","can","may","might","each","one","two",
            "per","i","e","g","etc","eg","ie",
            // Java / Javadoc boilerplate
            "returns","return","param","throws","method","class","interface",
            "object","instance","type","value","result","list","array","map",
            "null","true","false","public","private","protected","static",
            "final","abstract","new","get","set","void","given","using","used",
            "creates","created","provides","represents","contains","implements",
            "extends","allows","calls","called","whether","non","sub","pre",
            "example","see","since","deprecated","note","use"
        ), false);
    }

    /** Blend weights for the combined score. */
    private static final double STAT_WEIGHT = 0.4;
    private static final double POS_WEIGHT  = 0.6;

    // ── public API ────────────────────────────────────────────────────────────

    /**
     * A single characteristic term with its component scores.
     *
     * @param term             lowercased, analysed token
     * @param statisticalScore PPMI: {@code max(0, log(P(term|fg) / P(term|bg)))};
     *                         0 means equally distributed or under-represented in foreground
     * @param positionalScore  {@code Σ 1/√(rank+1)} weighted by rank within positional window
     * @param combinedScore    normalised blend used for ranking (primary sort key)
     * @param foregroundCount  foreground documents containing this term
     * @param backgroundCount  approximate index-wide documents containing this term
     */
    public record WormholeTerm(
            String term,
            double statisticalScore,
            double positionalScore,
            double combinedScore,
            int foregroundCount,
            int backgroundCount
    ) {}

    /**
     * Extracts characteristic terms using default fields, {@link #DEFAULT_TOP_N}, and
     * {@link #DEFAULT_POSITIONAL_WINDOW}.
     *
     * @param reader       open Lucene reader (must stay open during this call)
     * @param rankedDocIds Lucene internal doc IDs in descending relevance order
     *                     (rank 0 = best match)
     */
    public List<WormholeTerm> extract(IndexReader reader, List<Integer> rankedDocIds)
            throws IOException {
        return extract(reader, rankedDocIds, DEFAULT_FIELDS, DEFAULT_TOP_N, DEFAULT_POSITIONAL_WINDOW);
    }

    /**
     * Extracts characteristic terms with full parameter control.
     *
     * @param reader            open Lucene reader
     * @param rankedDocIds      doc IDs in rank order (rank 0 = best match)
     * @param fields            stored text fields to tokenise for term collection
     * @param topN              maximum number of terms to return
     * @param positionalWindow  only docs at rank &lt; positionalWindow contribute
     *                          positional weight
     * @return terms sorted by {@link WormholeTerm#combinedScore()} descending
     */
    public List<WormholeTerm> extract(
            IndexReader reader,
            List<Integer> rankedDocIds,
            String[] fields,
            int topN,
            int positionalWindow) throws IOException {

        if (rankedDocIds.isEmpty()) return List.of();

        int foregroundSize = rankedDocIds.size();
        int backgroundSize = reader.numDocs();
        if (backgroundSize == 0) return List.of();

        // term → list of ranks (0-indexed) of foreground docs that contain it
        Map<String, List<Integer>> termToRanks = new LinkedHashMap<>();

        try (Analyzer analyzer = new StandardAnalyzer(CODE_STOP_WORDS)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            StoredFields sf = searcher.storedFields();

            for (int rank = 0; rank < foregroundSize; rank++) {
                int docId = rankedDocIds.get(rank);
                Document doc = sf.document(docId);

                // Collect unique tokens from all target fields for this document
                Set<String> docTerms = new HashSet<>();
                for (String field : fields) {
                    for (String value : doc.getValues(field)) {
                        if (value.isBlank()) continue;
                        try (TokenStream ts = analyzer.tokenStream(field, value)) {
                            CharTermAttribute charAttr = ts.addAttribute(CharTermAttribute.class);
                            ts.reset();
                            while (ts.incrementToken()) {
                                String t = charAttr.toString();
                                // Reject code-expression tokens: StandardTokenizer keeps
                                // "this.field" and "1.0" as single tokens (Unicode MidNumLet).
                                if (t.length() >= MIN_TERM_LENGTH && !t.contains("."))
                                    docTerms.add(t);
                            }
                            ts.end();
                        }
                    }
                }

                final int r = rank;
                docTerms.forEach(t -> termToRanks.computeIfAbsent(t, k -> new ArrayList<>()).add(r));
            }
        }

        // Score each candidate
        List<WormholeTerm> candidates = new ArrayList<>(termToRanks.size());
        for (var entry : termToRanks.entrySet()) {
            String term = entry.getKey();
            List<Integer> ranks = entry.getValue();
            int foregroundCount = ranks.size();

            // Max docFreq across fields: a document containing the term in multiple
            // fields should still count as one background document.  MAX approximates
            // "unique docs containing this term in at least one field" without the
            // cross-field double-counting that SUM would introduce.
            int backgroundCount = 0;
            for (String field : fields) {
                backgroundCount = Math.max(backgroundCount, reader.docFreq(new Term(field, term)));
            }

            double pFg = (double) foregroundCount / foregroundSize;
            double pBg = (double) Math.max(backgroundCount, 1) / backgroundSize;
            // PPMI: clamp log-ratio at 0 so terms equally or less frequent in the
            // foreground contribute no statistical signal (only positional weight).
            double statisticalScore = Math.max(0.0, Math.log(pFg / pBg));

            double positionalScore = 0.0;
            for (int pos : ranks) {
                if (pos < positionalWindow) {
                    positionalScore += 1.0 / Math.sqrt(pos + 1.0);
                }
            }

            candidates.add(new WormholeTerm(term, statisticalScore, positionalScore, 0.0,
                    foregroundCount, backgroundCount));
        }

        if (candidates.isEmpty()) return List.of();

        // Min-max normalise then combine
        double maxStat = candidates.stream().mapToDouble(WormholeTerm::statisticalScore).max().orElse(1.0);
        double maxPos  = candidates.stream().mapToDouble(WormholeTerm::positionalScore).max().orElse(1.0);

        return candidates.stream()
                .map(t -> {
                    double normStat = maxStat > 0 ? t.statisticalScore() / maxStat : 0.0;
                    double normPos  = maxPos  > 0 ? t.positionalScore()  / maxPos  : 0.0;
                    double combined = STAT_WEIGHT * normStat + POS_WEIGHT * normPos;
                    return new WormholeTerm(t.term(), t.statisticalScore(), t.positionalScore(),
                            combined, t.foregroundCount(), t.backgroundCount());
                })
                .sorted(Comparator.comparingDouble(WormholeTerm::combinedScore).reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }
}
