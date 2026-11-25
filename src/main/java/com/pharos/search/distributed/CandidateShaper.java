package com.pharos.search.distributed;

import com.pharos.search.SearchResult;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The second pass of retrieval, split off the raw scores so a coordinator can apply it to candidates
 * merged from several shards: log-normalised in-degree boost (normalised by the maximum over
 * <b>the given list</b>), source-path penalty, doc-type bonus, class-type bonus (by query intent) and
 * access multiplier, then a stable sort by the boosted score.
 *
 * <p>The arithmetic is exactly the former {@code KeywordSearchStrategy.toResults} second pass, so the
 * local path's scores are bit-for-bit unchanged.
 */
public final class CandidateShaper {

    /**
     * Weight applied to the graph-derived boost on top of the raw score.
     * Final score = raw * (1 + GRAPH_BOOST_WEIGHT * log(1+inDeg)/log(1+maxInDeg))
     * At 0.3, a method called by 100 others gets ~30% score lift over a method never
     * called. Keeps relevance dominant while surfacing hub methods.
     */
    public static final float GRAPH_BOOST_WEIGHT = 0.3f;

    private CandidateShaper() {}

    /**
     * @param type   the {@link SearchResult#searchType()} of the shaped results
     * @param intent the detected query intent (e.g. {@code "INTERFACE"}); null disables classType boosting
     */
    /** The trace that records the raw scores shaped on this thread (P6-R20); set around a search. */
    public static final ThreadLocal<com.pharos.search.SearchTrace> RAW_SCORES = new ThreadLocal<>();

    public static List<SearchResult> shape(List<RawCandidate> raw, String type, String intent) {
        if (raw.isEmpty()) return List.of();
        com.pharos.search.SearchTrace sink = RAW_SCORES.get();
        if (sink != null) {
            for (RawCandidate c : raw) if (c.result() != null) sink.rawScore(type, c.result().id(), c.rawScore());
        }

        int maxInDegree = 1; // avoid division by zero
        for (RawCandidate c : raw) {
            if (c.inDegree() > maxInDegree) maxInDegree = c.inDegree();
        }

        final int maxDeg = maxInDegree;
        return raw.stream()
                .map(c -> {
                    float normBoost  = (float) (Math.log1p(c.inDegree()) / Math.log1p(maxDeg));
                    float penalty    = sourcePathPenalty(c.filePath());
                    float typeBonus  = docTypeBonus(c.docType(), c.filePath());
                    float ctBonus    = classTypeBonus(c.classType(), intent);
                    float accessMult = accessMultiplier(c.access());
                    float boostedScore = c.rawScore()
                            * (1.0f + GRAPH_BOOST_WEIGHT * normBoost)
                            * penalty * typeBonus * ctBonus * accessMult;
                    return c.result().withScore(boostedScore, type);
                })
                .sorted(Comparator.comparingDouble(SearchResult::score).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Bonus multiplier by document type so the ranking order is:
     *   class &gt; method/chunk
     * Applied on top of BM25 so a class whose name matches the query always
     * ranks above individual methods in that class for the same BM25 score.
     */
    public static float docTypeBonus(String docType, String filePath) {
        if (!"class".equals(docType)) return 1.0f;
        if (filePath != null) {
            String p = filePath.replace('\\', '/');
            if (p.contains("/src/test/") || p.contains("/test/")) return 1.0f; // test classes don't get class bonus
        }
        return 1.8f;
    }

    /**
     * Multiplier by {@link com.pharos.indexer.DocumentMapper#F_CLASS_TYPE} and query intent.
     *
     * <p>Intent → classType boost table:
     * <pre>
     *   INTERFACE:    interface 3.0×, abstract 1.8×, class 0.7×
     *   ABSTRACT:     abstract 3.0×, interface 1.8×, class 0.7×
     *   ENUM:         enum 3.0×, class 0.8×, other 0.6×
     *   RECORD:       record 3.0×, class 0.8×, other 0.6×
     *   ANNOTATION:   annotation 3.0×, interface 0.9×, class 0.7×
     *   IMPLEMENTATION: class 2.5×, enum/record 1.5×, abstract 0.7×, interface 0.4×
     * </pre>
     *
     * <p>Returns 1.0 for all other intents or when {@code classType} is null
     * (method documents do not have a classType field).
     */
    public static float classTypeBonus(String classType, String intent) {
        if (classType == null || intent == null) return 1.0f;
        return switch (intent) {
            // Java interface: pure interfaces get maximum boost; abstract classes still relevant
            case "INTERFACE" -> switch (classType) {
                case "interface"   -> 3.0f;
                case "abstract"    -> 1.8f;
                case "class"       -> 0.7f;
                default            -> 1.0f;
            };
            // Abstract class: abstract types get maximum boost; interfaces also conceptually valid
            case "ABSTRACT" -> switch (classType) {
                case "abstract"    -> 3.0f;
                case "interface"   -> 1.8f;
                case "class"       -> 0.7f;
                default            -> 1.0f;
            };
            // Enum: strongly filter to enum types; other class types unlikely to be relevant
            case "ENUM" -> switch (classType) {
                case "enum"        -> 3.0f;
                case "class"       -> 0.8f;
                default            -> 0.6f;
            };
            // Record: strongly filter to record types
            case "RECORD" -> switch (classType) {
                case "record"      -> 3.0f;
                case "class"       -> 0.8f;
                default            -> 0.6f;
            };
            // Annotation: strongly filter to annotation types
            case "ANNOTATION" -> switch (classType) {
                case "annotation"  -> 3.0f;
                case "interface"   -> 0.9f;  // annotations are @interface internally
                case "class"       -> 0.7f;
                default            -> 0.6f;
            };
            // Concrete implementation: boost non-abstract classes
            case "IMPLEMENTATION" -> switch (classType) {
                case "class"       -> 2.5f;
                case "enum",
                     "record"      -> 1.5f;  // enums/records are concrete, still valid
                case "abstract"    -> 0.7f;
                case "interface"   -> 0.4f;
                default            -> 1.0f;
            };
            default -> 1.0f;
        };
    }

    /**
     * Multiplier by access modifier so the ranking order is:
     *   public &gt; protected &gt; package-private &gt; private
     * Private implementation details rank last unless the BM25 score is
     * substantially higher (i.e. the query is very specific to that method).
     */
    public static float accessMultiplier(String access) {
        if (access == null || access.isBlank()) return 0.75f; // package-private
        return switch (access.toLowerCase()) {
            case "public"    -> 1.00f;
            case "protected" -> 0.85f;
            case "private"   -> 0.50f;
            default          -> 0.75f; // package-private or unrecognised
        };
    }

    /**
     * Penalizes test, benchmark, and non-source files so production classes
     * rank above ancillary code that matches the same tokens.
     *
     * Multipliers (applied after BM25 + graph boost):
     *   /benchmark/, /jmh/      → 0.25  (JMH benchmarks — heavy false-positive source)
     *   /src/test/, /test/      → 0.30  (tests are rarely the target)
     *   /.github/               → 0.10
     *   .md / .txt / .sh / .yml → 0.10  (documentation and scripts)
     *   otherwise               → 1.00
     */
    public static float sourcePathPenalty(String filePath) {
        if (filePath == null) return 1.0f;
        String p = filePath.replace('\\', '/');
        if (p.contains("/benchmark/")) return 0.25f;
        if (p.contains("/jmh/"))        return 0.25f;
        if (p.contains("/src/test/"))   return 0.30f; // stronger — tests are rarely the target
        if (p.contains("/test/"))       return 0.30f;
        if (p.contains("/.github/"))    return 0.10f;
        if (p.endsWith(".md") || p.endsWith(".txt") || p.endsWith(".sh") || p.endsWith(".yml")) return 0.10f;
        return 1.0f;
    }
}
