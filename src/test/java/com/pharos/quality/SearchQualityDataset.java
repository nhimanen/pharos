package com.pharos.quality;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides the labeled quality corpus and dataset used by
 * {@link SearchQualityBenchmarkTest} and {@link SearchAblationTest}.
 *
 * <p>The quality corpus is a purpose-built collection of 8 Java service classes
 * (~38 methods) covering authentication, caching, file storage, metrics, notifications,
 * order processing, search, and user management. The classes are designed to produce
 * realistic disambiguation scenarios.
 *
 * <p>Each {@link QueryCase} carries:
 * <ul>
 *   <li>a natural-language query phrased as a developer would type it
 *   <li>{@link IrMetrics.Judgment}s: graded relevance (0=irrelevant, 1=partial, 2=high) per method
 *   <li>a category: {@code name-lookup}, {@code semantic}, or {@code conceptual}
 *   <li>a rationale explaining the difficulty and what the query tests
 * </ul>
 */
public final class SearchQualityDataset {

    public static final String PROJECT = "quality-corpus";

    private SearchQualityDataset() {}

    // ── Dataset ───────────────────────────────────────────────────────────────

    /**
     * A labeled evaluation case: one query with graded relevance judgments.
     */
    public record QueryCase(
            String id,
            String query,
            List<IrMetrics.Judgment> judgments,
            String category,
            String rationale
    ) {
        @Override
        public String toString() { return id; }

        /** Convenience: builds a {@link IrMetrics.RelevanceMap} from this case's judgments. */
        public IrMetrics.RelevanceMap relevanceMap() {
            return new IrMetrics.RelevanceMap(judgments);
        }
    }

    /**
     * Returns the 20-query evaluation set spanning three difficulty tiers.
     *
     * <ul>
     *   <li>{@code name-lookup} (5 queries) — direct token match to method/class name; BM25 should excel
     *   <li>{@code semantic} (8 queries) — vocabulary gap between developer phrasing and code identifiers
     *   <li>{@code conceptual} (7 queries) — developer mental model differs from code structure
     * </ul>
     */
    public static List<QueryCase> queryCases() {
        return List.of(

            // ── name-lookup: direct token match ─────────────────────────────

            new QueryCase(
                "send-email",
                "send email notification to recipient",
                List.of(
                    IrMetrics.Judgment.high("NotificationDispatcher", "sendEmailNotification"),
                    IrMetrics.Judgment.partial("NotificationDispatcher", "broadcastToGroup")
                ),
                "name-lookup",
                "'sendEmailNotification' contains every query token; BM25 should rank it first"
            ),

            new QueryCase(
                "cache-get-or-compute",
                "get or compute a cached value with loader fallback",
                List.of(
                    IrMetrics.Judgment.high("CacheStore", "getOrCompute"),
                    IrMetrics.Judgment.partial("CacheStore", "getValue")
                ),
                "name-lookup",
                "'getOrCompute' directly matches 'get' and 'compute'; javadoc mentions 'cache-aside'"
            ),

            new QueryCase(
                "rank-results",
                "rank search results by relevance score",
                List.of(
                    IrMetrics.Judgment.high("SearchIndex", "rankByRelevance"),
                    IrMetrics.Judgment.partial("SearchIndex", "findByKeyword")
                ),
                "name-lookup",
                "'rankByRelevance' contains 'rank' and 'relevance'; straightforward name match"
            ),

            new QueryCase(
                "cancel-notification",
                "cancel a scheduled notification before it is sent",
                List.of(
                    IrMetrics.Judgment.high("NotificationDispatcher", "cancelScheduledNotification"),
                    IrMetrics.Judgment.partial("NotificationDispatcher", "scheduleNotification")
                ),
                "name-lookup",
                "'cancelScheduledNotification' is a direct name match for 'cancel scheduled'"
            ),

            new QueryCase(
                "refresh-token",
                "refresh an expired authentication token",
                List.of(
                    IrMetrics.Judgment.high("TokenManager", "refreshExpiredToken"),
                    IrMetrics.Judgment.partial("TokenManager", "validateToken")
                ),
                "name-lookup",
                "'refreshExpiredToken' matches all three key tokens: refresh, expired, token"
            ),

            // ── semantic: vocabulary gap between developer and code ──────────

            new QueryCase(
                "prevent-unauthorized-access",
                "prevent unauthorized access by checking auth credentials",
                List.of(
                    IrMetrics.Judgment.high("TokenManager", "validateToken"),
                    IrMetrics.Judgment.partial("TokenManager", "generateAccessToken"),
                    IrMetrics.Judgment.partial("UserAccountService", "verifyEmailAddress")
                ),
                "semantic",
                "Developer says 'check credentials'; code says 'validateToken' — vocabulary gap"
            ),

            new QueryCase(
                "measure-operation-time",
                "measure how long an operation takes in milliseconds",
                List.of(
                    IrMetrics.Judgment.high("MetricsRecorder", "recordLatency"),
                    IrMetrics.Judgment.partial("MetricsRecorder", "flushToBackend")
                ),
                "semantic",
                "'latency' is the technical term for 'how long something takes'; javadoc says 'milliseconds'"
            ),

            new QueryCase(
                "clear-cache",
                "remove all entries from the in-memory cache",
                List.of(
                    IrMetrics.Judgment.high("CacheStore", "clearAll"),
                    IrMetrics.Judgment.partial("CacheStore", "evictByPrefix")
                ),
                "semantic",
                "Developer says 'remove all entries'; code says 'clearAll' — near-synonym"
            ),

            new QueryCase(
                "send-to-group",
                "send a message to all members of a subscriber group",
                List.of(
                    IrMetrics.Judgment.high("NotificationDispatcher", "broadcastToGroup"),
                    IrMetrics.Judgment.partial("NotificationDispatcher", "sendEmailNotification")
                ),
                "semantic",
                "'broadcast' captures 'send to all'; 'group' appears in both query and method name"
            ),

            new QueryCase(
                "suspend-user",
                "suspend a user account and invalidate active sessions",
                List.of(
                    IrMetrics.Judgment.high("UserAccountService", "deactivateAccount"),
                    IrMetrics.Judgment.partial("UserAccountService", "createAccount")
                ),
                "semantic",
                "'suspend' → 'deactivate': the developer's HR term differs from the code verb"
            ),

            new QueryCase(
                "download-file-bytes",
                "retrieve the binary contents of a stored file",
                List.of(
                    IrMetrics.Judgment.high("FileStorageService", "downloadFile"),
                    IrMetrics.Judgment.partial("FileStorageService", "getFileMetadata")
                ),
                "semantic",
                "'retrieve binary contents' maps to 'downloadFile'; javadoc says 'raw file bytes'"
            ),

            new QueryCase(
                "check-token-valid",
                "check whether an auth credential is still valid and not expired",
                List.of(
                    IrMetrics.Judgment.high("TokenManager", "validateToken"),
                    IrMetrics.Judgment.partial("TokenManager", "refreshExpiredToken")
                ),
                "semantic",
                "'validateToken' javadoc says 'unexpired and signature verifies'; matches query"
            ),

            new QueryCase(
                "download-or-list-files",
                "list files stored under a directory path",
                List.of(
                    IrMetrics.Judgment.high("FileStorageService", "listFiles"),
                    IrMetrics.Judgment.partial("FileStorageService", "uploadFile")
                ),
                "semantic",
                "'listFiles' matches 'list files' directly; directory context narrows to this method"
            ),

            // ── conceptual: developer mental model ≠ code vocabulary ─────────

            new QueryCase(
                "apply-promo-discount",
                "apply a coupon or promotional voucher to reduce order total",
                List.of(
                    IrMetrics.Judgment.high("OrderProcessor", "applyPromoCode"),
                    IrMetrics.Judgment.partial("OrderProcessor", "calculateTotal"),
                    IrMetrics.Judgment.partial("OrderProcessor", "submitOrder")
                ),
                "conceptual",
                "'coupon/voucher' → 'promoCode'; marketing vocabulary vs. code term"
            ),

            new QueryCase(
                "verify-identity",
                "verify user identity before granting system access",
                List.of(
                    IrMetrics.Judgment.high("TokenManager", "validateToken"),
                    IrMetrics.Judgment.partial("UserAccountService", "verifyEmailAddress"),
                    IrMetrics.Judgment.partial("TokenManager", "generateAccessToken")
                ),
                "conceptual",
                "No single token maps directly; requires understanding of auth/access patterns"
            ),

            new QueryCase(
                "refund-customer",
                "return money to customer after order cancellation",
                List.of(
                    IrMetrics.Judgment.high("OrderProcessor", "refundPayment"),
                    IrMetrics.Judgment.partial("OrderProcessor", "cancelOrder")
                ),
                "conceptual",
                "'return money' → 'refundPayment'; developer intent expressed in customer terms"
            ),

            new QueryCase(
                "lazy-load-cache",
                "lazy loading pattern with automatic caching on first access",
                List.of(
                    IrMetrics.Judgment.high("CacheStore", "getOrCompute"),
                    IrMetrics.Judgment.partial("CacheStore", "getValue")
                ),
                "conceptual",
                "'lazy loading' is a pattern name; 'getOrCompute' javadoc says 'cache-aside'"
            ),

            new QueryCase(
                "export-telemetry",
                "export buffered telemetry and performance data to the monitoring backend",
                List.of(
                    IrMetrics.Judgment.high("MetricsRecorder", "flushToBackend"),
                    IrMetrics.Judgment.partial("MetricsRecorder", "recordLatency"),
                    IrMetrics.Judgment.partial("MetricsRecorder", "incrementCounter")
                ),
                "conceptual",
                "'export telemetry' → 'flushToBackend'; monitoring/export not in sibling method names"
            ),

            new QueryCase(
                "change-password",
                "change a user's login password or reset forgotten credentials",
                List.of(
                    IrMetrics.Judgment.high("UserAccountService", "resetPassword"),
                    IrMetrics.Judgment.partial("UserAccountService", "updateProfile")
                ),
                "conceptual",
                "'password' appears in both query and method name but intent (reset vs. change) differs"
            ),

            new QueryCase(
                "file-metadata",
                "get the size and creation timestamp of a stored file",
                List.of(
                    IrMetrics.Judgment.high("FileStorageService", "getFileMetadata"),
                    IrMetrics.Judgment.partial("FileStorageService", "downloadFile")
                ),
                "conceptual",
                "'size and creation timestamp' matches the FileMetadata record fields, not the method name"
            )
        );
    }

    // ── Corpus indexing ───────────────────────────────────────────────────────

    /**
     * Parses the quality corpus and indexes it into an in-memory Lucene directory.
     * All methods are indexed with {@code inDegree = 0} (no graph boost).
     *
     * <p>The returned {@link IndexedCorpus} must be closed when the test completes.
     */
    public static IndexedCorpus buildAndIndex() throws Exception {
        return buildAndIndexWithInDegrees(Map.of());
    }

    /**
     * Same as {@link #buildAndIndex} but allows injecting non-zero in-degree values
     * for specific methods. Used by ablation tests to measure graph boost contribution.
     *
     * @param inDegreeOverrides map from {@code "ClassName#methodName"} to the desired inDegree
     */
    public static IndexedCorpus buildAndIndexWithInDegrees(
            Map<String, Integer> inDegreeOverrides) throws Exception {

        Path projectRoot = resourcePath("test-projects/quality-corpus");
        JavaCodeParser parser = new JavaCodeParser();
        ParsedProject project = parser.parseProject(projectRoot, PROJECT);

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());

        Map<String, List<ParsedMethod>> methodsByClass = project.allMethods().stream()
                .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));

        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (ParsedFile file : project.files()) {
                for (ParsedMethod method : file.methods()) {
                    String key = method.className() + "#" + method.methodName();
                    int inDeg = inDegreeOverrides.getOrDefault(key, 0);
                    writer.addDocument(DocumentMapper.toDocument(method, null, inDeg, List.of()));
                }
                for (ParsedClass cls : file.classes()) {
                    List<ParsedMethod> methods = methodsByClass.getOrDefault(
                            cls.qualifiedClassName(), List.of());
                    String synthesized = buildSynthesized(methods);
                    writer.addDocument(DocumentMapper.toClassDocument(cls, synthesized, (float[]) null));
                }
            }
            writer.commit();
        }

        DirectoryReader reader = DirectoryReader.open(dir);
        return new IndexedCorpus(dir, reader, PROJECT, project);
    }

    // ── IndexedCorpus ─────────────────────────────────────────────────────────

    /**
     * A parsed-and-indexed quality corpus backed by an in-memory Lucene store.
     * Must be closed after tests complete.
     */
    public record IndexedCorpus(
            ByteBuffersDirectory directory,
            DirectoryReader reader,
            String projectName,
            ParsedProject parsedProject
    ) implements AutoCloseable {

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                directory.close();
            }
        }

        public int methodCount() {
            return parsedProject.allMethods().size();
        }

        public int classCount() {
            return parsedProject.allClasses().size();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String buildSynthesized(List<ParsedMethod> methods) {
        StringBuilder sb = new StringBuilder();
        for (ParsedMethod m : methods) {
            sb.append(m.signature()).append("\n");
            if (m.javadoc() != null && !m.javadoc().isBlank()) {
                sb.append("  // ").append(m.javadoc().replaceAll("\\s+", " ").trim()).append("\n");
            }
        }
        return sb.toString();
    }

    private static Path resourcePath(String path) throws URISyntaxException {
        URL url = SearchQualityDataset.class.getClassLoader().getResource(path);
        if (url == null) throw new IllegalStateException("Test resource not found: " + path);
        return Paths.get(url.toURI());
    }
}
