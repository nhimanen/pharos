package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exploratory tests for {@link WormholeTermExtractor}.
 *
 * Each test prints a term table to stdout so you can observe the extractor's output
 * for different corpus/foreground configurations. Run with:
 *
 *   mvn test -Dtest=WormholeTermExtractorTest -pl . 2>&1 | grep -A 200 "WORMHOLE"
 */
class WormholeTermExtractorTest {

    private ByteBuffersDirectory dir;
    private DirectoryReader reader;
    private final WormholeTermExtractor extractor = new WormholeTermExtractor();

    @BeforeEach
    void setUp() {
        dir = new ByteBuffersDirectory();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (reader != null) reader.close();
        dir.close();
    }

    // ── scenario 1: basic cluster extraction ─────────────────────────────────

    /**
     * Corpus: 5 cache-cluster docs + 5 retry-cluster docs.
     * Foreground: only the cache docs.
     * Expected: cache-specific terms (evict, cache, stale, ttl) dominate; retry terms absent.
     */
    @Test
    void clusterExtraction_cacheTermsDominateOverRetryTerms(TestInfo info) throws IOException {
        // Cache cluster
        index("proj", "CacheStore", "evict",
                "Evicts stale entries from the cache when the ttl expires.",
                "cache evict stale entry");
        index("proj", "CacheStore", "expire",
                "Marks a cache entry as expired based on its ttl configuration.",
                "cache expire ttl entry");
        index("proj", "CacheStore", "put",
                "Inserts an entry into the bounded cache, evicting lru entries if full.",
                "cache put bounded evict lru");
        index("proj", "CacheStore", "invalidate",
                "Invalidates all cached entries matching the given key prefix.",
                "cache invalidate stale key");
        index("proj", "CacheStore", "get",
                "Returns the cached entry or null if expired or evicted.",
                "cache get stale evict");

        // Retry cluster — deliberately different vocabulary
        index("proj", "RetryPolicy", "execute",
                "Executes the supplied action retrying on transient failures with backoff.",
                "retry attempt backoff execute");
        index("proj", "RetryPolicy", "shouldRetry",
                "Determines whether a failed attempt qualifies for another retry.",
                "retry attempt qualify failure");
        index("proj", "BackoffStrategy", "delay",
                "Computes the next backoff delay in milliseconds using exponential jitter.",
                "backoff delay jitter exponential");
        index("proj", "BackoffStrategy", "nextDelay",
                "Returns the next delay interval applying decorrelated jitter.",
                "backoff delay jitter interval");
        index("proj", "CircuitBreaker", "call",
                "Delegates the call through the circuit breaker checking failure thresholds.",
                "circuit breaker threshold failure");

        openReader();

        // Foreground = cache cluster (5 docs), ordered by simulated vector rank
        List<Integer> foreground = docIdsFor(
                "proj:com.example.CacheStore#evict()",
                "proj:com.example.CacheStore#expire()",
                "proj:com.example.CacheStore#put()",
                "proj:com.example.CacheStore#invalidate()",
                "proj:com.example.CacheStore#get()"
        );

        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, foreground, WormholeTermExtractor.DEFAULT_FIELDS, 12, 5);

        printTable(info.getDisplayName(), "cache-eviction foreground, 10-doc corpus", foreground, terms);

        // Cache-specific terms must appear in the results
        List<String> topTerms = terms.stream().map(WormholeTermExtractor.WormholeTerm::term).toList();
        assertThat(topTerms).contains("cache");
        assertThat(topTerms).contains("evict");
        assertThat(topTerms).contains("stale");

        // Retry-cluster terms must not appear in the top results at all
        // (they have 0 foreground count → excluded from candidates)
        assertThat(topTerms).doesNotContain("retry", "backoff", "circuit", "jitter");
    }

    // ── scenario 2: disambiguation ────────────────────────────────────────────

    /**
     * Classic wormhole disambiguation: corpus contains two "Java" clusters
     * (programming vs. coffee).  Foregrounding only the programming cluster should
     * surface JVM / compile vocabulary, not beans / roast vocabulary.
     */
    @Test
    void disambiguation_programmingClusterForegrounded(TestInfo info) throws IOException {
        // Java-programming cluster
        index("java", "JvmRuntime", "loadClass",
                "Loads and links a class from the classpath into the jvm runtime.",
                "jvm classpath class load runtime");
        index("java", "Compiler", "compile",
                "Compiles java source files to jvm bytecode using the javac toolchain.",
                "java compile bytecode javac jvm");
        index("java", "ClassLoader", "findClass",
                "Locates a class definition within the classpath delegation chain.",
                "classpath class loader delegation jvm");
        index("java", "GarbageCollector", "collect",
                "Triggers a jvm garbage collection cycle reclaiming heap memory.",
                "jvm garbage collect heap memory");
        index("java", "JitCompiler", "optimize",
                "Performs jit optimisation of hot bytecode methods in the jvm.",
                "jvm jit bytecode optimize hotspot");

        // Java-coffee cluster — completely different vocabulary
        index("java", "BeanRoaster", "roast",
                "Roasts raw green coffee beans to the desired roast level.",
                "coffee roast beans green level");
        index("java", "EspressoMachine", "brew",
                "Brews a single espresso shot from freshly ground java beans.",
                "espresso brew grind beans java");
        index("java", "GrindController", "grind",
                "Grinds coffee beans to the configured coarseness for pour-over brewing.",
                "coffee grind beans coarseness brew");
        index("java", "BrewProfile", "steep",
                "Steeps ground coffee in hot water for the prescribed brew duration.",
                "coffee steep brew water duration");
        index("java", "FlavorExtractor", "extract",
                "Extracts flavor compounds from roasted beans during the brew process.",
                "coffee flavor extract roast beans brew");

        openReader();

        // Foreground = programming cluster (rank 0 = JVM runtime load, most relevant)
        List<Integer> foreground = docIdsFor(
                "java:com.example.JvmRuntime#loadClass()",
                "java:com.example.Compiler#compile()",
                "java:com.example.ClassLoader#findClass()",
                "java:com.example.GarbageCollector#collect()",
                "java:com.example.JitCompiler#optimize()"
        );

        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, foreground, WormholeTermExtractor.DEFAULT_FIELDS, 12, 5);

        printTable(info.getDisplayName(), "java-programming foreground, mixed java corpus", foreground, terms);

        List<String> topTerms = terms.stream().map(WormholeTermExtractor.WormholeTerm::term).toList();

        // Programming-cluster vocabulary must appear
        assertThat(topTerms).contains("jvm");
        // Coffee terms must be excluded (0 foreground count)
        assertThat(topTerms).doesNotContain("beans", "roast", "espresso", "grind", "brew");
    }

    // ── scenario 3: positional weighting breaks ties ──────────────────────────

    /**
     * Six foreground docs, all in one corpus.
     * Docs at rank 0-2 contain "lrueviction"; docs at rank 3-5 contain "fifobuffer".
     * Both terms have equal foreground count (3) → equal statistical score.
     * The positional window is 3, so "lrueviction" accumulates position weight while
     * "fifobuffer" does not.  Combined score must favour "lrueviction".
     */
    @Test
    void positionalWeighting_topRankedTermWinsStatisticalTie(TestInfo info) throws IOException {
        // Docs at rank 0-2: distinctive term "lrueviction" (not a real word → zero background)
        index("pos", "LruCache", "evict",
                "Applies lrueviction policy removing the least recently used entry.",
                "lrueviction cache least recently used");
        index("pos", "LruPolicy", "select",
                "Selects the lrueviction candidate from the priority queue.",
                "lrueviction priority queue selection");
        index("pos", "LruTracker", "track",
                "Tracks access order for lrueviction across cache partitions.",
                "lrueviction access partition track");

        // Docs at rank 3-5: distinctive term "fifobuffer" (not a real word → zero background)
        index("pos", "FifoQueue", "enqueue",
                "Enqueues an element using fifobuffer ordering semantics.",
                "fifobuffer queue enqueue order");
        index("pos", "FifoPolicy", "select",
                "Selects the next eviction victim using fifobuffer sequence.",
                "fifobuffer eviction sequence victim");
        index("pos", "FifoTracker", "flush",
                "Flushes the fifobuffer segment and resets sequence counters.",
                "fifobuffer segment flush counter");

        openReader();

        // Ordered: LRU docs at ranks 0-2, FIFO docs at ranks 3-5
        List<Integer> foreground = docIdsFor(
                "pos:com.example.LruCache#evict()",
                "pos:com.example.LruPolicy#select()",
                "pos:com.example.LruTracker#track()",
                "pos:com.example.FifoQueue#enqueue()",
                "pos:com.example.FifoPolicy#select()",
                "pos:com.example.FifoTracker#flush()"
        );

        // Positional window = 3 → only ranks 0-2 contribute position weight.
        // Use large topN so both distinctive terms survive the limit.
        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, foreground,
                        WormholeTermExtractor.DEFAULT_FIELDS, 50, 3);

        printTable(info.getDisplayName(),
                "lrueviction@rank0-2 vs fifobuffer@rank3-5, window=3", foreground, terms);

        WormholeTermExtractor.WormholeTerm lru  = findTerm(terms, "lrueviction");
        WormholeTermExtractor.WormholeTerm fifo = findTerm(terms, "fifobuffer");

        assertThat(lru).as("lrueviction must appear in results").isNotNull();
        assertThat(fifo).as("fifobuffer must appear in results").isNotNull();

        // Both terms appear in exactly the same fraction of fg docs as bg docs →
        // PPMI = max(0, log(1.0)) = 0 for both.  Zero statistical signal; tie broken by position.
        assertThat(lru.statisticalScore())
                .as("PPMI must be equal (fg/bg ratio == 1 for both terms)")
                .isEqualTo(fifo.statisticalScore())
                .isEqualTo(0.0);

        // But combined score must favour lru thanks to positional weight
        assertThat(lru.combinedScore())
                .as("lrueviction (rank 0-2) must outscore fifobuffer (rank 3-5)")
                .isGreaterThan(fifo.combinedScore());

        // lrueviction must have non-zero positional score, fifobuffer must have zero
        assertThat(lru.positionalScore()).isGreaterThan(0.0);
        assertThat(fifo.positionalScore()).isEqualTo(0.0);
    }

    // ── scenario 4: realistic code-search simulation ──────────────────────────

    /**
     * A richer, more realistic corpus simulating a backend service codebase.
     * The foreground simulates what HNSW would return for the query
     * "cache eviction policy" — a mix of highly relevant + tangentially related docs.
     *
     * This test is primarily observational: it prints the full term table so you
     * can evaluate whether the extracted terms would make a good secondary BM25 query.
     */
    @Test
    void realisticCorpus_cacheEvictionQueryForeground(TestInfo info) throws IOException {
        // ── cache / eviction domain ───────────────────────────────────────────
        index("app", "EvictionPolicy", "evict",
                "Evicts entries from the cache according to the configured policy (lru, lfu, ttl).",
                "cache evict policy lru lfu ttl entry");
        index("app", "LruEvictionPolicy", "selectVictim",
                "Selects the least recently used cache entry as the eviction victim.",
                "cache lru evict victim least recently used");
        index("app", "TtlEvictionPolicy", "isExpired",
                "Returns true if the cache entry has outlived its time-to-live.",
                "cache ttl expire entry live");
        index("app", "BoundedCache", "put",
                "Adds an entry to the bounded cache, triggering eviction when capacity is reached.",
                "cache put bounded capacity evict entry");
        index("app", "CacheStats", "recordEviction",
                "Increments the eviction counter and records eviction latency metrics.",
                "cache evict stats counter metric");

        // ── tangentially related: expiry / invalidation ───────────────────────
        index("app", "SessionStore", "invalidate",
                "Invalidates an expired session entry and removes it from the store.",
                "session expire invalidate store entry");
        index("app", "TokenCache", "refresh",
                "Refreshes a cached access token before it expires.",
                "token cache expire refresh");

        // ── unrelated: retry / resilience ─────────────────────────────────────
        index("app", "RetryHandler", "execute",
                "Executes the action with retry semantics on transient failures.",
                "retry execute failure transient");
        index("app", "CircuitBreaker", "guard",
                "Guards a downstream call by opening the circuit on repeated failures.",
                "circuit breaker guard failure open");
        index("app", "BackoffStrategy", "nextDelay",
                "Computes the next exponential backoff delay between retry attempts.",
                "backoff delay retry exponential attempt");

        // ── unrelated: persistence / storage ──────────────────────────────────
        index("app", "Repository", "save",
                "Persists the entity to the database in a single transaction.",
                "persist entity database transaction save");
        index("app", "QueryBuilder", "buildQuery",
                "Builds a parameterised SQL query for the given filter criteria.",
                "sql query build filter criteria parameter");

        openReader();

        // Foreground = top-7 HNSW hits for "cache eviction policy"
        // Rank 0-4: highly relevant cache/eviction docs
        // Rank 5-6: tangentially related (expiry/token) — lower vector similarity
        List<Integer> foreground = docIdsFor(
                "app:com.example.EvictionPolicy#evict()",
                "app:com.example.LruEvictionPolicy#selectVictim()",
                "app:com.example.TtlEvictionPolicy#isExpired()",
                "app:com.example.BoundedCache#put()",
                "app:com.example.CacheStats#recordEviction()",
                "app:com.example.SessionStore#invalidate()",
                "app:com.example.TokenCache#refresh()"
        );

        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, foreground, WormholeTermExtractor.DEFAULT_FIELDS, 12, 5);

        printTable(info.getDisplayName(),
                "7-doc cache foreground, 12-doc mixed corpus", foreground, terms);

        List<String> topTerms = terms.stream().map(WormholeTermExtractor.WormholeTerm::term).toList();

        // Core cache/eviction vocabulary must surface
        assertThat(topTerms).contains("cache");
        assertThat(topTerms).contains("evict");

        // Pure retry/persistence vocabulary must be absent (0 foreground count)
        assertThat(topTerms).doesNotContain("retry", "backoff", "persist", "sql", "transaction");
    }

    // ── scenario 5: edge cases ────────────────────────────────────────────────

    @Test
    void emptyForeground_returnsEmptyList() throws IOException {
        index("proj", "Foo", "bar", "Does something.", "something");
        openReader();

        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, List.of());

        assertThat(terms).isEmpty();
    }

    @Test
    void singleDocument_returnsTermsFromThatDocument(TestInfo info) throws IOException {
        index("proj", "TokenValidator", "validate",
                "Validates the JWT token signature and expiry against the public key.",
                "token jwt validate signature expiry key");
        index("proj", "Unrelated", "unrelated",
                "Completely unrelated method about database transactions.",
                "database transaction commit rollback");
        openReader();

        List<Integer> foreground = docIdsFor("proj:com.example.TokenValidator#validate()");
        // topN=15: single-doc corpus produces ~12 unique terms all tied at combined=1.0;
        // a limit of 10 would cut some arbitrarily.
        List<WormholeTermExtractor.WormholeTerm> terms =
                extractor.extract(reader, foreground, WormholeTermExtractor.DEFAULT_FIELDS, 15, 3);

        printTable(info.getDisplayName(), "single-doc foreground", foreground, terms);

        List<String> topTerms = terms.stream().map(WormholeTermExtractor.WormholeTerm::term).toList();
        assertThat(topTerms).contains("token");
        assertThat(topTerms).contains("validate");
        assertThat(topTerms).doesNotContain("database", "transaction", "commit");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Indexes a single method document into the in-memory directory. */
    private void index(String project, String className, String methodName,
                       String javadoc, String body) throws IOException {
        String id = project + ":com.example." + className + "#" + methodName + "()";
        ParsedMethod method = new ParsedMethod(
                id, project, "com.example", className, "com.example." + className,
                methodName, "public void " + methodName + "()", "void",
                List.of(), List.of(),
                body, javadoc, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/" + className + ".java", 1, 10
        );
        Document doc = DocumentMapper.toDocument(method, null, 0, List.of());

        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            writer.addDocument(doc);
            writer.commit();
        }
    }

    private void openReader() throws IOException {
        reader = DirectoryReader.open(dir);
    }

    /** Resolves a stored document id (F_ID) to a Lucene internal doc ID. */
    private int docIdFor(String storedId) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_ID, storedId)), 1);
        assertThat(hits.scoreDocs).as("document not found: %s", storedId).isNotEmpty();
        return hits.scoreDocs[0].doc;
    }

    private List<Integer> docIdsFor(String... storedIds) throws IOException {
        List<Integer> ids = new ArrayList<>();
        for (String id : storedIds) ids.add(docIdFor(id));
        return ids;
    }

    private WormholeTermExtractor.WormholeTerm findTerm(
            List<WormholeTermExtractor.WormholeTerm> terms, String name) {
        return terms.stream()
                .filter(t -> t.term().equals(name))
                .findFirst()
                .orElse(null);
    }

    /** Prints a human-readable term table to stdout for exploratory inspection. */
    private void printTable(String testName, String scenario, List<Integer> foreground,
                            List<WormholeTermExtractor.WormholeTerm> terms) {
        System.out.printf("%n══ WORMHOLE: %s ══%n", testName.replaceAll("\\(.*\\)", ""));
        System.out.printf("   scenario : %s%n", scenario);
        System.out.printf("   fg docs  : %d  bg docs: %d%n", foreground.size(),
                reader != null ? reader.numDocs() : -1);
        System.out.printf("   %-20s  %6s  %6s  %6s  %4s/%4s%n",
                "term", "stat", "pos", "comb", "fg", "bg");
        System.out.println("   " + "─".repeat(64));
        for (var t : terms) {
            System.out.printf("   %-20s  %6.3f  %6.3f  %6.3f  %4d/%4d%n",
                    t.term(), t.statisticalScore(), t.positionalScore(),
                    t.combinedScore(), t.foregroundCount(), t.backgroundCount());
        }
        System.out.println();
    }
}
