package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class KeywordSearchStrategyTest {

    @TempDir
    Path tempDir;

    private ByteBuffersDirectory dir;
    private DirectoryReader reader;
    private KeywordSearchStrategy strategy;

    @BeforeEach
    void setUp() throws IOException {
        dir = new ByteBuffersDirectory();
        // Use a tempDir-relative path so no synonyms file is loaded — prevents the
        // production ~/.pharos/synonyms.txt from expanding query terms and making
        // scoring tests non-deterministic.
        strategy = new KeywordSearchStrategy(tempDir.resolve("synonyms.txt"));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (reader != null) reader.close();
        dir.close();
    }

    // --- basic retrieval ---

    @Test
    void search_findsByMethodName() throws IOException {
        writeMethod("proj", "Calculator", "add", "public int add(int a, int b)", "return a + b;", null, 0);
        writeMethod("proj", "Calculator", "subtract", "public int subtract(int a, int b)", "return a - b;", null, 0);
        openReader();

        List<SearchResult> results = search("add", "proj");

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).methodName()).isEqualTo("add");
    }

    @Test
    void search_findsByJavadoc() throws IOException {
        writeMethod("proj", "MathUtils", "fibonacci",
                "public int fibonacci(int n)",
                "if (n <= 1) return n; return fibonacci(n-1) + fibonacci(n-2);",
                "Computes the Fibonacci sequence value at position n.", 0);
        openReader();

        List<SearchResult> results = search("fibonacci sequence", "proj");

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).methodName()).isEqualTo("fibonacci");
    }

    @Test
    void search_findsByBodyContent() throws IOException {
        writeMethod("proj", "StringUtils", "reverse",
                "public String reverse(String s)",
                "StringBuilder sb = new StringBuilder(s); return sb.reverse().toString();",
                null, 0);
        openReader();

        List<SearchResult> results = search("StringBuilder reverse", "proj");

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).methodName()).isEqualTo("reverse");
    }

    @Test
    void search_returnsEmptyForNoMatch() throws IOException {
        writeMethod("proj", "Foo", "bar", "public void bar()", "// no-op", null, 0);
        openReader();

        List<SearchResult> results = search("zzznomatch", "proj");

        assertThat(results).isEmpty();
    }

    // --- graph boost ---

    @Test
    void search_graphBoost_highInDegreeRanksHigher() throws IOException {
        // Both methods match "process" — the one with high in-degree should rank first
        writeMethod("proj", "Processor", "processCore", "public void processCore()", "process data", null, 15);
        writeMethod("proj", "Processor", "processOther", "public void processOther()", "process data", null, 0);
        openReader();

        List<SearchResult> results = search("process", "proj");

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);
        assertThat(results.get(0).methodName()).isEqualTo("processCore");
    }

    // --- docType filter ---

    @Test
    void search_methodDocTypeFilter_returnsOnlyMethods() throws IOException {
        writeMethod("proj", "Foo", "doSomething", "public void doSomething()", "something", null, 0);
        writeClassDoc("proj", "Foo", "com.example.Foo", "class contains something methods");
        openReader();

        SearchRequest req = new SearchRequest("something", SearchRequest.SearchType.KEYWORD,
                "proj", null, 10, "text", "method", null, 0);
        List<SearchResult> results = strategy.search(reader, req);

        assertThat(results).isNotEmpty();
        assertThat(results).allMatch(r -> "method".equals(r.docType()));
    }

    @Test
    void search_classDocTypeFilter_returnsOnlyClasses() throws IOException {
        writeMethod("proj", "Foo", "compute", "public void compute()", "arithmetic class compute", null, 0);
        writeClassDoc("proj", "Foo", "com.example.Foo", "arithmetic compute class body");
        openReader();

        SearchRequest req = new SearchRequest("arithmetic", SearchRequest.SearchType.KEYWORD,
                "proj", null, 10, "text", "class", null, 0);
        List<SearchResult> results = strategy.search(reader, req);

        assertThat(results).isNotEmpty();
        assertThat(results).allMatch(r -> "class".equals(r.docType()));
    }

    @Test
    void search_noDocTypeFilter_returnsBothMethodsAndClasses() throws IOException {
        writeMethod("proj", "Foo", "myMethod", "public void myMethod()", "my body", null, 0);
        writeClassDoc("proj", "Foo", "com.example.Foo", "my class body");
        openReader();

        SearchRequest req = new SearchRequest("my", SearchRequest.SearchType.KEYWORD,
                "proj", null, 10, "text", null, null, 0);
        List<SearchResult> results = strategy.search(reader, req);

        assertThat(results).anyMatch(r -> "method".equals(r.docType()));
        assertThat(results).anyMatch(r -> "class".equals(r.docType()));
    }

    // --- project filter ---

    @Test
    void search_projectFilter_restrictsByProject() throws IOException {
        writeMethod("proj-a", "Service", "findUser", "public User findUser()", "body", null, 0);
        writeMethod("proj-b", "Service", "findUser", "public User findUser()", "body", null, 0);
        openReader();

        List<SearchResult> results = search("findUser", "proj-a");

        assertThat(results).isNotEmpty();
        assertThat(results).allMatch(r -> "proj-a".equals(r.project()));
    }

    // --- result fields ---

    @Test
    void search_resultContainsExpectedFields() throws IOException {
        writeMethod("proj", "Calculator", "add", "public int add(int a, int b)", "return a + b;", null, 0);
        openReader();

        List<SearchResult> results = search("add", "proj");

        assertThat(results).isNotEmpty();
        SearchResult r = results.get(0);
        assertThat(r.project()).isEqualTo("proj");
        assertThat(r.className()).isEqualTo("Calculator");
        assertThat(r.methodName()).isEqualTo("add");
        assertThat(r.signature()).contains("add");
        assertThat(r.score()).isPositive();
        assertThat(r.searchType()).isEqualTo("keyword");
        assertThat(r.docType()).isEqualTo("method");
    }

    // --- sourcePathPenalty ---

    @Test
    void sourcePathPenalty_productionSource_returnsOne() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/src/main/java/com/example/Foo.java"))
                .isEqualTo(1.0f);
    }

    @Test
    void sourcePathPenalty_testSource_returnsLow() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/src/test/java/com/example/FooTest.java"))
                .isEqualTo(0.30f);
    }

    @Test
    void sourcePathPenalty_benchmark_returnsLow() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/src/benchmark/java/com/example/Bench.java"))
                .isEqualTo(0.25f);
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/jmh/java/com/example/Bench.java"))
                .isEqualTo(0.25f);
    }

    @Test
    void sourcePathPenalty_githubWorkflow_returnsVeryLow() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/.github/workflows/ci.yml"))
                .isEqualTo(0.10f);
    }

    @Test
    void sourcePathPenalty_docFiles_returnsVeryLow() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/docs/README.md")).isEqualTo(0.10f);
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/scripts/build.sh")).isEqualTo(0.10f);
        assertThat(KeywordSearchStrategy.sourcePathPenalty("/config/app.yml")).isEqualTo(0.10f);
    }

    @Test
    void sourcePathPenalty_null_returnsOne() {
        assertThat(KeywordSearchStrategy.sourcePathPenalty(null)).isEqualTo(1.0f);
    }

    @Test
    void sourcePathPenalty_windowsPath_normalizedCorrectly() {
        // Backslash paths (Windows) must be treated the same as forward slashes
        assertThat(KeywordSearchStrategy.sourcePathPenalty("C:\\project\\src\\test\\FooTest.java"))
                .isEqualTo(0.30f);
    }

    // --- synonym hot-reload ---

    @Test
    void search_worksWithNoSynonymFile() throws IOException {
        // Synonym file absent — strategy must still construct and search normally
        Path absentFile = tempDir.resolve("missing-synonyms.txt");
        KeywordSearchStrategy s = new KeywordSearchStrategy(absentFile);

        writeMethod("proj", "Calc", "add", "public int add(int a, int b)", "return a + b;", null, 0);
        openReader();

        List<SearchResult> results = s.search(reader, SearchRequest.keyword("add", "proj", 10));
        assertThat(results).isNotEmpty();
    }

    @Test
    void search_picksUpSynonymAfterFileCreated() throws Exception {
        Path synonymFile = tempDir.resolve("synonyms.txt");
        // Start with no synonym file
        KeywordSearchStrategy s = new KeywordSearchStrategy(synonymFile);

        writeMethod("proj", "Calc", "sum", "public int sum(int a, int b)", "return a + b;", null, 0);
        openReader();

        // First search: no synonyms → "addition" does not match "sum"
        List<SearchResult> before = s.search(reader, SearchRequest.keyword("addition", "proj", 10));
        assertThat(before).isEmpty();

        // Write a synonym rule and ensure mtime advances
        Files.writeString(synonymFile, "addition => sum\n");
        // Force mtime change if filesystem resolution is coarse
        synonymFile.toFile().setLastModified(System.currentTimeMillis() + 1000);

        // Next search triggers reloadIfChanged → new analyzer active
        List<SearchResult> after = s.search(reader, SearchRequest.keyword("addition", "proj", 10));
        assertThat(after).isNotEmpty();
        assertThat(after.get(0).methodName()).isEqualTo("sum");
    }

    @Test
    void search_reloadsWhenSynonymFileModified() throws Exception {
        Path synonymFile = tempDir.resolve("synonyms.txt");
        Files.writeString(synonymFile, "arithmetic => add\n");

        KeywordSearchStrategy s = new KeywordSearchStrategy(synonymFile);

        writeMethod("proj", "Calc", "add", "public int add(int a, int b)", "return a + b;", null, 0);
        writeMethod("proj", "Calc", "multiply", "public int multiply(int a, int b)", "return a * b;", null, 0);
        openReader();

        // "arithmetic" maps to "add" — multiply should not appear at top
        List<SearchResult> first = s.search(reader, SearchRequest.keyword("arithmetic", "proj", 10));
        assertThat(first).isNotEmpty();
        assertThat(first.get(0).methodName()).isEqualTo("add");

        // Update synonym file: now "arithmetic" maps to "multiply"
        Files.writeString(synonymFile, "arithmetic => multiply\n");
        synonymFile.toFile().setLastModified(System.currentTimeMillis() + 2000);

        List<SearchResult> second = s.search(reader, SearchRequest.keyword("arithmetic", "proj", 10));
        assertThat(second).isNotEmpty();
        assertThat(second.get(0).methodName()).isEqualTo("multiply");
    }

    // --- 3-stage proximity ranking ---

    /**
     * Doc B has two of the three query terms in the high-boost methodName field (4× each),
     * giving it a strong OR score but failing the AND clause (missing "segment").
     * Doc A has all three terms adjacent in javadoc: AND and phrase SHOULD clauses combined
     * must overcome Doc B's methodName advantage.
     *
     * Before 3-stage scoring Doc B wins via OR alone.
     * After: Doc A gets AND bonus (all terms present) + phrase bonus (terms adjacent) → wins.
     */
    @Test
    @org.junit.jupiter.api.Disabled("signature moved to IDF-only; phrase/scatter balance needs retuning")
    void search_phraseMatch_ranksAboveScatteredTerms() throws IOException {
        // Doc B (written first): "merge" + "result" in methodName (4× boost, 2 of 3 terms).
        // AND fails (missing "segment"); phrase fails for the same reason.
        writeMethod("proj", "Alpha", "mergeResult",
                "public void mergeResult()",
                "perform work",
                null, 0);
        // Doc A (written second): all three terms adjacent in javadoc → AND + phrase both fire.
        writeMethod("proj", "Beta", "handle",
                "public void handle()",
                "handle work",
                "merge segment result process", 0);
        openReader();

        List<SearchResult> results = search("merge segment result", "proj");

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);
        assertThat(results.get(0).methodName()).isEqualTo("handle");
    }

    /**
     * Variant with a different partial-match pattern: Doc B has two query terms split across
     * methodName (merge) and className (segment), while Doc A has all three in javadoc.
     * The min-should-match SHOULD clause tips the balance once all terms are present.
     */
    @Test
    void search_allTermsPresentAndAdjacent_outranksHighBoostPartialMatch() throws IOException {
        // "merge" + "segment" appear across methodName/className (high boost) but "result" absent.
        writeMethod("proj", "SegmentManager", "mergeItems",
                "public void mergeItems()",
                "perform work",
                null, 0);
        // All three terms adjacent in javadoc → min-should-match + phrase both fire.
        writeMethod("proj", "Compactor", "compact",
                "public void compact()",
                "handle work",
                "merge segment result process", 0);
        openReader();

        List<SearchResult> results = search("merge segment result", "proj");

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);
        assertThat(results.get(0).methodName()).isEqualTo("compact");
    }

    /**
     * 4-token query: min-should-match floor is ceil(4 * 0.75) = 3.
     * Doc A has all 4 tokens; Doc B has only 1 token in a high-boost field.
     * Doc A must rank first via the middle tier even though Doc B has a field-boost advantage.
     */
    @Test
    void search_minShouldMatch_fourTermQuery_threeOfFourRequired() throws IOException {
        // Doc B: only "index" in methodName (4× boost) — misses "writer", "commit", "flush".
        writeMethod("proj", "Repository", "indexItems",
                "public void indexItems()",
                "perform work",
                null, 0);
        // Doc A: all 4 terms in javadoc → min-should-match fires (4/4 ≥ 3 required).
        writeMethod("proj", "IndexManager", "manage",
                "public void manage()",
                "handle work",
                "index writer commit flush operations", 0);
        openReader();

        List<SearchResult> results = search("index writer commit flush", "proj");

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);
        assertThat(results.get(0).methodName()).isEqualTo("manage");
    }

    /**
     * Regression: the OR base tier must still return documents that miss one query term.
     * Adding phrase and AND as SHOULD (not MUST) must not filter out partial matches.
     */
    @Test
    void search_orFallback_returnsDocWithPartialTermMatch() throws IOException {
        writeMethod("proj", "Validator", "validateSchema",
                "public void validateSchema()",
                "check structure",
                "validate schema document", 0);
        openReader();

        // "xml" is absent from the document — OR fallback must still return it on "validate"+"schema"
        List<SearchResult> results = search("validate xml schema", "proj");

        assertThat(results).isNotEmpty();
        assertThat(results.get(0).methodName()).isEqualTo("validateSchema");
    }

    // --- helpers ---

    private List<SearchResult> search(String query, String project) throws IOException {
        return strategy.search(reader, SearchRequest.keyword(query, project, 20));
    }

    private void writeMethod(String project, String className, String methodName,
                              String signature, String body, String javadoc,
                              int inDegree) throws IOException {
        ParsedMethod method = new ParsedMethod(
                project + ":com.example." + className + "#" + methodName + "()",
                project, "com.example", className, "com.example." + className,
                methodName, signature, "void",
                List.of(), List.of(),
                body, javadoc, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/" + className + ".java", 1, 10
        );
        writeDoc(DocumentMapper.toDocument(method, null, inDegree, List.of()));
    }

    private void writeClassDoc(String project, String className, String qualifiedName,
                                String synthesizedBody) throws IOException {
        ParsedClass cls = new ParsedClass(
                project, "com.example", className, qualifiedName,
                "class", null, List.of(), List.of(),
                "public", false, false, null,
                "/src/" + className + ".java", 1, 50
        );
        writeDoc(DocumentMapper.toClassDocument(cls, synthesizedBody, null));
    }

    private void writeDoc(Document doc) throws IOException {
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
}
