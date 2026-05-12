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
        strategy = new KeywordSearchStrategy();
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
                "proj", null, 10, "text", "method");
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
                "proj", null, 10, "text", "class");
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
                "proj", null, 10, "text", null);
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
