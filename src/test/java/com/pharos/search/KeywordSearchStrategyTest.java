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

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class KeywordSearchStrategyTest {

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
