package com.pharos.integration;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.PythonCodeParser;
import com.pharos.parser.model.*;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end integration test: parse sample-python-app → index into in-memory
 * Lucene → verify search results.
 */
class PythonSearchIntegrationTest {

    private static final Path SAMPLE_APP = sampleAppPath();
    private static final String PROJECT = "sample-python-app";

    private static ParsedProject parsedProject;

    private ByteBuffersDirectory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @BeforeAll
    static void requirePython3AndParse() throws Exception {
        Assumptions.assumeTrue(isPython3Available(), "python3 not on PATH — skipping Python integration tests");
        parsedProject = new PythonCodeParser().parseProject(SAMPLE_APP, PROJECT);
        Assumptions.assumeTrue(!parsedProject.files().isEmpty(),
                "Extractor returned no files — skipping");
    }

    @BeforeEach
    void buildIndex() throws Exception {
        directory = new ByteBuffersDirectory();

        Map<String, List<ParsedMethod>> byClass = parsedProject.allMethods().stream()
                .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));

        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (ParsedMethod m : parsedProject.allMethods()) {
                writer.addDocument(DocumentMapper.toDocument(m, null, 0, List.of()));
            }
            for (ParsedClass cls : parsedProject.allClasses()) {
                List<ParsedMethod> methods = byClass.getOrDefault(cls.qualifiedClassName(), List.of());
                String body = methods.stream()
                        .map(ParsedMethod::signature).collect(Collectors.joining("\n"));
                writer.addDocument(DocumentMapper.toClassDocument(cls, body, null));
            }
            writer.commit();
        }

        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    // -----------------------------------------------------------------------
    // Corpus validation
    // -----------------------------------------------------------------------

    @Test
    void parsesAllExpectedFiles() {
        assertThat(parsedProject.files()).hasSize(3);
    }

    @Test
    void indexesFunctions_andClasses() {
        assertThat(parsedProject.allMethods().size()).isGreaterThan(5);
        // Classes: User, GreetingService + module pseudo-classes
        assertThat(parsedProject.allClasses().size()).isGreaterThanOrEqualTo(2);
        long realClasses = parsedProject.allClasses().stream()
                .filter(c -> c.kind().equals("class")).count();
        assertThat(realClasses).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    @Test
    void search_functionByName_findsCalculatorAdd() throws Exception {
        List<SearchResult> results = search("add");
        List<String> names = results.stream().map(SearchResult::methodName).toList();
        assertThat(names).contains("add");
    }

    @Test
    void search_functionByDocstring_findsMultiply() throws Exception {
        List<SearchResult> results = search("product");
        List<String> names = results.stream().map(SearchResult::methodName).toList();
        assertThat(names).contains("multiply");
    }

    @Test
    void search_methodByDocstring_findsGreeting() throws Exception {
        List<SearchResult> results = search("greeting");
        // greet, farewell, or GreetingService class should appear
        assertThat(results).isNotEmpty();
    }

    @Test
    void search_byClassName_findsUser() throws Exception {
        List<SearchResult> results = search("User");
        assertThat(results).isNotEmpty();
        boolean hasUser = results.stream()
                .anyMatch(r -> "User".equals(r.className()) || "User".equals(r.methodName()));
        assertThat(hasUser).isTrue();
    }

    @Test
    void search_methodByBody_findsDivideZeroCheck() throws Exception {
        List<SearchResult> results = search("divide zero");
        assertThat(results).isNotEmpty();
        assertThat(results.get(0).methodName()).isEqualTo("divide");
    }

    @Test
    void docType_method_vs_class() {
        long methods = parsedProject.allMethods().stream()
                .filter(m -> !m.methodName().equals("__init__") || m.isConstructor())
                .count();
        long classes = parsedProject.allClasses().stream()
                .filter(c -> c.kind().equals("class"))
                .count();
        assertThat(methods).isGreaterThan(0);
        assertThat(classes).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<SearchResult> search(String query) throws Exception {
        SearchRequest req = new SearchRequest(
                query, SearchRequest.SearchType.KEYWORD,
                null, null, 20, "text", null);
        KeywordSearchStrategy strategy = new KeywordSearchStrategy();
        return strategy.search(reader, req);
    }

    private static Path sampleAppPath() {
        try {
            var url = PythonSearchIntegrationTest.class.getClassLoader()
                    .getResource("test-projects/sample-python-app");
            if (url == null) throw new RuntimeException("sample-python-app test resource not found");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isPython3Available() {
        try {
            Process p = new ProcessBuilder("python3", "--version")
                    .redirectErrorStream(true).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
