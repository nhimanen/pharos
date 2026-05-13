package com.pharos.integration;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end integration test that parses the sample-app test project,
 * indexes it into an in-memory Lucene store, and verifies search results.
 *
 * Uses {@link ByteBuffersDirectory} (no real disk I/O for Lucene)
 * and no ProjectRegistry — tests the parse → index → search pipeline directly.
 */
class SearchIndexIntegrationTest {

    private static final String PROJECT = "sample-app";

    private ByteBuffersDirectory dir;
    private DirectoryReader reader;
    private ParsedProject parsedProject;
    private KeywordSearchStrategy strategy;

    @BeforeEach
    void setUp() throws Exception {
        dir = new ByteBuffersDirectory();
        strategy = new KeywordSearchStrategy();

        // Parse the test project
        Path projectRoot = testProjectPath("sample-app");
        JavaCodeParser parser = new JavaCodeParser();
        parsedProject = parser.parseProject(projectRoot, PROJECT);

        // Index all methods and classes into in-memory Lucene
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            Map<String, List<ParsedMethod>> methodsByClass = parsedProject.allMethods().stream()
                    .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));

            for (ParsedFile file : parsedProject.files()) {
                for (ParsedMethod method : file.methods()) {
                    writer.addDocument(DocumentMapper.toDocument(method, null, 0, List.of()));
                }
                for (ParsedClass cls : file.classes()) {
                    List<ParsedMethod> methods = methodsByClass.getOrDefault(
                            cls.qualifiedClassName(), List.of());
                    String synthesized = buildSynthesizedBody(methods);
                    writer.addDocument(DocumentMapper.toClassDocument(cls, synthesized, null));
                }
            }
            writer.commit();
        }

        reader = DirectoryReader.open(dir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (reader != null) reader.close();
        dir.close();
    }

    // --- Parser verification ---

    @Test
    void parser_findsExpectedClasses() {
        List<String> classNames = parsedProject.allClasses().stream()
                .map(ParsedClass::className)
                .collect(Collectors.toList());

        assertThat(classNames).containsExactlyInAnyOrder("Calculator", "User", "GreetingService");
    }

    @Test
    void parser_findsExpectedMethodsInCalculator() {
        List<String> calculatorMethods = parsedProject.allMethods().stream()
                .filter(m -> "Calculator".equals(m.className()))
                .map(ParsedMethod::methodName)
                .collect(Collectors.toList());

        assertThat(calculatorMethods).containsExactlyInAnyOrder("add", "subtract", "multiply", "divide", "abs");
    }

    @Test
    void parser_extractsJavadocFromAddMethod() {
        Optional<ParsedMethod> addMethod = parsedProject.allMethods().stream()
                .filter(m -> "add".equals(m.methodName()) && "Calculator".equals(m.className()))
                .findFirst();

        assertThat(addMethod).isPresent();
        assertThat(addMethod.get().javadoc()).isNotBlank();
        assertThat(addMethod.get().javadoc()).contains("sum");
    }

    // --- Keyword search ---

    @Test
    void search_methodByName_findsAddMethod() throws IOException {
        List<SearchResult> results = searchMethods("add", 10);

        assertThat(results).anyMatch(r -> "add".equals(r.methodName()));
    }

    @Test
    void search_methodByJavadocConcept_findsAddMethod() throws IOException {
        // add() javadoc contains "sum" — BM25 should surface it
        List<SearchResult> results = searchMethods("sum", 10);

        assertThat(results).anyMatch(r -> "add".equals(r.methodName()));
    }

    @Test
    void search_greetingByBodyContent_findsGreetingService() throws IOException {
        List<SearchResult> results = searchMethods("Hello", 10);

        assertThat(results).anyMatch(r -> "GreetingService".equals(r.className()));
    }

    @Test
    void search_divideByZeroKeyword_findsDivideMethod() throws IOException {
        List<SearchResult> results = searchMethods("zero", 10);

        assertThat(results).anyMatch(r -> "divide".equals(r.methodName()));
    }

    @Test
    void search_userEmail_findsUserMethods() throws IOException {
        List<SearchResult> results = searchMethods("email", 10);

        assertThat(results).anyMatch(r -> "User".equals(r.className()));
    }

    // --- Class-level search ---

    @Test
    void search_classDocType_returnsOnlyClassDocuments() throws IOException {
        List<SearchResult> results = searchClasses("greet farewell", 10);

        assertThat(results).isNotEmpty();
        assertThat(results).allMatch(r -> "class".equals(r.docType()));
    }

    @Test
    void search_classForGreetingConcept_findsGreetingServiceClass() throws IOException {
        List<SearchResult> results = searchClasses("greeting farewell", 10);

        assertThat(results).anyMatch(r -> "GreetingService".equals(r.className()));
    }

    @Test
    void search_classForArithmeticConcept_findsCalculatorClass() throws IOException {
        List<SearchResult> results = searchClasses("add subtract multiply divide", 10);

        assertThat(results).anyMatch(r -> "Calculator".equals(r.className()));
    }

    // --- docType discrimination ---

    @Test
    void search_methodFilter_excludesClassDocuments() throws IOException {
        SearchRequest req = new SearchRequest("name email user", SearchRequest.SearchType.KEYWORD,
                PROJECT, null, 20, "text", "method", null);
        List<SearchResult> results = strategy.search(reader, req);

        assertThat(results).allMatch(r -> "method".equals(r.docType()));
    }

    // --- pom.xml parsing ---

    @Test
    void pomReader_parsesSampleAppPom() throws Exception {
        Path projectRoot = testProjectPath("sample-app");
        MavenPomReader reader = new MavenPomReader();
        MavenPomReader.PomInfo info = MavenPomReader.findPom(projectRoot)
                .flatMap(reader::read)
                .orElseThrow(() -> new AssertionError("pom.xml not found or parse failed"));

        assertThat(info.coordinates().groupId()).isEqualTo("com.example");
        assertThat(info.coordinates().artifactId()).isEqualTo("sample-app");
        assertThat(info.coordinates().version()).isEqualTo("1.0.0");
        assertThat(info.dependencies()).anyMatch(d -> "commons-lang3".equals(d.artifactId()));
        assertThat(info.dependencies()).anyMatch(d ->
                "junit-jupiter".equals(d.artifactId()) && "test".equals(d.scope()));
    }

    // --- helpers ---

    private List<SearchResult> searchMethods(String query, int limit) throws IOException {
        SearchRequest req = new SearchRequest(query, SearchRequest.SearchType.KEYWORD,
                PROJECT, null, limit, "text", "method", null);
        return strategy.search(reader, req);
    }

    private List<SearchResult> searchClasses(String query, int limit) throws IOException {
        SearchRequest req = new SearchRequest(query, SearchRequest.SearchType.KEYWORD,
                PROJECT, null, limit, "text", "class", null);
        return strategy.search(reader, req);
    }

    private static String buildSynthesizedBody(List<ParsedMethod> methods) {
        StringBuilder sb = new StringBuilder();
        for (ParsedMethod m : methods) {
            sb.append(m.signature()).append("\n");
            if (m.javadoc() != null && !m.javadoc().isBlank()) {
                sb.append("  // ").append(m.javadoc().replaceAll("\\s+", " ").trim()).append("\n");
            }
        }
        return sb.toString();
    }

    private static Path testProjectPath(String name) throws URISyntaxException {
        URL resource = SearchIndexIntegrationTest.class
                .getClassLoader().getResource("test-projects/" + name);
        assertThat(resource).as("test project not found: test-projects/" + name).isNotNull();
        return Paths.get(resource.toURI());
    }
}
