package com.pharos.indexer;

import com.pharos.parser.GenericFileParser;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Verifies that non-code files indexed via GenericFileParser produce
 * a single Lucene document per file (docType="document") with the file
 * content stored in the body field — no separate chunk documents.
 */
class DocumentFileIndexingTest {

    private static final String PROJECT = "sample-docs";
    private static final Path SAMPLE_DOCS = sampleDocsPath();

    // -------------------------------------------------------------------------
    // GenericFileParser output shape
    // -------------------------------------------------------------------------

    @Test
    void parser_producesOneClassPerFile_noMethods() throws Exception {
        GenericFileParser parser = new GenericFileParser();
        ParsedFile file = parser.parseFile(SAMPLE_DOCS.resolve("README.md"), PROJECT);

        assertThat(file.classes()).hasSize(1);
        assertThat(file.methods()).isEmpty();
    }

    @Test
    void parser_documentClassKindIsDocument() throws Exception {
        ParsedFile file = new GenericFileParser().parseFile(SAMPLE_DOCS.resolve("README.md"), PROJECT);

        assertThat(file.classes().get(0).kind()).isEqualTo("document");
    }

    // -------------------------------------------------------------------------
    // Synthesized body for document-kind classes
    // -------------------------------------------------------------------------

    @Test
    void synthesizedBody_containsFileContent() throws Exception {
        ParsedFile file = new GenericFileParser().parseFile(SAMPLE_DOCS.resolve("README.md"), PROJECT);
        ParsedClass docClass = file.classes().get(0);

        // Simulate what ProjectIndexManager does: read file content as body for document classes.
        String body = DocumentMapper.readBodyFromFile(docClass.filePath(), docClass.startLine(), docClass.endLine());

        assertThat(body).contains("Code Search");
        assertThat(body).contains("BM25");
        assertThat(body).contains("Installation");
    }

    @Test
    void synthesizedBody_isNotEmpty_forAllDocTypes() throws Exception {
        for (String fileName : List.of("README.md", "notes.txt", "config.yaml")) {
            ParsedFile file = new GenericFileParser().parseFile(SAMPLE_DOCS.resolve(fileName), PROJECT);
            ParsedClass docClass = file.classes().get(0);
            String body = DocumentMapper.readBodyFromFile(docClass.filePath(), docClass.startLine(), docClass.endLine());
            assertThat(body).as("body for %s", fileName).isNotBlank();
        }
    }

    // -------------------------------------------------------------------------
    // Lucene document shape — no chunk docType, body holds file text
    // -------------------------------------------------------------------------

    @Test
    void indexedDocuments_haveDocTypeDocument_notChunk() throws Exception {
        List<Document> docs = indexSampleDocs();

        for (Document doc : docs) {
            String docType = doc.get(DocumentMapper.F_DOC_TYPE);
            assertThat(docType)
                    .as("docType for id=%s", doc.get(DocumentMapper.F_ID))
                    .isNotEqualTo("chunk");
        }
    }

    @Test
    void indexedDocuments_onePerFile() throws Exception {
        List<Document> docs = indexSampleDocs();

        // sample-docs has 4 files: README.md, docs/getting-started.md, notes.txt, config.yaml
        assertThat(docs).hasSize(4);
    }

    @Test
    void indexedDocuments_bodyContainsFileContent() throws Exception {
        List<Document> docs = indexSampleDocs();

        Document readme = docs.stream()
                .filter(d -> d.get(DocumentMapper.F_ID).contains("README"))
                .findFirst().orElseThrow();

        String body = readme.get(DocumentMapper.F_BODY);
        assertThat(body).contains("Code Search");
        assertThat(body).contains("Installation");
    }

    @Test
    void indexedDocuments_javadocIsFileDescription() throws Exception {
        List<Document> docs = indexSampleDocs();

        Document readme = docs.stream()
                .filter(d -> d.get(DocumentMapper.F_ID).contains("README"))
                .findFirst().orElseThrow();

        // javadoc field = first heading/paragraph, set by GenericFileParser
        assertThat(readme.get(DocumentMapper.F_JAVADOC)).isEqualTo("Code Search");
    }

    @Test
    void indexedDocuments_filePathStored() throws Exception {
        List<Document> docs = indexSampleDocs();

        docs.forEach(doc ->
                assertThat(doc.get(DocumentMapper.F_FILE_PATH))
                        .as("filePath for %s", doc.get(DocumentMapper.F_ID))
                        .isNotBlank());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Index the full sample-docs project into an in-memory Lucene directory. */
    private static List<Document> indexSampleDocs() throws Exception {
        GenericFileParser parser = new GenericFileParser();
        var project = parser.parseProject(SAMPLE_DOCS, PROJECT);

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (var file : project.files()) {
                for (ParsedClass cls : file.classes()) {
                    // Mirror what ProjectIndexManager does for document classes:
                    // synthesized body = raw file content.
                    String body = DocumentMapper.readBodyFromFile(
                            cls.filePath(), cls.startLine(), cls.endLine());
                    writer.addDocument(DocumentMapper.toClassDocument(cls, body, null));
                }
            }
        }

        List<Document> results = new ArrayList<>();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new MatchAllDocsQuery(), 100);
            for (var sd : hits.scoreDocs) {
                results.add(reader.storedFields().document(sd.doc));
            }
        }
        return results;
    }

    private static Path sampleDocsPath() {
        try {
            var url = DocumentFileIndexingTest.class.getClassLoader()
                    .getResource("test-projects/sample-docs");
            if (url == null) throw new RuntimeException("sample-docs not found");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
