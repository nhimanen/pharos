package com.pharos.analysis;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.model.ParsedClass;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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

class ConceptMinerTest {

    @TempDir
    Path tempDir;

    private ByteBuffersDirectory dir;
    private DirectoryReader reader;

    private final ConceptMiner miner = new ConceptMiner();

    @BeforeEach
    void setUp() {
        dir = new ByteBuffersDirectory();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (reader != null) reader.close();
        dir.close();
    }

    // ── appendNewSynonyms ──────────────────────────────────────────────────────

    @Test
    void appendNewSynonyms_createsFileAndWritesRules() throws Exception {
        // Multi-word class names produce bigrams from Source 1 (class name decomposition):
        // "TokenStreamFilter" → tokens [token, stream, filter] → bigrams [tokenstream, streamfilter]
        writeClassDoc("proj", "TokenStreamFilter", "com.example.TokenStreamFilter", "");
        writeClassDoc("proj", "ByteBlockAllocator",  "com.example.ByteBlockAllocator",  "");
        openReader();

        Path synonymFile = tempDir.resolve("synonyms.txt");
        int added = miner.appendNewSynonyms(reader, synonymFile, "proj");

        assertThat(synonymFile).exists();
        assertThat(added).isGreaterThan(0);

        String content = Files.readString(synonymFile);
        assertThat(content).contains("=>");
        assertThat(content).contains("# ── Auto-mined from 'proj'");
    }

    @Test
    void appendNewSynonyms_noRulesWhenNoClassDocs() throws Exception {
        // Method docs alone produce nothing — method names are no longer a source.
        writeMethodDoc("proj", "SomeClass", "doWork", "performs important work");
        openReader();

        Path synonymFile = tempDir.resolve("synonyms.txt");
        int added = miner.appendNewSynonyms(reader, synonymFile, "proj");

        assertThat(added).isEqualTo(0);
        assertThat(synonymFile).doesNotExist();
    }

    @Test
    void appendNewSynonyms_skipsTermsAlreadyInFile() throws Exception {
        Path synonymFile = tempDir.resolve("synonyms.txt");
        Files.writeString(synonymFile, "tokenstream => tokenstreamfilter\n");

        writeClassDoc("proj", "TokenStreamFilter", "com.example.TokenStreamFilter", "");
        writeClassDoc("proj", "ByteBlockAllocator",  "com.example.ByteBlockAllocator",  "");
        openReader();

        miner.appendNewSynonyms(reader, synonymFile, "proj");

        String content = Files.readString(synonymFile);
        long lhsOccurrences = content.lines()
                .filter(l -> !l.startsWith("#") && l.contains("=>"))
                .filter(l -> l.split("=>")[0].strip().equals("tokenstream"))
                .count();
        assertThat(lhsOccurrences).isEqualTo(1);
    }

    @Test
    void appendNewSynonyms_appendsOnSubsequentRuns() throws Exception {
        writeClassDoc("proj", "TokenStreamFilter", "com.example.TokenStreamFilter", "");
        writeClassDoc("proj", "ByteBlockAllocator",  "com.example.ByteBlockAllocator",  "");
        openReader();

        Path synonymFile = tempDir.resolve("synonyms.txt");
        int firstRun  = miner.appendNewSynonyms(reader, synonymFile, "proj");
        int secondRun = miner.appendNewSynonyms(reader, synonymFile, "proj");

        assertThat(firstRun).isGreaterThan(0);
        assertThat(secondRun).isEqualTo(0);
    }

    @Test
    void appendNewSynonyms_shortTermsAreSkipped() throws Exception {
        // Single-char tokens from class name splits are below the 4-char minimum
        writeClassDoc("proj", "BPReorderer", "com.example.BPReorderer", "");
        writeClassDoc("proj", "BPIndexer",   "com.example.BPIndexer",   "");
        openReader();

        Path synonymFile = tempDir.resolve("synonyms.txt");
        miner.appendNewSynonyms(reader, synonymFile, "proj");

        if (Files.exists(synonymFile)) {
            String content = Files.readString(synonymFile);
            content.lines()
                    .filter(l -> !l.startsWith("#") && l.contains("=>"))
                    .forEach(line -> {
                        String lhs = line.split("=>")[0].strip();
                        assertThat(lhs.length()).isGreaterThanOrEqualTo(4);
                    });
        }
    }

    @Test
    void appendNewSynonyms_termEqualsClassnameIsSkipped() throws Exception {
        writeClassDoc("proj", "TokenStreamFilter", "com.example.TokenStreamFilter", "");
        writeClassDoc("proj", "ByteBlockAllocator",  "com.example.ByteBlockAllocator",  "");
        openReader();

        Path synonymFile = tempDir.resolve("synonyms.txt");
        miner.appendNewSynonyms(reader, synonymFile, "proj");

        if (Files.exists(synonymFile)) {
            String content = Files.readString(synonymFile);
            content.lines()
                    .filter(l -> !l.startsWith("#") && l.contains("=>"))
                    .forEach(line -> {
                        String lhs = line.split("=>")[0].strip();
                        String rhs = line.split("=>")[1].strip().split("\\s")[0];
                        assertThat(lhs).isNotEqualTo(rhs);
                    });
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private void writeClassDoc(String project, String className, String qualifiedName,
                                String synthesizedBody) throws IOException {
        ParsedClass cls = new ParsedClass(
                project, "com.example", className, qualifiedName,
                "class", null, List.of(), List.of(),
                "public", false, false, null,
                "/src/" + className + ".java", 1, 50
        );
        Document doc = DocumentMapper.toClassDocument(cls, synthesizedBody, (float[]) null);
        writeDoc(doc);
    }

    private void writeMethodDoc(String project, String className, String methodName,
                                 String body) throws IOException {
        com.pharos.parser.model.ParsedMethod method = new com.pharos.parser.model.ParsedMethod(
                project + ":com.example." + className + "#" + methodName + "()",
                project, "com.example", className, "com.example." + className,
                methodName, "public void " + methodName + "()", "void",
                List.of(), List.of(),
                body, null, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/" + className + ".java", 1, 10
        );
        writeDoc(DocumentMapper.toDocument(method, null, 0, List.of()));
    }

    private void writeDoc(Document doc) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer());
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
