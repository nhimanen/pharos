package com.pharos.parser;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class GenericFileParserTest {

    private static final Path SAMPLE_DOCS = sampleDocsPath();
    private static final String PROJECT = "sample-docs";

    // -----------------------------------------------------------------------
    // Project-level parsing
    // -----------------------------------------------------------------------

    @Test
    void parseProject_findsAllDocumentFiles() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // README.md, docs/getting-started.md, notes.txt, config.yaml
        assertThat(project.files()).hasSize(4);
    }

    @Test
    void parseProject_markdownProducesDocumentClass() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        List<String> kinds = project.allClasses().stream()
                .map(ParsedClass::kind).distinct().toList();
        assertThat(kinds).containsOnly("document");
    }

    @Test
    void parseProject_markdownProducesChunkMethods() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        List<ParsedMethod> chunks = project.allMethods();
        assertThat(chunks).isNotEmpty();
        // All must carry the __chunk__ annotation
        assertThat(chunks).allMatch(m -> m.annotations().contains("__chunk__"));
    }

    @Test
    void parseProject_markdownHeadingsBecomeSections() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        List<String> methodNames = project.allMethods().stream()
                .map(ParsedMethod::methodName).toList();
        // README.md has: Installation, Quick_Start, Features, Configuration, Contributing
        assertThat(methodNames).contains("Installation", "Quick_Start", "Features");
    }

    @Test
    void parseProject_breadcrumbStoredInSignature() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // "Java Projects" is a subsection of "Indexing Your Project" in getting-started.md
        ParsedMethod javaSection = project.allMethods().stream()
                .filter(m -> m.methodName().equals("Java_Projects"))
                .findFirst().orElseThrow();
        // breadcrumb preserves raw heading text (spaces), method name is sanitized
        assertThat(javaSection.signature()).contains("Indexing Your Project");
        assertThat(javaSection.signature()).contains("Java Projects");
    }

    @Test
    void parseProject_internalLinksExtracted() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // README.md Installation section links to docs/getting-started.md
        long linkCount = project.allMethods().stream()
                .mapToLong(m -> m.calledMethods().size())
                .sum();
        assertThat(linkCount).isGreaterThan(0);
    }

    @Test
    void parseProject_externalLinksIgnored() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // No http:// links should appear as call refs
        boolean anyExternal = project.allMethods().stream()
                .flatMap(m -> m.calledMethods().stream())
                .anyMatch(c -> c.calleeSimpleName().startsWith("http"));
        assertThat(anyExternal).isFalse();
    }

    @Test
    void parseProject_allLinksAreUnresolved() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        boolean allUnresolved = project.allMethods().stream()
                .flatMap(m -> m.calledMethods().stream())
                .allMatch(c -> !c.resolved());
        assertThat(allUnresolved).isTrue();
    }

    @Test
    void parseProject_docClassHasFileDescription() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        ParsedClass readme = project.allClasses().stream()
                .filter(c -> c.className().equals("README"))
                .findFirst().orElseThrow();
        // First heading of README.md is "Code Search"
        assertThat(readme.javadoc()).isEqualTo("Code Search");
    }

    @Test
    void parseProject_qualifiedClassNameFromPath() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // docs/getting-started.md → qualifiedClassName = "docs.getting-started"
        ParsedClass gettingStarted = project.allClasses().stream()
                .filter(c -> c.qualifiedClassName().contains("getting-started"))
                .findFirst().orElseThrow();
        assertThat(gettingStarted.qualifiedClassName()).isEqualTo("docs.getting-started");
    }

    @Test
    void parseProject_plainTextChunksAssParagraphs() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // notes.txt has 3 paragraphs
        List<ParsedMethod> txtChunks = project.allMethods().stream()
                .filter(m -> m.filePath().endsWith("notes.txt"))
                .toList();
        assertThat(txtChunks).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    void parseProject_yamlTopLevelKeysAsChunks() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        List<ParsedMethod> yamlChunks = project.allMethods().stream()
                .filter(m -> m.filePath().endsWith("config.yaml"))
                .toList();
        // config.yaml has 5 top-level keys
        assertThat(yamlChunks).hasSize(5);
        List<String> names = yamlChunks.stream().map(ParsedMethod::methodName).toList();
        assertThat(names).contains("indexDir", "embeddingModel", "maxResults");
    }

    @Test
    void parseProject_chunkLineRangeSet() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        project.allMethods().forEach(m -> {
            assertThat(m.startLine()).isGreaterThan(0);
            assertThat(m.endLine()).isGreaterThanOrEqualTo(m.startLine());
        });
    }

    // -----------------------------------------------------------------------
    // Graceful fallback
    // -----------------------------------------------------------------------

    @Test
    void parseProject_emptyDirectory_returnsEmptyProject() throws Exception {
        Path emptyDir = Files.createTempDirectory("generic-empty");
        ParsedProject project = new GenericFileParser().parseProject(emptyDir, "empty");
        assertThat(project.files()).isEmpty();
    }

    @Test
    void parseFile_singleMarkdown_returnsChunks() throws Exception {
        Path readme = SAMPLE_DOCS.resolve("README.md");
        var file = new GenericFileParser().parseFile(readme, PROJECT);

        assertThat(file.classes()).hasSize(1);
        assertThat(file.classes().get(0).kind()).isEqualTo("document");
        assertThat(file.methods()).isNotEmpty();
        assertThat(file.methods()).allMatch(m -> m.annotations().contains("__chunk__"));
    }

    // -----------------------------------------------------------------------
    // sanitizeMethodName
    // -----------------------------------------------------------------------

    @Test
    void sanitizeMethodName_replacesSpacesAndSpecialChars() {
        assertThat(GenericFileParser.sanitizeMethodName("Quick Start")).isEqualTo("Quick_Start");
        // Consecutive special chars are collapsed to single underscore
        assertThat(GenericFileParser.sanitizeMethodName("C++ Tips & Tricks")).isEqualTo("C_Tips_Tricks");
        assertThat(GenericFileParser.sanitizeMethodName("  leading space  ")).isEqualTo("leading_space");
    }

    @Test
    void sanitizeMethodName_truncatesLongTitles() {
        String long80 = "A".repeat(100);
        assertThat(GenericFileParser.sanitizeMethodName(long80)).hasSize(80);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Path sampleDocsPath() {
        try {
            var url = GenericFileParserTest.class.getClassLoader()
                    .getResource("test-projects/sample-docs");
            if (url == null) throw new RuntimeException("sample-docs test resource not found");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
