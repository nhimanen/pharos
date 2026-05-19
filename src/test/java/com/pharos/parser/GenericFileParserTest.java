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
    void parseProject_producesNoChunkMethods() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        // Each file is now a single document-class; no chunk ParsedMethods are emitted.
        assertThat(project.allMethods()).isEmpty();
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
    void parseProject_documentClassLineRangeSpansWholeFile() throws Exception {
        ParsedProject project = new GenericFileParser().parseProject(SAMPLE_DOCS, PROJECT);

        project.allClasses().forEach(c -> {
            assertThat(c.startLine()).isEqualTo(1);
            assertThat(c.endLine()).isGreaterThan(1);
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
    void parseFile_singleMarkdown_returnsOneDocumentClassNoMethods() throws Exception {
        Path readme = SAMPLE_DOCS.resolve("README.md");
        var file = new GenericFileParser().parseFile(readme, PROJECT);

        assertThat(file.classes()).hasSize(1);
        assertThat(file.classes().get(0).kind()).isEqualTo("document");
        assertThat(file.methods()).isEmpty();
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
