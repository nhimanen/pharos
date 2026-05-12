package com.pharos.parser;

import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class JsCodeParserTest {

    @TempDir Path tempDir;
    private final JsCodeParser parser = new JsCodeParser();

    @Test
    void supportedExtensions_includesJsAndTs() {
        List<String> exts = parser.supportedExtensions();
        assertThat(exts).contains(".js", ".ts", ".jsx", ".tsx", ".mjs", ".cjs");
    }

    @Test
    void parseProject_whenNodeNotAvailable_returnsEmptyGracefully() throws Exception {
        // Write a minimal JS file
        Path src = tempDir.resolve("app.js");
        Files.writeString(src, "function greet(name) { return 'Hello ' + name; }\n");

        // JsCodeParser gracefully returns an empty project when 'node' is not on PATH.
        // This test validates the fallback behaviour in environments without Node.js.
        ParsedProject project = parser.parseProject(tempDir, "test-project");
        assertThat(project).isNotNull();
        assertThat(project.projectName()).isEqualTo("test-project");
        // Either parsed successfully (node present) or returned empty (node absent)
        // — either outcome is valid; the important thing is no exception is thrown.
    }

    @Test
    void parseProject_skipsNonJsFiles() throws Exception {
        // A directory with only Java files should produce an empty project
        Path src = tempDir.resolve("Main.java");
        Files.writeString(src, "public class Main { public static void main(String[] a) {} }\n");

        ParsedProject project = parser.parseProject(tempDir, "java-proj");
        // JsCodeParser should produce 0 results for Java files
        assertThat(project.files()).isEmpty();
    }

    @Test
    void defaultReturnType_isAny() {
        assertThat(parser.defaultReturnType()).isEqualTo("any");
    }

    @Test
    void buildSignatureString_noDecorators() {
        // Access via subclass hook
        String sig = parser.buildSignatureString("greet", List.of("name", "options"), List.of());
        assertThat(sig).isEqualTo("greet(name, options)");
    }

    @Test
    void buildSignatureString_withDecorators() {
        String sig = parser.buildSignatureString("handler", List.of("req", "res"),
                List.of("Get", "Authenticated"));
        assertThat(sig).isEqualTo("@Get @Authenticated handler(req, res)");
    }
}
