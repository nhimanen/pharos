package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class JavaCodeParserDetectSourceRootTest {

    @TempDir
    Path projectRoot;

    @Test
    void detectSourceRoot_mavenLayout_returnsSrcMainJava() throws IOException {
        Path expected = Files.createDirectories(projectRoot.resolve("src/main/java"));

        Path result = JavaCodeParser.detectSourceRoot(projectRoot);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void detectSourceRoot_gradleLayout_returnsSameAsMaven() throws IOException {
        // Gradle standard layout is also src/main/java — same result as Maven
        Path expected = Files.createDirectories(projectRoot.resolve("src/main/java"));

        Path result = JavaCodeParser.detectSourceRoot(projectRoot);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void detectSourceRoot_srcLayout_returnsSrc() throws IOException {
        // No src/main/java — only src/
        Path expected = Files.createDirectories(projectRoot.resolve("src"));

        Path result = JavaCodeParser.detectSourceRoot(projectRoot);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void detectSourceRoot_noConventionalLayout_returnsProjectRoot() {
        // No src or src/main/java — falls back to project root
        Path result = JavaCodeParser.detectSourceRoot(projectRoot);

        assertThat(result).isEqualTo(projectRoot);
    }

    @Test
    void detectSourceRoot_srcMainJavaTakesPrecedenceOverSrc() throws IOException {
        // Both exist — src/main/java should win
        Path srcMainJava = Files.createDirectories(projectRoot.resolve("src/main/java"));
        // src exists implicitly as parent

        Path result = JavaCodeParser.detectSourceRoot(projectRoot);

        assertThat(result).isEqualTo(srcMainJava);
    }
}
