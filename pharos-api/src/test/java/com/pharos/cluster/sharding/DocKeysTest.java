package com.pharos.cluster.sharding;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class DocKeysTest {

    @Test
    void normalisesToForwardSlashRelativePaths() {
        assertThat(DocKeys.normalizePath("src/A.java")).isEqualTo("src/A.java");
        assertThat(DocKeys.normalizePath("src\\main\\A.java")).isEqualTo("src/main/A.java");
        assertThat(DocKeys.normalizePath("./src/A.java")).isEqualTo("src/A.java");
        assertThat(DocKeys.normalizePath("././src//./A.java/")).isEqualTo("src/A.java");
        assertThat(DocKeys.normalizePath(".github/ci.yml")).isEqualTo(".github/ci.yml");
        assertThat(DocKeys.normalizePath("a:b.txt")).isEqualTo("a:b.txt");
    }

    @Test
    void preservesCaseAndCharacters() {
        assertThat(DocKeys.normalizePath("Src/ReadMe.MD")).isEqualTo("Src/ReadMe.MD");
        assertThat(DocKeys.normalizePath("docs/a b/ü.md")).isEqualTo("docs/a b/ü.md");
    }

    @Test
    void rejectsPathsOutsideTheProject() {
        for (String bad : new String[]{"/etc/passwd", "\\abs", "C:/x.java", "c:\\x.java", "C:", "../x", "a/../../x", "", ".", "./"}) {
            assertThatThrownBy(() -> DocKeys.normalizePath(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> DocKeys.normalizePath((String) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void normalisesAgainstAProjectRoot(@TempDir Path root) {
        assertThat(DocKeys.normalizePath(root, root.resolve("src").resolve("main").resolve("A.java"))).isEqualTo("src/main/A.java");
        assertThat(DocKeys.normalizePath(root.resolve("."), root.resolve("x/./y.txt"))).isEqualTo("x/y.txt");
        assertThatThrownBy(() -> DocKeys.normalizePath(root.resolve("sub"), root.resolve("y.txt")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorNormalises() {
        DocKeys k = new DocKeys("p", ".\\a\\b.go", "m", "go", "code");
        assertThat(k.relativePath()).isEqualTo("a/b.go");
        assertThat(DocKeys.of("p", "a/b.go")).isEqualTo(new DocKeys("p", "a/b.go", null, null, null));
    }
}
