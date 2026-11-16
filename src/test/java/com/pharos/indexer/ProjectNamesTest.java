package com.pharos.indexer;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProjectNamesTest {

    private static final Path INDEX = Path.of("/data/indexes");

    @Test
    void rejectsDotEmptyAndSeparators() {
        for (String bad : new String[]{null, "", " ", ".", "..", "a/b", "a\\b", "/abs", "../x"}) {
            assertThatThrownBy(() -> ProjectNames.validate(bad))
                    .as("name %s", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("--project");
            assertThat(ProjectNames.isValid(bad)).isFalse();
        }
    }

    @Test
    void acceptsOrdinaryNamesIncludingSpacesAndDots() {
        for (String ok : new String[]{"my-app", "lucene", "my app", "a.b", "..hidden", "x_y+z"}) {
            assertThatCode(() -> ProjectNames.validate(ok, INDEX)).as(ok).doesNotThrowAnyException();
        }
    }

    @Test
    void indexDirMustBeStrictDirectChild() {
        assertThatThrownBy(() -> ProjectNames.validate(".", INDEX))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ProjectNames.resolveProjectDir(INDEX, "my-app")).isEqualTo(INDEX.resolve("my-app"));
        assertThatThrownBy(() -> ProjectNames.resolveProjectDir(INDEX, ".."))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
