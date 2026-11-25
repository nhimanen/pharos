package com.pharos.operator.api;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CRD in deploy/crd/ is the one the build generates from the {@code api} classes (fabric8
 * crd-generator, process-classes phase, into target/crd/). After changing the classes, refresh it with
 * {@code mvn -pl operator -am test -Dtest=CrdGenerationTest -Dpharos.regenerateCrd=true}.
 */
class CrdGenerationTest {

    private static final String CRD_FILE = "pharosclusters.pharos.dev-v1.yml";

    @Test
    void crdGeneratedMatchesCommitted() throws IOException {
        Path generatedDir = Path.of(System.getProperty("pharos.crdGenerated", "target/crd"));
        Path committedDir = Path.of(System.getProperty("pharos.crdCommitted", "../deploy/crd"));
        assertThat(files(generatedDir)).as("generated CRDs in %s", generatedDir).containsExactly(CRD_FILE);

        Path generated = generatedDir.resolve(CRD_FILE);
        Path committed = committedDir.resolve(CRD_FILE);
        if (Boolean.getBoolean("pharos.regenerateCrd")) {
            Files.createDirectories(committedDir);
            Files.copy(generated, committed, StandardCopyOption.REPLACE_EXISTING);
        }
        assertThat(files(committedDir)).as("committed CRDs in %s", committedDir).containsExactly(CRD_FILE);
        assertThat(Files.readString(committed))
                .as("%s is stale: regenerate it with -Dpharos.regenerateCrd=true (see this test's javadoc)", committed)
                .isEqualTo(Files.readString(generated));
    }

    @Test
    void crdIsNamespacedWithStatusSubresource() throws IOException {
        String crd = Files.readString(Path.of(System.getProperty("pharos.crdGenerated", "target/crd"), CRD_FILE));
        assertThat(crd).contains("group: \"pharos.dev\"", "kind: \"PharosCluster\"", "scope: \"Namespaced\"",
                "name: \"v1alpha1\"", "subresources:\n      status: {}");
    }

    private static List<String> files(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }
}
