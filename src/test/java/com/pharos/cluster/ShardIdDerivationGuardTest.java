package com.pharos.cluster;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ruling P6-R8: the project of a shard comes from the catalog mapping
 * ({@link com.pharos.cluster.sharding.ProjectShards}), never from the shard id string. Fails when main
 * code outside {@code ShardId} and {@code ProjectShards} splits or searches a string on {@code '~'},
 * the shard id separator.
 */
class ShardIdDerivationGuardTest {

    private static final Set<String> ALLOWED = Set.of(
            "com/pharos/cluster/ShardId.java",
            "com/pharos/cluster/sharding/ProjectShards.java");

    /** split("~"), split(Pattern.quote("~")), indexOf('~'), lastIndexOf("~"), and a Pattern compiled from "~". */
    private static final Pattern DERIVES = Pattern.compile(
            "(split|indexOf|lastIndexOf)\\(\\s*('~'|\"(\\\\\\\\)?~[^\"]*\"|Pattern\\.quote\\(\\s*\"~[^\"]*\"\\s*\\))"
                    + "|Pattern\\.compile\\(\\s*\"[^\"]*~[^\"]*\"\\s*\\)\\.split");

    @Test
    void noCodeDerivesAProjectFromAShardId() throws IOException {
        Path root = Path.of("src/main/java");
        assertThat(root).isDirectory();
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String rel = root.relativize(f).toString().replace('\\', '/');
                if (ALLOWED.contains(rel)) continue;
                List<String> lines = Files.readAllLines(f);
                for (int i = 0; i < lines.size(); i++) {
                    if (DERIVES.matcher(lines.get(i)).find()) offenders.add(rel + ":" + (i + 1) + ": " + lines.get(i).strip());
                }
            }
        }
        assertThat(offenders).as("use ProjectShards.projectOf(shard) instead of parsing the shard id").isEmpty();
    }

    @Test
    void theGuardCatchesTheUsualForms() {
        for (String bad : List.of("String p = id.value().split(\"~\")[0];", "int i = s.indexOf('~');",
                "s.lastIndexOf(\"~\")", "v.split(Pattern.quote(\"~\"))", "v.split(\"~ring=\")",
                "Pattern.compile(\"~\").split(v)")) {
            assertThat(DERIVES.matcher(bad).find()).as(bad).isTrue();
        }
        assertThat(DERIVES.matcher("decl.contains(\"~\")").find()).isFalse();
        assertThat(DERIVES.matcher("line.startsWith(\"~~~\")").find()).isFalse();
    }
}
