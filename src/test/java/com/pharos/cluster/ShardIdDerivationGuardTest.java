package com.pharos.cluster;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ruling P6-R8: the project of a shard comes from the catalog mapping
 * ({@link com.pharos.cluster.sharding.ProjectShards}), never from the shard id string. Any code that
 * splits, searches, strips or prefixes on the separator needs a tilde literal somewhere, so main code
 * outside {@code ShardId} and {@code ProjectShards} may contain <b>no</b> tilde char literal and no
 * string or text-block literal containing a tilde (also written as a unicode or octal escape).
 * Comments are ignored. A few unrelated lines are allowed by file and exact content.
 */
class ShardIdDerivationGuardTest {

    private static final Set<String> ALLOWED_FILES = Set.of(
            "com/pharos/cluster/ShardId.java",
            "com/pharos/cluster/sharding/ProjectShards.java");

    /**
     * Unrelated tildes, allowed by file and exact line content (any other tilde literal in these files
     * still fails): Go generic constraints, markdown fences and RST heading characters, "approximately"
     * in CLI output, and the home directory in messages.
     */
    private static final Map<String, Set<String>> ALLOWED_LINES = Map.of(
            "com/pharos/parser/GoCodeParser.java",
            Set.of("if (!decl.isEmpty() && !decl.contains(\"|\") && !decl.contains(\"~\")) {"),
            "com/pharos/parser/GenericFileParser.java",
            Set.of("if (line.startsWith(\"```\") || line.startsWith(\"~~~\")) {",
                    "private static final String RST_CHARS = \"=-~^\\\"'`#*+<>\";"),
            "com/pharos/cli/SearchCommand.java",
            Set.of("String tag = \"related\".equals(r.searchType()) ? \" ~related\" : \"\";"),
            "com/pharos/cli/IndexCommand.java",
            Set.of("return \"ETA ~\" + etaStr + \"  elapsed \" + elapsed(startMs);"),
            "com/pharos/embedding/EmbeddingProvider.java",
            Set.of("\"Add an entry to 'embeddingProviders' in ~/.pharos/config.json to enable.\");"),
            "com/pharos/indexer/EmbeddingCacheBackfiller.java",
            Set.of("System.err.println(\"embeddingDimensions not configured in ~/.pharos/config.json — cannot backfill.\");"),
            "com/pharos/indexer/MultiModelEmbedder.java",
            Set.of("\"Add an entry to ~/.pharos/config.json first.\"));"));

    /** A tilde, its unicode escape (backslash, one or more u, 007e) or its octal escape (backslash 176). */
    private static final Pattern TILDE = Pattern.compile("~|\\\\u+007[eE]|\\\\176");

    @Test
    void noCodeDerivesAProjectFromAShardId() throws IOException {
        // the app, the pharos-api module (ShardId, ClusterConfig, the sharding spec) and the operator
        List<Path> roots = List.of(Path.of("src/main/java"), Path.of("pharos-api/src/main/java"),
                Path.of("operator/src/main/java"));
        List<String> offenders = new ArrayList<>();
        for (Path root : roots) {
            assertThat(root).isDirectory();
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    String rel = root.relativize(f).toString().replace('\\', '/');
                    if (ALLOWED_FILES.contains(rel)) continue;
                    List<String> lines = Files.readAllLines(f);
                    for (int line : tildeLiteralLines(String.join("\n", lines))) {
                        String text = lines.get(line - 1).strip();
                        if (ALLOWED_LINES.getOrDefault(rel, Set.of()).contains(text)) continue;
                        offenders.add(root + "/" + rel + ":" + line + ": " + text);
                    }
                }
            }
        }
        assertThat(offenders).as("use ProjectShards.projectOf(shard) instead of parsing the shard id").isEmpty();
    }

    @Test
    void theGuardCatchesTheUsualForms() {
        List<String> bad = List.of(
                "String p = id.value().split(\"~\")[0];",
                "int i = s.indexOf('~');",
                "s.lastIndexOf(\"~\")",
                "v.split(Pattern.quote(\"~\"))",
                "v.split(\"~ring=\")",
                "Pattern.compile(\"~\").split(v)",
                "boolean mine = shard.value().startsWith(p + \"~\");",
                "if (shard.value().contains(\"~\")) {}",
                "String project = id.value().replaceFirst(\"~.*\", \"\");",
                "static final String SEP = \"~\";",
                "static final char SEP = '~';",
                "int i = s.indexOf('\\u007e');",
                "int i = s.indexOf(\"\\176\");",
                "String t = \"\"\"\n    a~b\n    \"\"\";");
        for (String code : bad) {
            assertThat(tildeLiteralLines(code)).as(code).isNotEmpty();
        }
        // a SEP constant used with indexOf is caught where it is declared
        assertThat(tildeLiteralLines("class X {\n  static final String SEP = \"~\";\n  int f(String s) { return s.indexOf(SEP); }\n}"))
                .containsExactly(2);
        // comments, javadoc and literals without a tilde are fine
        for (String code : List.of("// lucene~ring=s03", "/* p~module=core */ int x = 1;",
                "/** {@code lucene~ring=s03} */", "String s = \"a // b\" + \"c\";",
                "char c = '\"'; String s = \"it's\";", "String s = \"\\\"\"; // ~")) {
            assertThat(tildeLiteralLines(code)).as(code).isEmpty();
        }
        // the allowed parser lines are still literals with a tilde: allowed only by exact content
        ALLOWED_LINES.values().forEach(set -> set.forEach(l -> assertThat(tildeLiteralLines(l)).as(l).isNotEmpty()));
    }

    /** The 1-based lines of char, string and text-block literals that contain a tilde; comments are skipped. */
    static List<Integer> tildeLiteralLines(String src) {
        List<Integer> out = new ArrayList<>();
        int line = 1;
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '\n') {
                line++;
                i++;
            } else if (src.startsWith("//", i)) {
                while (i < n && src.charAt(i) != '\n') i++;
            } else if (src.startsWith("/*", i)) {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                line += count(src, i, end);
                i = end;
            } else if (src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                end = end < 0 ? n : end + 3;
                check(src.substring(i, end), line, out);
                line += count(src, i, end);
                i = end;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && src.charAt(j) != c && src.charAt(j) != '\n') j += src.charAt(j) == '\\' ? 2 : 1;
                int end = Math.min(n, j + 1);
                check(src.substring(i, end), line, out);
                i = end;
            } else {
                i++;
            }
        }
        return out;
    }

    private static void check(String literal, int line, List<Integer> out) {
        if (TILDE.matcher(literal).find() && !out.contains(line)) out.add(line);
    }

    private static int count(String s, int from, int to) {
        int k = 0;
        for (int i = from; i < to; i++) if (s.charAt(i) == '\n') k++;
        return k;
    }
}
