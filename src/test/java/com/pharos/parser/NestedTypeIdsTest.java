package com.pharos.parser;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Members of nested types, anonymous classes and enum-constant bodies must be attributed to
 * their own type exactly once. Previously the method visitor walked the whole type subtree, so
 * e.g. {@code Outer.Inner()} was indexed both as {@code Outer.Inner#<init>()} and as
 * {@code Outer#<init>()}, producing duplicate, mis-owned document ids.
 */
class NestedTypeIdsTest {

    @TempDir Path tmp;

    private static final String SRC = """
            package com.example;

            import java.util.Comparator;

            public class Outer {
                private final Comparator<String> cmp = new Comparator<String>() {
                    @Override public int compare(String a, String b) { return 0; }
                };

                public Outer() {}
                public String toString() { return "outer"; }

                public static class Inner {
                    public Inner() {}
                    public String toString() { return "inner"; }
                }

                public enum Kind {
                    A { @Override int code() { return 1; } },
                    B { @Override int code() { return 2; } };

                    abstract int code();

                    static class Helper { void help() {} }
                }

                public record Point(int x) {
                    public int twice() { return x * 2; }
                }
            }
            """;

    private ParsedFile parse() throws Exception {
        Path file = tmp.resolve("src/main/java/com/example/Outer.java");
        Files.createDirectories(file.getParent());
        Files.writeString(file, SRC);
        return new JavaCodeParser().parseFile(file, "p");
    }

    @Test
    void everyMethodIdIsUniqueAndOwnedByItsDeclaringType() throws Exception {
        List<String> ids = parse().methods().stream().map(ParsedMethod::id).toList();

        assertThat(ids).doesNotHaveDuplicates();
        assertThat(ids).containsExactlyInAnyOrder(
                "p:com.example.Outer#<init>()",
                "p:com.example.Outer#toString()",
                "p:com.example.Outer.Inner#<init>()",
                "p:com.example.Outer.Inner#toString()",
                "p:com.example.Outer.Kind#code()",
                "p:com.example.Outer.Kind.Helper#help()",
                "p:com.example.Outer.Point#twice()");
    }

    @Test
    void methodLinesMatchTheirOwner() throws Exception {
        ParsedFile f = parse();
        ParsedMethod innerCtor = f.methods().stream()
                .filter(m -> m.id().equals("p:com.example.Outer.Inner#<init>()")).findFirst().orElseThrow();
        ParsedMethod outerCtor = f.methods().stream()
                .filter(m -> m.id().equals("p:com.example.Outer#<init>()")).findFirst().orElseThrow();
        assertThat(innerCtor.startLine()).isNotEqualTo(outerCtor.startLine());
        assertThat(innerCtor.signature()).contains("Inner()");
        assertThat(outerCtor.signature()).contains("Outer()");
    }

    @Test
    void nestedEnumsRecordsAndTheirNestedTypesAreFullyQualified() throws Exception {
        List<String> classes = parse().classes().stream().map(ParsedClass::qualifiedClassName).toList();

        assertThat(classes).doesNotHaveDuplicates();
        assertThat(classes).contains(
                "com.example.Outer",
                "com.example.Outer.Inner",
                "com.example.Outer.Kind",
                "com.example.Outer.Kind.Helper",
                "com.example.Outer.Point");
    }
}
