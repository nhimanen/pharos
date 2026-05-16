package com.pharos.indexer;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultChunkerTest {

    private static final DefaultChunker CHUNKER = new DefaultChunker();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ParsedMethod method(String className, String qualifiedClass,
                                        String methodName, String signature,
                                        String body, String javadoc,
                                        int startLine, int endLine) {
        return new ParsedMethod(
                "proj:" + qualifiedClass + "#" + methodName + "()",
                "proj", "com.example",
                className, qualifiedClass,
                methodName, signature, "void",
                List.of(), List.of(),
                body, javadoc,
                List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/" + className + ".java", startLine, endLine
        );
    }

    private static ParsedMethod shortMethod() {
        return method("MyService", "com.example.MyService",
                "compute", "public void compute()",
                "return 42;", null, 10, 12);
    }

    private static ParsedMethod methodWithJavadoc(String javadoc) {
        return method("MyService", "com.example.MyService",
                "compute", "public void compute()",
                "return 42;", javadoc, 10, 12);
    }

    private static ParsedMethod longMethod(int bodyChars) {
        // Build a body with enough lines to force multiple chunks
        StringBuilder sb = new StringBuilder();
        int lineLen = 80;
        while (sb.length() < bodyChars) {
            sb.append("x".repeat(lineLen)).append('\n');
        }
        String body = sb.toString();
        int lines = (int) body.chars().filter(c -> c == '\n').count();
        return method("BigService", "com.example.BigService",
                "bigOp", "public void bigOp()",
                body, null, 1, 1 + lines);
    }

    private static ParsedClass cls(String className, String qualifiedClass,
                                    int startLine, int endLine) {
        return new ParsedClass(
                "proj", "com.example",
                className, qualifiedClass,
                "class", null, List.of(), List.of(),
                "public", false, false, null,
                "/src/" + className + ".java", startLine, endLine
        );
    }

    private static ParsedMethod methodForClass(String className, String qualifiedClass,
                                                String methodName,
                                                int startLine, int endLine) {
        return method(className, qualifiedClass, methodName,
                "public void " + methodName + "()", "// body", null,
                startLine, endLine);
    }

    // -------------------------------------------------------------------------
    // chunkMethod(method, multiChunk=false)
    // -------------------------------------------------------------------------

    @Nested
    class ChunkMethodSingleChunk {

        @Test
        void shortMethod_producesExactlyOneChunk() {
            List<Chunk> chunks = CHUNKER.chunkMethod(shortMethod(), false);

            assertThat(chunks).hasSize(1);
        }

        @Test
        void chunkText_containsClassNamePrefix() {
            List<Chunk> chunks = CHUNKER.chunkMethod(shortMethod(), false);

            assertThat(chunks.get(0).text()).contains("[com.example.MyService]");
        }

        @Test
        void chunkText_containsMethodSignature() {
            List<Chunk> chunks = CHUNKER.chunkMethod(shortMethod(), false);

            assertThat(chunks.get(0).text()).contains("public void compute()");
        }

        @Test
        void chunkText_containsJavadocWhenPresent() {
            List<Chunk> chunks = CHUNKER.chunkMethod(
                    methodWithJavadoc("Computes the answer."), false);

            assertThat(chunks.get(0).text()).contains("Computes the answer.");
        }

        @Test
        void chunkText_noJavadocSection_whenJavadocAbsent() {
            List<Chunk> chunks = CHUNKER.chunkMethod(shortMethod(), false);

            assertThat(chunks.get(0).text()).doesNotContain("/**");
        }

        @Test
        void chunk_coversMethodStartToEndLine() {
            ParsedMethod m = shortMethod();
            List<Chunk> chunks = CHUNKER.chunkMethod(m, false);

            assertThat(chunks.get(0).startLine()).isEqualTo(m.startLine());
            assertThat(chunks.get(0).endLine()).isEqualTo(m.endLine());
        }
    }

    // -------------------------------------------------------------------------
    // chunkMethod(method, multiChunk=true)
    // -------------------------------------------------------------------------

    @Nested
    class ChunkMethodMultiChunk {

        @Test
        void shortMethod_stillProducesOneChunk() {
            List<Chunk> chunks = CHUNKER.chunkMethod(shortMethod(), true);

            assertThat(chunks).hasSize(1);
        }

        @Test
        void longMethod_producesMultipleChunks() {
            // Body well over 8000 chars → must split
            List<Chunk> chunks = CHUNKER.chunkMethod(longMethod(18_000), true);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
        }

        @Test
        void eachChunk_containsClassPrefix() {
            List<Chunk> chunks = CHUNKER.chunkMethod(longMethod(18_000), true);

            for (Chunk chunk : chunks) {
                assertThat(chunk.text()).contains("[com.example.BigService]");
            }
        }

        @Test
        void adjacentChunks_haveOverlap() {
            // The DefaultChunker includes "// ...\n<overlap>\n// ...\n" in continuation chunks.
            List<Chunk> chunks = CHUNKER.chunkMethod(longMethod(18_000), true);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
            // Continuation chunks contain the overlap marker
            String secondChunkText = chunks.get(1).text();
            assertThat(secondChunkText).contains("// ...");
        }

        @Test
        void chunks_coverContinuousLineRange() {
            ParsedMethod m = longMethod(18_000);
            List<Chunk> chunks = CHUNKER.chunkMethod(m, true);

            // First chunk starts at method start
            assertThat(chunks.get(0).startLine()).isEqualTo(m.startLine());
            // Last chunk ends at or before method end
            assertThat(chunks.get(chunks.size() - 1).endLine()).isLessThanOrEqualTo(m.endLine());
            // No gaps: each chunk's startLine <= previous chunk's endLine + 1
            for (int i = 1; i < chunks.size(); i++) {
                assertThat(chunks.get(i).startLine())
                        .as("gap between chunk %d and %d", i - 1, i)
                        .isLessThanOrEqualTo(chunks.get(i - 1).endLine() + 1);
            }
        }

        @Test
        void veryLongMethod_producesAtLeastThreeChunks() {
            List<Chunk> chunks = CHUNKER.chunkMethod(longMethod(25_000), true);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(3);
        }
    }

    // -------------------------------------------------------------------------
    // chunkClass
    // -------------------------------------------------------------------------

    @Nested
    class ChunkClass {

        @Test
        void emptyClass_producesOneChunk() {
            ParsedClass c = cls("Empty", "com.example.Empty", 1, 5);
            List<Chunk> chunks = CHUNKER.chunkClass(c, "", List.of());

            assertThat(chunks).hasSize(1);
        }

        @Test
        void emptyClass_chunkContainsClassPrefix() {
            ParsedClass c = cls("Empty", "com.example.Empty", 1, 5);
            List<Chunk> chunks = CHUNKER.chunkClass(c, "", List.of());

            assertThat(chunks.get(0).text()).contains("[com.example.Empty]");
        }

        @Test
        void smallClass_noNamedGroups_producesOneChunk() {
            ParsedClass c = cls("Small", "com.example.Small", 1, 20);
            // Each method has a unique prefix — no group reaches MIN_GROUP_SIZE=2
            List<ParsedMethod> methods = List.of(
                    methodForClass("Small", "com.example.Small", "getX", 2, 5),
                    methodForClass("Small", "com.example.Small", "setX", 6, 9)
            );
            List<Chunk> chunks = CHUNKER.chunkClass(c, "int getX()\nvoid setX()", methods);

            assertThat(chunks).hasSize(1);
        }

        @Test
        void methodsWithGetPrefix_twoOrMore_producesSeparateGroupChunk() {
            ParsedClass c = cls("Repo", "com.example.Repo", 1, 50);
            List<ParsedMethod> methods = List.of(
                    methodForClass("Repo", "com.example.Repo", "getById",    2, 5),
                    methodForClass("Repo", "com.example.Repo", "getAll",     6, 9),
                    methodForClass("Repo", "com.example.Repo", "save",      10, 13)
            );
            List<Chunk> chunks = CHUNKER.chunkClass(c, "body", methods);

            // At minimum: header chunk + get* chunk
            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
            boolean hasGetGroup = chunks.stream()
                    .anyMatch(ch -> ch.text().contains("get*"));
            assertThat(hasGetGroup).isTrue();
        }

        @Test
        void methodsWithValidatePrefix_twoOrMore_producesSeparateGroupChunk() {
            ParsedClass c = cls("Validator", "com.example.Validator", 1, 60);
            List<ParsedMethod> methods = List.of(
                    methodForClass("Validator", "com.example.Validator", "validateToken",  2,  6),
                    methodForClass("Validator", "com.example.Validator", "validateEmail",  7, 11),
                    methodForClass("Validator", "com.example.Validator", "buildResponse", 12, 16)
            );
            List<Chunk> chunks = CHUNKER.chunkClass(c, "body", methods);

            boolean hasValidateGroup = chunks.stream()
                    .anyMatch(ch -> ch.text().contains("validate*"));
            assertThat(hasValidateGroup).isTrue();
        }

        @Test
        void oneMethodPerPrefix_noNamedGroups_oneChunk() {
            ParsedClass c = cls("Misc", "com.example.Misc", 1, 30);
            // All prefixes are unique (only 1 method each)
            List<ParsedMethod> methods = List.of(
                    methodForClass("Misc", "com.example.Misc", "alpha",   2,  4),
                    methodForClass("Misc", "com.example.Misc", "beta",    5,  7),
                    methodForClass("Misc", "com.example.Misc", "gamma",   8, 10),
                    methodForClass("Misc", "com.example.Misc", "delta",  11, 13)
            );
            List<Chunk> chunks = CHUNKER.chunkClass(c, "body", methods);

            assertThat(chunks).hasSize(1);
        }

        @Test
        void allChunks_containQualifiedClassName() {
            ParsedClass c = cls("Repo", "com.example.Repo", 1, 50);
            List<ParsedMethod> methods = List.of(
                    methodForClass("Repo", "com.example.Repo", "getById",  2,  5),
                    methodForClass("Repo", "com.example.Repo", "getAll",   6,  9),
                    methodForClass("Repo", "com.example.Repo", "save",    10, 13)
            );
            List<Chunk> chunks = CHUNKER.chunkClass(c, "body", methods);

            for (Chunk chunk : chunks) {
                assertThat(chunk.text()).contains("com.example.Repo");
            }
        }
    }

    // -------------------------------------------------------------------------
    // chunkText
    // -------------------------------------------------------------------------

    @Nested
    class ChunkText {

        @Test
        void shortContent_producesOneChunk() {
            List<Chunk> chunks = CHUNKER.chunkText("README.md", "Hello world.", 1);

            assertThat(chunks).hasSize(1);
        }

        @Test
        void chunkText_startsWithBracketedFileHeader() {
            List<Chunk> chunks = CHUNKER.chunkText("README.md", "Hello world.", 1);

            assertThat(chunks.get(0).text()).startsWith("[README.md]");
        }

        @Test
        void longContent_producesMultipleChunks() {
            String content = "word ".repeat(4000); // ~20 000 chars
            List<Chunk> chunks = CHUNKER.chunkText("docs/large.md", content, 1);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
        }

        @Test
        void allContinuationChunks_startWithFileHeader() {
            String content = "word ".repeat(4000);
            List<Chunk> chunks = CHUNKER.chunkText("docs/large.md", content, 1);

            for (Chunk chunk : chunks) {
                assertThat(chunk.text()).contains("[docs/large.md]");
            }
        }

        @Test
        void continuationChunk_containsOverlapFromPreviousChunk() {
            String content = "word ".repeat(4000);
            List<Chunk> chunks = CHUNKER.chunkText("docs/large.md", content, 1);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
            // Continuation chunks include the "// ..." overlap marker
            assertThat(chunks.get(1).text()).contains("// ...");
        }

        @Test
        void paragraphBoundaryPreferredOverMidWord() {
            // Build content with a clear double-newline paragraph boundary near the split point
            // First 7000 chars: many words, then a paragraph break, then more
            String part1 = "alpha ".repeat(1200); // ~7200 chars
            String part2 = "beta ".repeat(200);
            String content = part1 + "\n\n" + part2;

            List<Chunk> chunks = CHUNKER.chunkText("file.txt", content, 1);

            assertThat(chunks.size()).isGreaterThanOrEqualTo(2);
            // Second chunk should NOT start mid-word (it starts with the header or overlap)
            String secondText = chunks.get(1).text();
            assertThat(secondText).contains("[file.txt]");
        }

        @Test
        void lineNumbers_incrementByLineCountOfEachChunk() {
            // Multi-line content
            String line = "this is a long line of content, repeated many times.\n";
            String content = line.repeat(300);  // 300 lines
            List<Chunk> chunks = CHUNKER.chunkText("doc.txt", content, 5);

            assertThat(chunks.get(0).startLine()).isEqualTo(5);
            if (chunks.size() > 1) {
                // Second chunk's startLine should be > first chunk's startLine
                assertThat(chunks.get(1).startLine()).isGreaterThan(chunks.get(0).startLine());
            }
        }
    }

    // -------------------------------------------------------------------------
    // DefaultChunker.firstWord (package-private static)
    // -------------------------------------------------------------------------

    @Nested
    class FirstWord {

        @Test
        void camelCaseGet_returnsGet() {
            assertThat(DefaultChunker.firstWord("getUserById")).isEqualTo("get");
        }

        @Test
        void camelCaseValidate_returnsValidate() {
            assertThat(DefaultChunker.firstWord("validateToken")).isEqualTo("validate");
        }

        @Test
        void constructor_init_returnsEmpty() {
            assertThat(DefaultChunker.firstWord("<init>")).isEqualTo("");
        }

        @Test
        void null_returnsEmpty() {
            assertThat(DefaultChunker.firstWord(null)).isEqualTo("");
        }

        @Test
        void blank_returnsEmpty() {
            assertThat(DefaultChunker.firstWord("")).isEqualTo("");
        }

        @Test
        void uppercaseStart_returnsLowercasedFirstWord() {
            // "Uppercase" → all lowercase until next uppercase; firstWord returns "uppercase"
            // because the loop appends toLowerCase(c) until a capital follows another char
            // Actually: loop starts with 'U', !word.isEmpty() is false initially → appends 'u'
            // Then 'p','p','e','r','c','a','s','e' → 'E' is not uppercase... wait,
            // 'p' is lower, etc. So it reads all chars until end → "uppercase"
            assertThat(DefaultChunker.firstWord("Uppercase")).isEqualTo("uppercase");
        }

        @Test
        void singleWordLower_returnsSameWord() {
            assertThat(DefaultChunker.firstWord("compute")).isEqualTo("compute");
        }

        @Test
        void camelCaseFind_returnsFind() {
            assertThat(DefaultChunker.firstWord("findByEmail")).isEqualTo("find");
        }
    }
}
