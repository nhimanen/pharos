package com.pharos.parser;

import com.pharos.parser.model.ParsedFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression tests for JavaParser language-level configuration.
 *
 * Documents concrete failures that occurred with incorrect language levels:
 *
 * 1. {@code yield} in switch expressions (Java 14+): RAW mode treated yield as an
 *    identifier and failed to parse switch expressions that use it.
 *
 * 2. String literals in switch expression cases: same root cause — switch expression
 *    arrow cases with string case constants failed under RAW mode.
 *
 * 3. {@code _} as a variable name: JAVA_21 mode treats _ as a reserved keyword
 *    (JEP 443 unnamed variables), breaking files that used _ as a variable name.
 *    Handled by the RAW fallback pass.
 *
 * 4. Java 21 type patterns in switch (finalized in JEP 441): {@code case SomeType t ->}
 *    arms were parsed as JAVA_17 preview and rejected at the stable JAVA_17 level.
 *
 * The fix: two-pass strategy — JAVA_21 first (supports yield, type patterns, record
 * patterns, guarded patterns), then RAW fallback (handles {@code _} as identifier).
 */
class JavaCodeParserLanguageLevelTest {

    @TempDir
    Path tempDir;

    private final JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), 1);

    // -----------------------------------------------------------------------
    // 1. yield in switch expressions
    // -----------------------------------------------------------------------

    @Test
    void parsesYieldInSwitchExpression() throws Exception {
        Path file = writeJava("YieldSwitch.java", """
                package com.example;
                public class YieldSwitch {
                    public String describe(int n) {
                        return switch (n) {
                            case 1 -> "one";
                            case 2 -> "two";
                            default -> {
                                String result = "many: " + n;
                                yield result;
                            }
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "describe".equals(m.methodName()));
        assertThat(result.classes()).anyMatch(c -> "YieldSwitch".equals(c.className()));
    }

    @Test
    void parsesYieldInNestedSwitchExpression() throws Exception {
        Path file = writeJava("NestedYield.java", """
                package com.example;
                public class NestedYield {
                    public int compute(String op, int a, int b) {
                        return switch (op) {
                            case "add" -> a + b;
                            case "sub" -> a - b;
                            default -> {
                                int fallback = switch (op) {
                                    case "mul" -> a * b;
                                    default -> {
                                        yield 0;
                                    }
                                };
                                yield fallback;
                            }
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "compute".equals(m.methodName()));
    }

    // -----------------------------------------------------------------------
    // 2. String literals as switch case constants
    // -----------------------------------------------------------------------

    @Test
    void parsesStringLiteralsInSwitchExpressionCases() throws Exception {
        Path file = writeJava("StringSwitch.java", """
                package com.example;
                public class StringSwitch {
                    public String handle(String command, String task) {
                        return switch (command) {
                            case "add" -> {
                                if (task == null) {
                                    yield "Error: 'task' is required for add";
                                }
                                yield "Added: " + task;
                            }
                            case "remove" -> "Removed: " + task;
                            default -> "Unknown command: " + command;
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "handle".equals(m.methodName()));
    }

    @Test
    void parsesComplexSwitchWithYieldAndStringCases() throws Exception {
        // Mirrors the exact pattern that failed in FileAccessTool.java and CronTool.java
        Path file = writeJava("CronTool.java", """
                package dev.mypal.tools;
                public class CronTool {
                    public String execute(String action, String task) {
                        return switch (action) {
                            case "add" -> {
                                if (task == null || task.isBlank()) {
                                    yield "Error: 'task' is required for add";
                                }
                                yield scheduleCron(task);
                            }
                            case "list" -> listCrons();
                            case "remove" -> removeCron(task);
                            default -> {
                                yield "Unknown action: " + action;
                            }
                        };
                    }
                    private String scheduleCron(String t) { return "scheduled: " + t; }
                    private String listCrons() { return "[]"; }
                    private String removeCron(String t) { return "removed: " + t; }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods())
                .anyMatch(m -> "execute".equals(m.methodName()))
                .anyMatch(m -> "scheduleCron".equals(m.methodName()));
        assertThat(result.classes()).anyMatch(c -> "CronTool".equals(c.className()));
    }

    // -----------------------------------------------------------------------
    // 3. Underscore as a variable name (JAVA_21 forbids it; JAVA_17 allows it)
    // -----------------------------------------------------------------------

    @Test
    void parsesUnderscoreAsVariableName() throws Exception {
        Path file = writeJava("UnderscoreVar.java", """
                package com.example;
                public class UnderscoreVar {
                    public void process() {
                        String _ = "unused";
                        System.out.println(_);
                    }
                    public int compute(int _, int y) {
                        return y * 2;
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "process".equals(m.methodName()));
        assertThat(result.methods()).anyMatch(m -> "compute".equals(m.methodName()));
    }

    @Test
    void parsesUnderscoreAsFieldName() throws Exception {
        Path file = writeJava("UnderscoreField.java", """
                package com.example;
                public class UnderscoreField {
                    private final String _ = "";
                    public boolean isEmpty() {
                        return _.isEmpty();
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "isEmpty".equals(m.methodName()));
    }

    // -----------------------------------------------------------------------
    // 4. Combination: both yield and _ in same file
    // -----------------------------------------------------------------------

    @Test
    void parsesSwitchWithYieldAndUnderscoreVariable() throws Exception {
        Path file = writeJava("Combined.java", """
                package com.example;
                public class Combined {
                    public String classify(int value) {
                        String _ = "prefix";
                        return switch (value) {
                            case 0 -> _ + ":zero";
                            default -> {
                                String label = _ + ":other";
                                yield label;
                            }
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "classify".equals(m.methodName()));
    }

    // -----------------------------------------------------------------------
    // 4. Java 21 type patterns in switch (JEP 441)
    // -----------------------------------------------------------------------

    /**
     * Regression for the exact pattern that failed:
     * {@code case SomeType varName ->} arms require JAVA_21 level;
     * they were preview-only at JAVA_17 and rejected at stable JAVA_17.
     */
    @Test
    void parsesTypePatternsInSwitchExpression() throws Exception {
        Path file = writeJava("ContextBuilderTest.java", """
                package dev.mypal.agent;
                public class ContextBuilderTest {
                    interface Msg { String text(); }
                    record SystemMsg(String text) implements Msg {}
                    record UserMsg(String text)   implements Msg {}
                    record AiMsg(String text)     implements Msg {}

                    /** Mirrors the real failure: switch over a sealed-like hierarchy. */
                    private String textOf(Msg msg) {
                        return switch (msg) {
                            case SystemMsg sm -> sm.text();
                            case AiMsg    am -> am.text();
                            case UserMsg  um -> {
                                yield um.text().strip();
                            }
                            default -> msg.toString();
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "textOf".equals(m.methodName()));
        assertThat(result.classes()).anyMatch(c -> "ContextBuilderTest".equals(c.className()));
    }

    @Test
    void parsesGuardedPatternInSwitch() throws Exception {
        Path file = writeJava("ShapeArea.java", """
                package com.example;
                public class ShapeArea {
                    sealed interface Shape permits Circle, Rectangle {}
                    record Circle(double radius)              implements Shape {}
                    record Rectangle(double w, double h)     implements Shape {}

                    public double area(Shape s) {
                        return switch (s) {
                            case Circle c    when c.radius() > 0 -> Math.PI * c.radius() * c.radius();
                            case Rectangle r when r.w() > 0 && r.h() > 0 -> r.w() * r.h();
                            default -> 0.0;
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "area".equals(m.methodName()));
    }

    // -----------------------------------------------------------------------
    // 5. Arrow switch expressions without yield (baseline — must still work)
    // -----------------------------------------------------------------------

    @Test
    void parsesArrowSwitchExpressionWithoutYield() throws Exception {
        Path file = writeJava("ArrowSwitch.java", """
                package com.example;
                public class ArrowSwitch {
                    public int dayValue(String day) {
                        return switch (day) {
                            case "MON", "FRI", "SUN" -> 6;
                            case "TUE"               -> 7;
                            case "THU", "SAT"        -> 8;
                            case "WED"               -> 9;
                            default                  -> throw new IllegalArgumentException(day);
                        };
                    }
                }
                """);

        ParsedFile result = parser.parseFile(file, "test");

        assertThat(result.methods()).anyMatch(m -> "dayValue".equals(m.methodName()));
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private Path writeJava(String filename, String content) throws Exception {
        Path file = tempDir.resolve(filename);
        Files.writeString(file, content);
        return file;
    }
}
