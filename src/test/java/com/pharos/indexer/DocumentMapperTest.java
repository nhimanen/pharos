package com.pharos.indexer;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class DocumentMapperTest {

    private static final String PROJECT = "test-project";

    // --- toDocument (method) ---

    @Test
    void toDocument_setsDocTypeMethod() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_DOC_TYPE)).isEqualTo("method");
    }

    @Test
    void toDocument_setsIdProjectClassMethod() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_ID)).isEqualTo(
                PROJECT + ":com.example.Calculator#add(int,int)");
        assertThat(doc.get(DocumentMapper.F_PROJECT)).isEqualTo(PROJECT);
        assertThat(doc.get(DocumentMapper.F_CLASS_NAME)).isEqualTo("Calculator");
        assertThat(doc.get(DocumentMapper.F_METHOD_NAME)).isEqualTo("add");
        assertThat(doc.get(DocumentMapper.F_RETURN_TYPE)).isEqualTo("int");
        assertThat(doc.get(DocumentMapper.F_ACCESS)).isEqualTo("public");
        assertThat(doc.get(DocumentMapper.F_PACKAGE)).isEqualTo("com.example");
    }

    @Test
    void toDocument_storesInDegreeAsStoredField() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 7, List.of());

        assertThat(doc.get(DocumentMapper.F_IN_DEGREE)).isEqualTo("7");
    }

    @Test
    void toDocument_storesCallerContext() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 2, List.of("compute", "execute"));

        assertThat(doc.get(DocumentMapper.F_CALLER_CONTEXT)).isEqualTo("compute execute");
    }

    @Test
    void toDocument_omitsCallerContextWhenEmpty() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_CALLER_CONTEXT)).isNull();
    }

    @Test
    void toDocument_storesJavadocWhenPresent() {
        Document doc = DocumentMapper.toDocument(
                methodWithJavadoc("Returns the sum of a and b."), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_JAVADOC)).isEqualTo("Returns the sum of a and b.");
    }

    @Test
    void toDocument_omitsJavadocFieldWhenAbsent() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_JAVADOC)).isNull();
    }

    @Test
    void toDocument_storesStartAndEndLine() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());

        assertThat(doc.get(DocumentMapper.F_START_LINE)).isEqualTo("10");
        assertThat(doc.get(DocumentMapper.F_END_LINE)).isEqualTo("12");
    }

    @Test
    void toDocument_twoArgOverload_usesZeroInDegree() {
        Document doc = DocumentMapper.toDocument(testMethod(), null);

        assertThat(doc.get(DocumentMapper.F_IN_DEGREE)).isEqualTo("0");
        assertThat(doc.get(DocumentMapper.F_DOC_TYPE)).isEqualTo("method");
    }

    // --- toClassDocument ---

    @Test
    void toClassDocument_setsDocTypeClass() {
        Document doc = DocumentMapper.toClassDocument(testClass(), "body", null);

        assertThat(doc.get(DocumentMapper.F_DOC_TYPE)).isEqualTo("class");
    }

    @Test
    void toClassDocument_setsClassId() {
        Document doc = DocumentMapper.toClassDocument(testClass(), "", null);

        assertThat(doc.get(DocumentMapper.F_ID)).isEqualTo(PROJECT + ":com.example.Calculator");
    }

    @Test
    void toClassDocument_storesSynthesizedBodyAsF_BODY() {
        String body = "public int add(int a, int b)\n  // Adds two integers\n";
        Document doc = DocumentMapper.toClassDocument(testClass(), body, null);

        assertThat(doc.get(DocumentMapper.F_BODY)).isEqualTo(body);
    }

    @Test
    void toClassDocument_storesClassNameAsMethodName_forBoostCompatibility() {
        Document doc = DocumentMapper.toClassDocument(testClass(), "", null);

        assertThat(doc.get(DocumentMapper.F_METHOD_NAME)).isEqualTo("Calculator");
    }

    @Test
    void toClassDocument_storesInDegreeAsZero() {
        Document doc = DocumentMapper.toClassDocument(testClass(), "", null);

        assertThat(doc.get(DocumentMapper.F_IN_DEGREE)).isEqualTo("0");
    }

    @Test
    void toClassDocument_storesJavadocWhenPresent() {
        ParsedClass cls = classWithJavadoc("A calculator class.");
        Document doc = DocumentMapper.toClassDocument(cls, "", null);

        assertThat(doc.get(DocumentMapper.F_JAVADOC)).isEqualTo("A calculator class.");
    }

    // --- buildEmbeddingText ---

    @Test
    void buildEmbeddingText_includesSignatureAndBody() {
        String text = DocumentMapper.buildEmbeddingText(testMethod());

        assertThat(text).contains("public int add(int a, int b)");
        assertThat(text).contains("return a + b;");
    }

    @Test
    void buildEmbeddingText_includesJavadocWhenPresent() {
        String text = DocumentMapper.buildEmbeddingText(methodWithJavadoc("Adds two numbers."));

        assertThat(text).contains("Adds two numbers.");
    }

    @Test
    void buildEmbeddingText_truncatesLongBody() {
        String longBody = "x".repeat(9000);
        ParsedMethod method = new ParsedMethod(
                PROJECT + ":com.example.Foo#bar()", PROJECT, "com.example",
                "Foo", "com.example.Foo", "bar", "public void bar()", "void",
                List.of(), List.of(), longBody, null, List.of(), "public",
                false, false, false, false, List.of(), List.of(),
                "/src/Foo.java", 1, 10);

        String text = DocumentMapper.buildEmbeddingText(method);

        assertThat(text).contains("...");
    }

    // --- buildClassEmbeddingText ---

    @Test
    void buildClassEmbeddingText_includesClassNameAndBody() {
        String text = DocumentMapper.buildClassEmbeddingText(testClass(), "int add(int a, int b)\n");

        assertThat(text).contains("Calculator");
        assertThat(text).contains("int add(int a, int b)");
    }

    // --- helpers ---

    private static ParsedMethod testMethod() {
        return new ParsedMethod(
                PROJECT + ":com.example.Calculator#add(int,int)",
                PROJECT, "com.example", "Calculator", "com.example.Calculator",
                "add", "public int add(int a, int b)", "int",
                List.of("int", "int"), List.of("a", "b"),
                "return a + b;", null, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/Calculator.java", 10, 12
        );
    }

    private static ParsedMethod methodWithJavadoc(String javadoc) {
        return new ParsedMethod(
                PROJECT + ":com.example.Calculator#add(int,int)",
                PROJECT, "com.example", "Calculator", "com.example.Calculator",
                "add", "public int add(int a, int b)", "int",
                List.of("int", "int"), List.of("a", "b"),
                "return a + b;", javadoc, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/Calculator.java", 10, 12
        );
    }

    // --- computeScope ---

    @Test
    void computeScope_prodForMainJava() {
        assertThat(DocumentMapper.computeScope("/src/main/java/com/example/Foo.java", false)).isEqualTo("prod");
    }

    @Test
    void computeScope_testForSrcTestPath() {
        assertThat(DocumentMapper.computeScope("/src/test/java/com/example/FooTest.java", false)).isEqualTo("test");
    }

    @Test
    void computeScope_testForBenchmarkPath() {
        assertThat(DocumentMapper.computeScope("/benchmark/com/example/Bench.java", false)).isEqualTo("test");
    }

    @Test
    void computeScope_docsForMarkdown() {
        assertThat(DocumentMapper.computeScope("/README.md", false)).isEqualTo("docs");
    }

    @Test
    void computeScope_docsForChunkAnnotation() {
        assertThat(DocumentMapper.computeScope("/src/main/java/Foo.java", true)).isEqualTo("docs");
    }

    // --- classType field ---

    @Test
    void toClassDocument_classType_concreteClass() {
        ParsedClass cls = new ParsedClass(
                PROJECT, "com.example", "Calculator", "com.example.Calculator",
                "class", null, List.of(), List.of(),
                "public", false /*isAbstract*/, false, null, "/src/C.java", 1, 10);
        Document doc = DocumentMapper.toClassDocument(cls, "body", null);
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isEqualTo("class");
    }

    @Test
    void toClassDocument_classType_abstractClass() {
        ParsedClass cls = new ParsedClass(
                PROJECT, "com.example", "BaseProcessor", "com.example.BaseProcessor",
                "class", null, List.of(), List.of(),
                "public", true /*isAbstract*/, false, null, "/src/BP.java", 1, 20);
        Document doc = DocumentMapper.toClassDocument(cls, "body", null);
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isEqualTo("abstract");
    }

    @Test
    void toClassDocument_classType_interface() {
        ParsedClass cls = new ParsedClass(
                PROJECT, "com.example", "Collector", "com.example.Collector",
                "interface", null, List.of(), List.of(),
                "public", false, false, null, "/src/Collector.java", 1, 5);
        Document doc = DocumentMapper.toClassDocument(cls, "body", null);
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isEqualTo("interface");
    }

    @Test
    void toClassDocument_classType_enum() {
        ParsedClass cls = new ParsedClass(
                PROJECT, "com.example", "Status", "com.example.Status",
                "enum", null, List.of(), List.of(),
                "public", false, false, null, "/src/Status.java", 1, 8);
        Document doc = DocumentMapper.toClassDocument(cls, "body", null);
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isEqualTo("enum");
    }

    @Test
    void toClassDocument_classType_record() {
        ParsedClass cls = new ParsedClass(
                PROJECT, "com.example", "Point", "com.example.Point",
                "record", null, List.of(), List.of(),
                "public", false, false, null, "/src/Point.java", 1, 3);
        Document doc = DocumentMapper.toClassDocument(cls, "body", null);
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isEqualTo("record");
    }

    @Test
    void toDocument_method_hasNoClassTypeField() {
        Document doc = DocumentMapper.toDocument(testMethod(), null, 0, List.of());
        // method documents must not carry a classType — field is class-only
        assertThat(doc.get(DocumentMapper.F_CLASS_TYPE)).isNull();
    }

    private static ParsedClass testClass() {
        return new ParsedClass(
                PROJECT, "com.example", "Calculator", "com.example.Calculator",
                "class", null, List.of(), List.of(),
                "public", false, false, null,
                "/src/Calculator.java", 1, 50
        );
    }

    private static ParsedClass classWithJavadoc(String javadoc) {
        return new ParsedClass(
                PROJECT, "com.example", "Calculator", "com.example.Calculator",
                "class", null, List.of(), List.of(),
                "public", false, false, javadoc,
                "/src/Calculator.java", 1, 50
        );
    }
}
