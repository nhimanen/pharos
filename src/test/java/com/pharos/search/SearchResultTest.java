package com.pharos.search;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SearchResultTest {

    @Test
    void label_forMethodDocType_returnsQualifiedClassHashMethod() {
        SearchResult result = makeResult("com.example.Calculator", "add", "method");

        assertThat(result.label()).isEqualTo("com.example.Calculator#add");
    }

    @Test
    void label_forClassDocType_returnsQualifiedClassNameOnly() {
        SearchResult result = makeResult("com.example.Calculator", "Calculator", "class");

        assertThat(result.label()).isEqualTo("com.example.Calculator");
    }

    @Test
    void label_forNullDocType_treatedAsMethod() {
        SearchResult result = new SearchResult(
                "proj:com.example.Foo#bar()", "proj", "com.example",
                "Foo", "com.example.Foo", "bar",
                "public void bar()", "void", "body", null,
                "public", "/src/Foo.java", 1, 5,
                1.0f, "keyword", null
        );

        // null docType → not "class" → method label format
        assertThat(result.label()).isEqualTo("com.example.Foo#bar");
    }

    @Test
    void score_returnsCorrectValue() {
        SearchResult result = makeResult("com.example.Foo", "bar", "method");

        assertThat(result.score()).isEqualTo(2.5f);
    }

    @Test
    void searchType_returnsSearchType() {
        SearchResult result = new SearchResult(
                "id", "proj", "pkg", "Cls", "pkg.Cls", "method",
                "sig", "void", "body", null, "public", "/f.java",
                1, 5, 1.0f, "related", "method"
        );

        assertThat(result.searchType()).isEqualTo("related");
    }

    @Test
    void docType_returnsDocType() {
        SearchResult method = makeResult("com.example.Foo", "bar", "method");
        SearchResult cls    = makeResult("com.example.Foo", "Foo",  "class");

        assertThat(method.docType()).isEqualTo("method");
        assertThat(cls.docType()).isEqualTo("class");
    }

    private static SearchResult makeResult(String qualifiedClass, String methodName, String docType) {
        return new SearchResult(
                "proj:" + qualifiedClass + "#" + methodName + "()",
                "proj", "com.example", methodName, qualifiedClass,
                methodName, "public void " + methodName + "()",
                "void", "body", null, "public",
                "/src/" + methodName + ".java", 1, 10,
                2.5f, "keyword", docType
        );
    }
}
