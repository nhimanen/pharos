package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PassageBuilderTest {

    private static SearchResult method(String cls, String name, String javadoc) {
        return new SearchResult("p:x", "p", "pkg", cls, "pkg." + cls, name,
                "void " + name + "()", "void", "body", javadoc, "public",
                "/X.java", 1, 2, 1f, "hybrid", "method");
    }

    @Test
    void methodPassageStartsWithClassDotMethod() {
        String p = PassageBuilder.build(method("LuceneIndexer", "getProjectIndexDir", "Returns the dir."));
        assertThat(p.lines().findFirst().orElse("")).isEqualTo("LuceneIndexer.getProjectIndexDir");
        assertThat(p).contains("Returns the dir.");
    }

    @Test
    void classDocPassageStartsWithClassNameOnly() {
        SearchResult cls = new SearchResult("p:c", "p", "pkg", "LuceneIndexer", "pkg.LuceneIndexer",
                null, null, null, "members", "Manages indexes.", "public",
                "/X.java", 1, 2, 1f, "hybrid", "class");
        String p = PassageBuilder.build(cls);
        assertThat(p.lines().findFirst().orElse("")).isEqualTo("LuceneIndexer");
    }

    @Test
    void nullClassNameFallsBackToMethodNameOnly() {
        SearchResult r = new SearchResult("p:x", "p", null, null, null, "foo", null, null,
                null, null, "public", null, 0, 0, 1f, "hybrid", "method");
        assertThat(PassageBuilder.build(r)).startsWith("foo");
    }
}
