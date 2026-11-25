package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SynonymsMergeTest {

    private static String section(String project, String date, String... rules) {
        StringBuilder sb = new StringBuilder("\n# ── Auto-mined from '" + project + "' on " + date + " (" + rules.length
                + " rules) ─\n");
        for (String r : rules) sb.append(r).append("  # auto:").append(project).append(':').append(date).append('\n');
        return sb.toString();
    }

    @Test
    void replacesOnlyThePublishedProjectsSection() {
        String published = "db, database\n" + section("a", "d1", "alpha => one") + section("b", "d1", "beta => two");
        String local = "db, database\n" + section("b", "d2", "beta => three") + section("a", "d0", "stale => x");
        String merged = SynonymsMerge.merge(published, local, "b");
        assertThat(merged).contains("alpha => one", "beta => three").doesNotContain("beta => two", "stale => x");
        // canonical: the same text whatever order the sections came in
        assertThat(SynonymsMerge.merge(merged, local, "b")).isEqualTo(merged);
        assertThat(merged.indexOf("alpha")).isLessThan(merged.indexOf("beta"));
    }

    @Test
    void writerWithoutTheProjectsRulesKeepsThePublishedOnes() {
        String published = section("a", "d1", "alpha => one");
        assertThat(SynonymsMerge.merge(published, "db, database\n", "a")).contains("alpha => one", "db, database");
    }

    @Test
    void firstPublishTakesTheWholeLocalFile() {
        String local = "calc, calculator\nusr => user\n";
        assertThat(SynonymsMerge.merge(null, local, "a")).isEqualTo(local);
        String withSections = "x, y\n" + section("b", "d", "beta => two");
        assertThat(SynonymsMerge.merge(null, withSections, "a")).contains("beta => two");
    }

    @Test
    void legacySingleFileIsReadTheSameWay() {
        // a Phase 1-3 bucket: one writer's whole file, sections in publish order, blank-line runs
        String legacy = "auth, authentication\n\n\n" + section("z", "d", "zeta => six") + section("a", "d", "alpha => one");
        String merged = SynonymsMerge.merge(legacy, "auth, authentication\n" + section("m", "d", "mu => m"), "m");
        assertThat(merged).contains("zeta => six", "alpha => one", "mu => m");
        assertThat(merged.indexOf("alpha")).isLessThan(merged.indexOf("mu")).isLessThan(merged.indexOf("zeta"));
    }
}
