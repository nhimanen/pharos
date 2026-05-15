package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;

final class PassageBuilder {

    private PassageBuilder() {}

    static String build(SearchResult r) {
        StringBuilder sb = new StringBuilder();
        if (r.methodName() != null) sb.append(r.methodName()).append(' ');
        if (r.signature()  != null) sb.append(r.signature()).append('\n');
        if (r.javadoc()    != null) sb.append(truncate(r.javadoc(), 300)).append('\n');
        if (r.body()       != null) sb.append(truncate(r.body(), 200));
        return sb.toString().strip();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }
}
