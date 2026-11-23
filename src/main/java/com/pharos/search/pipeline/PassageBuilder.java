package com.pharos.search.pipeline;

import com.pharos.indexer.DocumentMapper;
import com.pharos.search.SearchResult;

/**
 * Builds the text passage fed to the cross-encoder for query-document scoring.
 *
 * <p>When javadoc is present it is used verbatim — it already provides natural language.
 * When absent, a compact natural-language header is synthesized from the split method name
 * and formatted return type so the cross-encoder sees semantically meaningful text rather
 * than raw identifiers.
 */
final class PassageBuilder {

    private PassageBuilder() {}

    static String build(SearchResult r) {
        StringBuilder sb = new StringBuilder();

        // Identifier line: the cross-encoder otherwise never sees the class name or camelCase method
        // name, so exact-name targets score ~0 (class docs get just the class name).
        StringBuilder id = new StringBuilder();
        if (r.className() != null && !r.className().isBlank()) id.append(r.className());
        if (r.methodName() != null && !r.methodName().isBlank()) {
            if (id.length() > 0) id.append('.');
            id.append(r.methodName());
        }
        if (id.length() > 0) sb.append(id).append('\n');

        boolean hasJavadoc = r.javadoc() != null && !r.javadoc().isBlank();

        if (hasJavadoc) {
            // Javadoc leads — most informative signal for the cross-encoder
            sb.append(truncate(r.javadoc(), 300)).append('\n');
        } else {
            // Synthesize a natural-language header: "find active — returns list of User"
            String header = buildNaturalHeader(r);
            if (!header.isBlank()) sb.append(header).append('\n');
        }

        // Signature always included (type info + camelCase names help both model vocabularies)
        if (r.signature() != null) sb.append(r.signature()).append('\n');

        // Body snippet — cross-encoder sees implementation for intent matching
        if (r.body() != null) sb.append(truncate(r.body(), 300));

        return sb.toString().strip();
    }

    /**
     * Builds a short NL phrase from the method name and return type.
     * E.g. "findActiveUsers" + "List&lt;User&gt;" → "find active users — returns list of User"
     */
    private static String buildNaturalHeader(SearchResult r) {
        StringBuilder header = new StringBuilder();

        String splitName = DocumentMapper.splitIdentifier(r.methodName());
        if (splitName != null && !splitName.isBlank()) header.append(splitName);

        String formatted = DocumentMapper.formatReturnType(r.returnType());
        if (formatted != null) header.append(" — returns ").append(formatted);

        return header.toString();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }
}
