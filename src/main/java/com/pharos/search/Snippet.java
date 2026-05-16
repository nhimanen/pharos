package com.pharos.search;

/**
 * A source excerpt extracted from a {@link SearchResult} body, with its
 * absolute file line range. Produced by {@link SnippetDecorator}.
 *
 * {@code text} may be null when the result has no stored body.
 */
public record Snippet(String text, int startLine, int endLine) {}
