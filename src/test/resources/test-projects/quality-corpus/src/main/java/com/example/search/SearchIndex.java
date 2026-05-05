package com.example.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Full-text search index for adding, removing, and querying text documents.
 */
public class SearchIndex {

    private final Map<String, List<String>> index = new HashMap<>();

    /**
     * Adds a document to the search index under the given identifier.
     * If a document with this ID already exists it is replaced.
     * The content is tokenized and stored for keyword lookup.
     *
     * @param id      the unique document identifier
     * @param content the text content to index and make searchable
     */
    public void indexDocument(String id, String content) {
        index.put(id, tokenize(content));
    }

    /**
     * Removes a document from the search index.
     * Has no effect if the identifier does not exist.
     *
     * @param id the identifier of the document to remove
     */
    public void removeDocument(String id) {
        index.remove(id);
    }

    /**
     * Searches the index for documents containing the given keyword.
     * Returns document identifiers ranked by term frequency (most occurrences first).
     *
     * @param keyword the search term to look up
     * @return list of matching document IDs ordered by relevance
     */
    public List<String> findByKeyword(String keyword) {
        String kw = keyword.toLowerCase();
        return index.entrySet().stream()
                .filter(e -> e.getValue().contains(kw))
                .sorted((a, b) -> Long.compare(
                        b.getValue().stream().filter(kw::equals).count(),
                        a.getValue().stream().filter(kw::equals).count()))
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * Re-ranks an existing candidate result set by relevance to the query.
     * Uses a TF-inspired score: documents where more query terms appear rank higher.
     *
     * @param documentIds the candidate result set to re-rank
     * @param query       the original search query string
     * @return the document IDs reordered by relevance score, descending
     */
    public List<String> rankByRelevance(List<String> documentIds, String query) {
        List<String> queryTerms = tokenize(query);
        return documentIds.stream()
                .sorted(Comparator.comparingDouble((String id) -> score(id, queryTerms)).reversed())
                .toList();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<String> tokenize(String text) {
        return new ArrayList<>(Arrays.asList(text.toLowerCase().split("\\W+")));
    }

    private double score(String id, List<String> terms) {
        List<String> tokens = index.getOrDefault(id, List.of());
        return terms.stream().mapToLong(t -> tokens.stream().filter(t::equals).count()).sum();
    }
}
