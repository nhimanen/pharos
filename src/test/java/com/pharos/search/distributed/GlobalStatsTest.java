package com.pharos.search.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.internal.TermStatsResponse;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalStatsTest {

    private static final List<String> QUERIES = List.of(
            "parse query", "index writer", "close reader", "merge segments", "parseQuery",
            "search engine facade", "writer", "missing nothing matches");

    private final List<Closeable> open = new ArrayList<>();
    private final KeywordSearchStrategy keyword = new KeywordSearchStrategy();

    private interface Closeable { void close() throws IOException; }

    @AfterEach
    void closeAll() throws IOException {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    @Test
    void globalStatsSearcherOverSingleShardEqualsPlainSearcher() throws Exception {
        IndexReader a = shard("a", 0, 40);
        for (String q : QUERIES) {
            SearchRequest req = request(q);
            Query query = keyword.buildQuery(req);
            GlobalStats stats = GlobalStats.merge(List.of(response(a, query)));

            Map<String, Float> plain = scores(keyword.retrieveRaw(new IndexSearcher(a), req));
            Map<String, Float> global = scores(keyword.retrieveRaw(new GlobalStatsSearcher(a, stats), req));
            assertThat(global).as(q).isEqualTo(plain);
            assertThat(new ArrayList<>(global.keySet())).as(q + " order").isEqualTo(new ArrayList<>(plain.keySet()));
        }
    }

    @Test
    void twoShardGlobalStatsEqualMultiReader() throws Exception {
        IndexReader a = shard("a", 0, 40);
        IndexReader b = shard("b", 40, 130);   // different size and term mix: local stats differ
        MultiReader both = new MultiReader(new IndexReader[]{a, b}, false);
        open.add(both::close);

        boolean anyDiffersLocally = false;
        for (String q : QUERIES) {
            SearchRequest req = request(q);
            Query query = keyword.buildQuery(req);
            GlobalStats stats = GlobalStats.merge(List.of(response(a, query), response(b, query)));

            Map<String, Float> multi = scores(keyword.retrieveRaw(new IndexSearcher(both), wide(req)));
            Map<String, Float> globalA = scores(keyword.retrieveRaw(new GlobalStatsSearcher(a, stats), wide(req)));
            Map<String, Float> localA = scores(keyword.retrieveRaw(new IndexSearcher(a), wide(req)));

            for (Map.Entry<String, Float> e : globalA.entrySet()) {
                assertThat(Float.floatToRawIntBits(e.getValue()))
                        .as("%s: %s global %s vs multi %s", q, e.getKey(), e.getValue(), multi.get(e.getKey()))
                        .isEqualTo(Float.floatToRawIntBits(multi.get(e.getKey())));
            }
            assertThat(globalA.keySet()).as(q).isEqualTo(localA.keySet());
            if (!globalA.isEmpty() && !globalA.equals(localA)) anyDiffersLocally = true;
        }
        assertThat(anyDiffersLocally).as("the fixture must make shard-local stats wrong").isTrue();
    }

    @Test
    void mergeSumsFieldAndTermStatsAcrossShards() throws Exception {
        GlobalStats one = new GlobalStats(
                Map.of("body", new GlobalStats.FieldStats(10, 100, 50)),
                List.of(new GlobalStats.TermStat("body", "foo", 3, 7)));
        GlobalStats two = new GlobalStats(
                Map.of("body", new GlobalStats.FieldStats(5, 40, 20),
                        "javadoc", new GlobalStats.FieldStats(2, 9, 4)),
                List.of(new GlobalStats.TermStat("body", "foo", 1, 1),
                        new GlobalStats.TermStat("body", "bar", 2, 2)));

        GlobalStats merged = GlobalStats.merge(List.of(
                new TermStatsResponse(1, one), new TermStatsResponse(4, two)));

        assertThat(merged.fields()).containsEntry("body", new GlobalStats.FieldStats(15, 140, 70))
                .containsEntry("javadoc", new GlobalStats.FieldStats(2, 9, 4));
        assertThat(merged.terms()).containsExactlyInAnyOrder(
                new GlobalStats.TermStat("body", "foo", 4, 8),
                new GlobalStats.TermStat("body", "bar", 2, 2));

        GlobalStats back = new ObjectMapper().readValue(
                new ObjectMapper().writeValueAsString(merged), GlobalStats.class);
        assertThat(back).isEqualTo(merged);
    }

    // ------------------------------------------------------------------ helpers

    private TermStatsResponse response(IndexReader shard, Query query) throws IOException {
        Set<Term> terms = GlobalStats.queryTerms(query);
        return new TermStatsResponse(1, GlobalStats.of(new IndexSearcher(shard), terms));
    }

    private static SearchRequest request(String q) {
        return new SearchRequest(q, SearchRequest.SearchType.KEYWORD, null, null, 10, "text", null, null, 0);
    }

    private static SearchRequest wide(SearchRequest r) {
        return new SearchRequest(r.query(), r.type(), r.project(), r.projects(), 1000, r.outputFormat(),
                r.docType(), r.scope(), 0);
    }

    private static Map<String, Float> scores(List<RawCandidate> raw) {
        Map<String, Float> out = new LinkedHashMap<>();
        for (RawCandidate c : raw) out.put(c.result().id(), c.rawScore());
        return out;
    }

    private static final String[] VERBS = {"parse", "close", "merge", "write", "search", "index", "flush"};
    private static final String[] NOUNS = {"query", "reader", "writer", "segments", "engine", "facade", "cache"};

    /** A shard of synthetic method docs {@code [from, to)}; term mix drifts with the doc number. */
    private IndexReader shard(String name, int from, int to) throws IOException {
        Directory dir = new ByteBuffersDirectory();
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(LuceneIndexer.buildAnalyzer()))) {
            for (int i = from; i < to; i++) {
                String verb = VERBS[(i * 3) % VERBS.length];
                String noun = NOUNS[(i * 5 + i / 7) % NOUNS.length];
                Document d = new Document();
                d.add(new StringField(DocumentMapper.F_ID, name + ":m" + i, Field.Store.YES));
                d.add(new StringField(DocumentMapper.F_PROJECT, name, Field.Store.YES));
                d.add(new StringField(DocumentMapper.F_DOC_TYPE, "method", Field.Store.YES));
                d.add(new TextField(DocumentMapper.F_METHOD_NAME, verb + Character.toUpperCase(noun.charAt(0))
                        + noun.substring(1), Field.Store.YES));
                d.add(new TextField(DocumentMapper.F_CLASS_NAME, NOUNS[i % NOUNS.length] + " manager", Field.Store.YES));
                d.add(new TextField(DocumentMapper.F_JAVADOC, verb + " the " + noun + (i % 4 == 0 ? " quickly" : ""),
                        Field.Store.YES));
                StringBuilder body = new StringBuilder();
                for (int k = 0; k <= i % 6; k++) body.append(VERBS[(i + k) % VERBS.length]).append(' ')
                        .append(NOUNS[(i + 2 * k) % NOUNS.length]).append(' ');
                d.add(new TextField(DocumentMapper.F_BODY, body.toString(), Field.Store.YES));
                d.add(new TextField(DocumentMapper.F_SIGNATURE, verb + "(" + noun + ")", Field.Store.YES));
                w.addDocument(d);
            }
        }
        DirectoryReader r = DirectoryReader.open(dir);
        open.add(dir::close);
        open.add(r::close);
        return r;
    }
}
