package com.pharos.search.distributed;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.cluster.internal.TermStatsResponse;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Term and collection statistics, per shard on the wire and summed across shards by the coordinator
 * so every shard scores BM25 against the same corpus (DFS query-then-fetch). {@link #of} reads one
 * shard's, {@link #merge} sums them, {@link GlobalStatsSearcher} scores with them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GlobalStats(Map<String, FieldStats> fields, List<TermStat> terms) {

    /** Collection statistics of one field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldStats(long docCount, long sumTotalTermFreq, long sumDocFreq) {}

    /** Document and total frequency of one term of one field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TermStat(String field, String text, long docFreq, long totalTermFreq) {}

    public GlobalStats {
        fields = fields == null ? Map.of() : fields;
        terms = terms == null ? List.of() : terms;
    }

    /**
     * Sums the shards' statistics: per field, {@code docCount}, {@code sumTotalTermFreq} and
     * {@code sumDocFreq}; per term, {@code docFreq} and {@code totalTermFreq}. These are exactly the
     * sums a {@code MultiReader} over the shards computes.
     */
    public static GlobalStats merge(List<TermStatsResponse> shards) {
        Map<String, long[]> fields = new TreeMap<>();
        Map<List<String>, long[]> terms = new LinkedHashMap<>();
        for (TermStatsResponse shard : shards) {
            if (shard == null || shard.stats() == null) continue;
            shard.stats().fields().forEach((field, fs) -> {
                long[] sum = fields.computeIfAbsent(field, f -> new long[3]);
                sum[0] += fs.docCount();
                sum[1] += fs.sumTotalTermFreq();
                sum[2] += fs.sumDocFreq();
            });
            for (TermStat t : shard.stats().terms()) {
                long[] sum = terms.computeIfAbsent(List.of(t.field(), t.text()), k -> new long[2]);
                sum[0] += t.docFreq();
                sum[1] += t.totalTermFreq();
            }
        }
        Map<String, FieldStats> outFields = new LinkedHashMap<>();
        fields.forEach((f, s) -> outFields.put(f, new FieldStats(s[0], s[1], s[2])));
        List<TermStat> outTerms = new ArrayList<>(terms.size());
        terms.forEach((k, s) -> outTerms.add(new TermStat(k.get(0), k.get(1), s[0], s[1])));
        return new GlobalStats(outFields, outTerms);
    }

    /** One shard's statistics for {@code terms} and their fields. */
    public static GlobalStats of(IndexSearcher shard, Collection<Term> terms) throws IOException {
        Set<String> fields = new TreeSet<>();
        for (Term t : terms) fields.add(t.field());
        return of(shard, fields, terms);
    }

    /**
     * One shard's statistics: collection statistics of {@code fields} (fields without indexed terms
     * are left out) and the frequencies of {@code terms} (terms absent from the shard are left out).
     * Read straight from the shard's reader, never from an overriding searcher.
     */
    public static GlobalStats of(IndexSearcher shard, Collection<String> fields, Collection<Term> terms)
            throws IOException {
        IndexReader reader = shard.getIndexReader();
        IndexSearcher local = new IndexSearcher(reader);
        Map<String, FieldStats> outFields = new LinkedHashMap<>();
        for (String field : new TreeSet<>(fields)) {
            CollectionStatistics cs = local.collectionStatistics(field);
            if (cs != null) {
                outFields.put(field, new FieldStats(cs.docCount(), cs.sumTotalTermFreq(), cs.sumDocFreq()));
            }
        }
        List<TermStat> outTerms = new ArrayList<>();
        for (Term t : new LinkedHashSet<>(terms)) {
            int df = reader.docFreq(t);
            if (df > 0) outTerms.add(new TermStat(t.field(), t.text(), df, reader.totalTermFreq(t)));
        }
        return new GlobalStats(outFields, outTerms);
    }

    /**
     * The terms a query scores with, as the coordinator collects them before asking shards for
     * statistics. {@code query} must already be in its scoring form: multi-term queries (wildcard,
     * fuzzy) contribute no terms before they are rewritten against an index, and are then scored
     * with the shard's local statistics.
     */
    public static Set<Term> queryTerms(Query query) {
        Set<Term> out = new LinkedHashSet<>();
        query.visit(QueryVisitor.termCollector(out));
        return out;
    }
}
