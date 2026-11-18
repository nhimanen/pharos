package com.pharos.search.distributed;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TermStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link IndexSearcher} over one shard that scores with corpus-wide statistics: term and
 * collection statistics come from {@link GlobalStats} (summed over every shard of the query), so a
 * shard's BM25 scores equal those of the same documents in a {@code MultiReader} over all shards.
 *
 * <p>Terms and fields the stats don't cover fall back to the shard's own statistics; the coordinator
 * collects every scoring term of the query ({@link GlobalStats#queryTerms}) so this only happens for
 * multi-term queries rewritten on the shard. Each fallback is counted ({@link #localFallbacks}) and
 * logged at debug, so a coordinator that misses terms shows up. {@code maxDoc} is not on the wire: collection statistics
 * report {@code docCount} as {@code maxDoc}, which no similarity pharos uses reads (BM25 and
 * {@code SimilarityBase} score with {@code docCount}).
 *
 * <p><b>Known gap: synonyms across shards.</b> {@code SynonymQuery} (and {@code MultiPhraseQuery})
 * ask the searcher for a term's statistics only when that term occurs in the shard being searched
 * (Lucene 10.4 {@code SynonymWeight}: {@code if (ts.docFreq() > 0)}). A synonym that exists only on
 * another shard therefore drops out of the pseudo-term's statistics here (its {@code docFreq} is the
 * max over the synonyms), while a {@code MultiReader} over all shards counts it: such documents can
 * score differently from a single node. Plain term, boolean and phrase queries are exact. The
 * keyword pipeline builds {@code SynonymQuery} only with a {@code synonyms.txt}; the gap is pinned
 * by {@code GlobalStatsTest#synonymTermOnlyOnAnotherShardDivergesFromMultiReader}.
 */
public class GlobalStatsSearcher extends IndexSearcher {

    private static final Logger log = LoggerFactory.getLogger(GlobalStatsSearcher.class);

    private final Map<String, GlobalStats.FieldStats> fields;
    private final Map<String, Map<String, GlobalStats.TermStat>> terms = new HashMap<>();
    private final AtomicLong localFallbacks = new AtomicLong();

    public GlobalStatsSearcher(IndexReader reader, GlobalStats stats) {
        super(reader);
        this.fields = Map.copyOf(stats.fields());
        for (GlobalStats.TermStat t : stats.terms()) {
            terms.computeIfAbsent(t.field(), f -> new HashMap<>()).put(t.text(), t);
        }
    }

    /** A {@link SearcherFactory} handing out searchers over {@code stats}, for the strategies' seam. */
    public static SearcherFactory factory(GlobalStats stats) {
        return new SearcherFactory() {
            @Override
            public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) {
                return new GlobalStatsSearcher(reader, stats);
            }
        };
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
        Map<String, GlobalStats.TermStat> byText = terms.get(term.field());
        GlobalStats.TermStat global = byText == null ? null : byText.get(term.text());
        if (global == null) {
            fellBack("term", term.field() + ":" + term.text());
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        return new TermStatistics(term.bytes(), global.docFreq(), global.totalTermFreq());
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
        GlobalStats.FieldStats global = fields.get(field);
        if (global == null) {
            fellBack("field", field);
            return super.collectionStatistics(field);
        }
        if (global.docCount() == 0) return null;
        return new CollectionStatistics(field, global.docCount(), global.docCount(),
                global.sumTotalTermFreq(), global.sumDocFreq());
    }

    /** How many term or field statistics this searcher answered from the shard's own stats. */
    public long localFallbacks() {
        return localFallbacks.get();
    }

    private void fellBack(String kind, String what) {
        localFallbacks.incrementAndGet();
        log.debug("No global statistics for {} {}: using the shard's own", kind, what);
    }
}
