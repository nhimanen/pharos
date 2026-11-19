package com.pharos.search.distributed;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TermStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * <p><b>Synonyms across shards.</b> {@code SynonymQuery} and {@code MultiPhraseQuery} ask the searcher
 * for a term's statistics only when that term occurs in the shard being searched (Lucene 10.4
 * {@code SynonymWeight} and {@code MultiPhraseQuery}: {@code if (ts.docFreq() > 0)}), so a synonym
 * present only on another shard would drop out of the combined statistics here. {@link #rewrite}
 * therefore marks each such query ({@link GlobalTermsScope}); while its weight is built,
 * {@link #getSimilarity} hands out a similarity that scores with the statistics a {@code MultiReader}
 * over all shards would use, computed from {@link GlobalStats} for <em>every</em> term the query
 * expands to: for a synonym group the pseudo-term ({@code docFreq} = the maximum, {@code
 * totalTermFreq} = the sum over the terms with a global {@code docFreq > 0}); for a multi-phrase,
 * every such term's statistics in the query's term order. Only the statistics change: which
 * documents match is the shard's own. A field the stats don't cover keeps the shard's statistics.
 */
public class GlobalStatsSearcher extends IndexSearcher {

    private static final Logger log = LoggerFactory.getLogger(GlobalStatsSearcher.class);

    private final Map<String, GlobalStats.FieldStats> fields;
    private final Map<String, Map<String, GlobalStats.TermStat>> terms = new HashMap<>();
    private final AtomicLong localFallbacks = new AtomicLong();
    /** The coordinator shipped only some of the query's terms (see {@link GlobalStats#asPartial}). */
    private final boolean partial;

    public GlobalStatsSearcher(IndexReader reader, GlobalStats stats) {
        super(reader);
        this.partial = stats.partial();
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

    /** The query whose weight is being built under {@link GlobalTermsScope} on this thread, or null. */
    private static final ThreadLocal<Query> SCOPE = new ThreadLocal<>();

    /**
     * The rewritten query, with every {@code SynonymQuery} and {@code MultiPhraseQuery} reachable
     * through boolean, boost and disjunction-max clauses wrapped in a {@link GlobalTermsScope}.
     */
    @Override
    public Query rewrite(Query original) throws IOException {
        return scoped(super.rewrite(original));
    }

    static Query scoped(Query q) {
        if (q instanceof SynonymQuery || q instanceof MultiPhraseQuery) return new GlobalTermsScope(q);
        if (q instanceof BoostQuery b) {
            Query inner = scoped(b.getQuery());
            return inner == b.getQuery() ? q : new BoostQuery(inner, b.getBoost());
        }
        if (q instanceof BooleanQuery bq) {
            boolean changed = false;
            BooleanQuery.Builder out = new BooleanQuery.Builder()
                    .setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
            for (BooleanClause c : bq.clauses()) {
                Query inner = c.isScoring() ? scoped(c.query()) : c.query();
                changed |= inner != c.query();
                out.add(inner, c.occur());
            }
            return changed ? out.build() : q;
        }
        if (q instanceof DisjunctionMaxQuery d) {
            boolean changed = false;
            List<Query> out = new ArrayList<>();
            for (Query inner : d.getDisjuncts()) {
                Query s = scoped(inner);
                changed |= s != inner;
                out.add(s);
            }
            return changed ? new DisjunctionMaxQuery(out, d.getTieBreakerMultiplier()) : q;
        }
        return q;
    }

    @Override
    public Similarity getSimilarity() {
        Similarity base = super.getSimilarity();
        Query scope = SCOPE.get();
        if (scope == null) return base;
        TermStatistics[] global = globalTermStatistics(scope);
        return global == null ? base : new GlobalTermsSimilarity(base, global);
    }

    /**
     * The term statistics a {@code MultiReader} over every shard would score {@code q} with, or null
     * when the stats don't cover its field (the shard's own statistics are used) or no term occurs.
     */
    TermStatistics[] globalTermStatistics(Query q) {
        if (q instanceof SynonymQuery sq) {
            if (!fields.containsKey(sq.getField())) return null;
            long docFreq = 0, totalTermFreq = 0;
            for (Term t : sq.getTerms()) {
                GlobalStats.TermStat g = termStat(t);
                if (g == null && partial) return null; // not shipped: this group keeps the shard's statistics
                if (g == null || g.docFreq() <= 0) continue;
                docFreq = Math.max(docFreq, g.docFreq());
                totalTermFreq += g.totalTermFreq();
            }
            return docFreq == 0 ? null : new TermStatistics[]{
                    new TermStatistics(new BytesRef("synonym pseudo-term"), docFreq, totalTermFreq)};
        }
        if (q instanceof MultiPhraseQuery mpq) {
            Term[][] arrays = mpq.getTermArrays();
            if (arrays.length == 0 || arrays[0].length == 0 || !fields.containsKey(arrays[0][0].field())) return null;
            List<TermStatistics> out = new ArrayList<>();
            for (Term[] position : arrays) {
                for (Term t : position) {
                    GlobalStats.TermStat g = termStat(t);
                    if (g == null && partial) return null;
                    if (g != null && g.docFreq() > 0) {
                        out.add(new TermStatistics(t.bytes(), g.docFreq(), g.totalTermFreq()));
                    }
                }
            }
            return out.isEmpty() ? null : out.toArray(new TermStatistics[0]);
        }
        return null;
    }

    private GlobalStats.TermStat termStat(Term t) {
        Map<String, GlobalStats.TermStat> byText = terms.get(t.field());
        return byText == null ? null : byText.get(t.text());
    }

    /**
     * Marks a synonym or multi-phrase query so its weight is built with global statistics for all its
     * terms (see the class doc). Matching, rewriting and visiting are the wrapped query's.
     */
    static final class GlobalTermsScope extends Query {
        private final Query inner;

        GlobalTermsScope(Query inner) {
            this.inner = inner;
        }

        Query inner() {
            return inner;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            if (!scoreMode.needsScores() || !(searcher instanceof GlobalStatsSearcher)) {
                return inner.createWeight(searcher, scoreMode, boost);
            }
            Query outer = SCOPE.get();
            SCOPE.set(inner);
            try {
                return inner.createWeight(searcher, scoreMode, boost);
            } finally {
                if (outer == null) SCOPE.remove(); else SCOPE.set(outer);
            }
        }

        @Override
        public Query rewrite(IndexSearcher searcher) {
            return this; // the wrapped query is already rewritten
        }

        @Override
        public void visit(QueryVisitor visitor) {
            inner.visit(visitor);
        }

        @Override
        public String toString(String field) {
            return inner.toString(field);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof GlobalTermsScope g && g.inner.equals(inner);
        }

        @Override
        public int hashCode() {
            return 31 * GlobalTermsScope.class.hashCode() + inner.hashCode();
        }
    }

    /** The searcher's similarity, scoring with the given term statistics instead of those passed in. */
    private static final class GlobalTermsSimilarity extends Similarity {
        private final Similarity base;
        private final TermStatistics[] global;

        GlobalTermsSimilarity(Similarity base, TermStatistics[] global) {
            this.base = base;
            this.global = global;
        }

        @Override
        public long computeNorm(org.apache.lucene.index.FieldInvertState state) {
            return base.computeNorm(state);
        }

        @Override
        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return base.scorer(boost, collectionStats, global);
        }
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
