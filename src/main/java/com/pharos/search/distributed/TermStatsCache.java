package com.pharos.search.distributed;

import org.apache.lucene.index.Term;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A coordinator's copy of the term and field statistics its statistics rounds read, per shard and
 * generation, so a keyword query whose statistics are all here can skip that round
 * ({@code DistributedSearch.Query#keywordRoundWithCachedStats}). Least recently used entries go first
 * beyond the bound; a newer generation of a shard simply has other keys. Entries are only hints: the
 * search round that uses them returns each shard's own statistics of the same terms, and a query whose
 * shards disagree with what was cached runs the statistics round after all, so a stale or wrong entry
 * can cost a round but never change a score.
 */
final class TermStatsCache {

    /** Default bound on cached term entries (field entries: a tenth of it). */
    static final int DEFAULT_MAX_TERMS = 200_000;

    private record FieldKey(String shard, long generation, String field) {}

    private record TermKey(String shard, long generation, String field, String text) {}

    /** docFreq 0: the term does not occur in that generation of the shard. */
    private record TermValue(long docFreq, long totalTermFreq) {}

    private final Map<FieldKey, GlobalStats.FieldStats> fields;
    private final Map<TermKey, TermValue> terms;

    TermStatsCache(int maxTerms) {
        int maxFields = Math.max(16, maxTerms / 10);
        this.fields = new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<FieldKey, GlobalStats.FieldStats> e) {
                return size() > maxFields;
            }
        };
        this.terms = new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<TermKey, TermValue> e) {
                return size() > maxTerms;
            }
        };
    }

    /**
     * Remembers one shard's answer to a statistics request of {@code fieldNames} and {@code requested}
     * at {@code generation} (a requested term the answer leaves out does not occur there).
     */
    synchronized void put(String shard, long generation, Collection<String> fieldNames, Collection<Term> requested,
                          GlobalStats answer) {
        if (answer == null || answer.partial()) return;
        for (String f : fieldNames) {
            GlobalStats.FieldStats fs = answer.fields().get(f);
            if (fs != null) fields.put(new FieldKey(shard, generation, f), fs);
        }
        Map<List<String>, GlobalStats.TermStat> got = new LinkedHashMap<>();
        for (GlobalStats.TermStat t : answer.terms()) got.put(List.of(t.field(), t.text()), t);
        for (Term t : requested) {
            GlobalStats.TermStat st = got.get(List.of(t.field(), t.text()));
            terms.put(new TermKey(shard, generation, t.field(), t.text()),
                    st == null ? new TermValue(0, 0) : new TermValue(st.docFreq(), st.totalTermFreq()));
        }
    }

    /**
     * The shard's statistics of {@code fieldNames} and {@code requested} at {@code generation} exactly as
     * {@link GlobalStats#of(org.apache.lucene.search.IndexSearcher, Collection, Collection)} builds them
     * (fields in name order, then the occurring terms in request order), or null when one is not cached.
     */
    synchronized GlobalStats get(String shard, long generation, Collection<String> fieldNames,
                                 Collection<Term> requested) {
        Map<String, GlobalStats.FieldStats> outFields = new LinkedHashMap<>();
        for (String f : new TreeSet<>(fieldNames)) {
            GlobalStats.FieldStats fs = fields.get(new FieldKey(shard, generation, f));
            if (fs == null) return null;
            outFields.put(f, fs);
        }
        List<GlobalStats.TermStat> outTerms = new ArrayList<>();
        for (Term t : new LinkedHashSet<>(requested)) {
            TermValue v = terms.get(new TermKey(shard, generation, t.field(), t.text()));
            if (v == null) return null;
            if (v.docFreq() > 0) outTerms.add(new GlobalStats.TermStat(t.field(), t.text(), v.docFreq(), v.totalTermFreq()));
        }
        return new GlobalStats(outFields, outTerms);
    }

    synchronized int termEntries() {
        return terms.size();
    }

    /** Tests only: every cached term occurs once more than it does (a wrong entry). */
    synchronized void corruptForTest() {
        terms.replaceAll((k, v) -> new TermValue(v.docFreq() + 1, v.totalTermFreq() + 1));
    }
}
