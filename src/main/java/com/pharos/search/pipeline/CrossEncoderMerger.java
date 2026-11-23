package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges multiple candidate lists by scoring every deduplicated candidate with a
 * cross-encoder model and fusing the cross-encoder ranking with the
 * first-stage order (best rank across lists) by equal-weight RRF (see {@link RrfFusion}).
 *
 * <p>Deduplication is by {@link SearchResult#id()}, first-occurrence wins (preserves
 * the searchType of whichever retriever found it first).
 *
 * <p>When the encoder is unavailable, returns the deduplicated pool truncated to limit
 * (ordered by the first retriever's ranking).
 */
public final class CrossEncoderMerger implements MergeStage {

    private final CrossEncoder encoder;

    public CrossEncoderMerger(CrossEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public List<SearchResult> merge(List<List<SearchResult>> candidates,
                                    com.pharos.search.SearchRequest req, SearchTrace trace) {
        int limit = req.limit();
        String query = req.query();
        long t = System.currentTimeMillis();

        Map<String, SearchResult> deduped = new LinkedHashMap<>();
        for (List<SearchResult> list : candidates) {
            for (SearchResult r : list) {
                deduped.putIfAbsent(r.id(), r);
            }
        }
        // First-stage order: each candidate's best rank in any list (ties: list order).
        Map<String, Integer> bestRank = new LinkedHashMap<>();
        for (List<SearchResult> list : candidates) {
            for (int i = 0; i < list.size(); i++) bestRank.merge(list.get(i).id(), i, Math::min);
        }
        List<SearchResult> pool = new ArrayList<>(deduped.values());
        pool.sort(Comparator.comparingInt(r -> bestRank.get(r.id())));

        if (!encoder.isAvailable()) {
            if (trace != null) trace.record("cross-encoder merge (encoder unavailable)", t);
            return pool.subList(0, Math.min(limit, pool.size()));
        }

        List<String> passages = pool.stream().map(PassageBuilder::build).toList();
        float[] scores = encoder.score(query, passages);

        List<SearchResult> scored = RrfFusion.fuse(pool, scores, "cross-encoder");

        if (trace != null) trace.record("cross-encoder merge", t);
        return scored.subList(0, Math.min(limit, scored.size()));
    }
}
