package com.pharos.cluster.internal;

import com.pharos.cluster.ShardId;
import com.pharos.search.Snippet;
import com.pharos.search.distributed.GlobalStats;
import com.pharos.search.distributed.RawCandidate;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Test double of {@link ShardSearchService}: serves fixed generations, counts every handler call. */
public class FakeShardSearchService implements ShardSearchService {
    public final Map<String, Long> generations = new ConcurrentHashMap<>();
    public final AtomicInteger calls = new AtomicInteger();
    /** When set, every call fails with it (a 500). */
    public volatile RuntimeException failure;
    /** When set, the work answers with this generation although {@link #servedGeneration} said another: a swap in between. */
    public volatile Long generationDuringWork;

    private long workGeneration(String shard) {
        Long g = generationDuringWork;
        return g != null ? g : generations.get(shard);
    }

    public FakeShardSearchService serving(String shard, long generation) {
        generations.put(shard, generation);
        return this;
    }

    @Override
    public OptionalLong servedGeneration(ShardId shard) {
        Long g = generations.get(shard.value());
        return g == null ? OptionalLong.empty() : OptionalLong.of(g);
    }

    private void enter() {
        calls.incrementAndGet();
        if (failure != null) throw failure;
    }

    @Override
    public TermStatsResponse termStats(TermStatsRequest r) {
        enter();
        return new TermStatsResponse(workGeneration(r.shard()),
                new GlobalStats(Map.of("body", new GlobalStats.FieldStats(10, 100, 50)),
                        List.of(new GlobalStats.TermStat("body", "foo", 3, 7))));
    }

    @Override
    public ShardSearchResponse searchRaw(ShardSearchRequest r) {
        enter();
        RawCandidate c = new RawCandidate(1.5f, 4, "A.java", "method", "service", "public", null);
        return new ShardSearchResponse(workGeneration(r.shard()), 9, List.of(c));
    }

    @Override
    public SnippetsResponse snippets(SnippetsRequest r) {
        enter();
        return new SnippetsResponse(generationDuringWork != null ? generationDuringWork : r.generation(), Map.of("id1", new Snippet("text", 1, 2)));
    }

    @Override
    public CalleesResponse callees(CalleesRequest r) {
        enter();
        return new CalleesResponse(generationDuringWork != null ? generationDuringWork : r.generation(), Map.of("a.B#c()", List.of("a.B#d()")));
    }

    @Override
    public GraphResponse graph(GraphRequest r) {
        enter();
        return new GraphResponse(generationDuringWork != null ? generationDuringWork : workGeneration(r.shard()),
                Map.of("a.B#c()", List.of("a.B#d()")), null, null);
    }
}
