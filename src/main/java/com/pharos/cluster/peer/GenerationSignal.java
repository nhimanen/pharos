package com.pharos.cluster.peer;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The condition {@code GET /replication/<shard>/latest?wait=&after=} waits on: signalled by the
 * peer publisher right after a generation is committed. A waiter is a future, so a long-poll holds
 * no thread while it waits (the web server suspends the request).
 *
 * <p>Publishes in this process signal directly. A generation published by another process on the
 * same store (a {@code pharos index} or {@code pharos publish} run beside {@code pharos web}, which is
 * how every publish reaches a peer-mode writer) is signalled by the endpoint's watch of
 * {@code LATEST} for the shards with waiters ({@link ReplicationEndpoint}).
 */
public final class GenerationSignal {

    private record Waiter(long after, CompletableFuture<Void> future) {}

    private final Map<ShardId, List<Waiter>> waiters = new HashMap<>();
    private boolean closed;

    /**
     * A future completed when a generation newer than {@code after} of {@code shard} is signalled,
     * or on {@link #close}. Completing (or cancelling) it any other way unregisters it.
     */
    public CompletableFuture<Void> awaitNewer(ShardId shard, long after) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        Waiter w = new Waiter(after, f);
        synchronized (this) {
            if (closed) {
                f.complete(null);
                return f;
            }
            waiters.computeIfAbsent(shard, s -> new ArrayList<>()).add(w);
        }
        f.whenComplete((v, e) -> remove(shard, w));
        return f;
    }

    /** Wakes every waiter of {@code shard} waiting for a generation older than {@code gen}. */
    public void published(ShardId shard, Generation gen) {
        List<Waiter> wake = new ArrayList<>();
        synchronized (this) {
            List<Waiter> list = waiters.get(shard);
            if (list == null) return;
            for (Waiter w : list) {
                if (gen.value() > w.after()) wake.add(w);
            }
        }
        for (Waiter w : wake) w.future().complete(null); // outside the lock: completion runs the response
    }

    /** The shards with at least one waiter. */
    public synchronized java.util.Set<ShardId> waitingShards() {
        return java.util.Set.copyOf(waiters.keySet());
    }

    /** Waiters currently registered (all shards). */
    public synchronized int waiting() {
        int n = 0;
        for (List<Waiter> l : waiters.values()) n += l.size();
        return n;
    }

    /** Wakes every waiter (they answer with the current generation); later waits complete at once. */
    public void close() {
        List<Waiter> wake = new ArrayList<>();
        synchronized (this) {
            closed = true;
            waiters.values().forEach(wake::addAll);
        }
        for (Waiter w : wake) w.future().complete(null);
    }

    private synchronized void remove(ShardId shard, Waiter w) {
        List<Waiter> list = waiters.get(shard);
        if (list == null) return;
        list.remove(w);
        if (list.isEmpty()) waiters.remove(shard);
    }
}
