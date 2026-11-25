package com.pharos.cluster.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeaseManagerTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ShardId SHARD = new ShardId("alpha");
    private static final Duration TTL = Duration.ofSeconds(30);

    /** The bucket's clock: only the test moves it. */
    static final class TestClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-21T10:00:00Z"));
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return now.get(); }
        void advance(long seconds) { now.updateAndGet(i -> i.plusSeconds(seconds)); }
    }

    private final TestClock bucketClock = new TestClock();
    private final InMemoryObjectStore store = new InMemoryObjectStore(bucketClock);
    private final ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
    private final String leaseKey = layout.leaseKey(SHARD);

    /** A writer whose "own latest write" is a heartbeat it writes each time the bucket's clock is asked. */
    private LeaseManager writer(ObjectStore s, String node) {
        String hb = "pharos/test-heartbeats/" + node + ".json";
        return new LeaseManager(s, layout, node, TTL, () -> {
            s.put(hb, "{}".getBytes(StandardCharsets.UTF_8));
            return s.stat(hb).map(ObjectInfo::lastModified);
        }, bucketClock);
    }

    private LeaseManager writer(String node) {
        return writer(store, node);
    }

    private JsonNode leaseBody() throws IOException {
        return JSON.readTree(store.get(leaseKey).orElseThrow().data());
    }

    @Test
    void leaseKeyIsUnderTheShard() {
        assertThat(leaseKey).isEqualTo("pharos/shards/alpha/lease");
    }

    @Test
    void acquireWhenFree() throws Exception {
        LeaseManager a = writer("node-a");

        Lease lease = a.tryAcquire(SHARD).orElseThrow();

        assertThat(lease.shard()).isEqualTo(SHARD);
        assertThat(lease.writer()).isEqualTo("node-a");
        assertThat(lease.etag()).isEqualTo(store.get(leaseKey).orElseThrow().etag());
        assertThat(lease.expires()).isEqualTo(store.stat(leaseKey).orElseThrow().lastModified().plus(TTL));
        JsonNode body = leaseBody();
        assertThat(body.get("writer").asText()).isEqualTo("node-a");
        assertThat(Instant.parse(body.get("expires").asText())).isEqualTo(bucketClock.instant().plus(TTL));
        assertThat(a.isHeld(SHARD)).isTrue();
        assertThat(a.held()).containsExactly(lease);
        a.assertHeld(SHARD); // passes
        // Acquiring again is idempotent.
        assertThat(a.tryAcquire(SHARD)).contains(lease);
    }

    @Test
    void cannotAcquireHeldLease() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();

        assertThat(b.tryAcquire(SHARD)).isEmpty();
        bucketClock.advance(30); // exactly the TTL: not yet expired
        assertThat(b.tryAcquire(SHARD)).isEmpty();

        assertThat(store.get(leaseKey).orElseThrow().etag()).isEqualTo(lease.etag());
        assertThat(b.isHeld(SHARD)).isFalse();
        assertThatThrownBy(() -> b.assertHeld(SHARD)).isInstanceOf(PublishConflictException.class);
        a.assertHeld(SHARD);
    }

    @Test
    void renewedLeaseIsNotTakenOver() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();
        for (int i = 0; i < 6; i++) {
            bucketClock.advance(10); // TTL/3
            assertThat(a.renew(lease)).isTrue();
            assertThat(b.tryAcquire(SHARD)).isEmpty();
        }
        assertThat(a.held().getFirst().etag()).isNotEqualTo(lease.etag());
        a.assertHeld(SHARD);
    }

    @Test
    void takeoverAfterExpiry() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        a.tryAcquire(SHARD).orElseThrow();

        bucketClock.advance(31);
        Lease taken = b.tryAcquire(SHARD).orElseThrow();

        assertThat(taken.writer()).isEqualTo("node-b");
        assertThat(leaseBody().get("writer").asText()).isEqualTo("node-b");
        b.assertHeld(SHARD);
        assertThatThrownBy(() -> a.assertHeld(SHARD))
                .isInstanceOf(PublishConflictException.class).hasMessageContaining("lost");
        assertThat(a.isHeld(SHARD)).isFalse();
    }

    @Test
    void expiryUsesTheBucketClockNotTheLocalOne() throws Exception {
        writer("node-a").tryAcquire(SHARD).orElseThrow();
        // B's local clock is far ahead, but the bucket's clock has not moved: no takeover.
        Clock skewed = Clock.offset(bucketClock, Duration.ofHours(1));
        String hb = "pharos/test-heartbeats/node-b.json";
        LeaseManager b = new LeaseManager(store, layout, "node-b", TTL, () -> {
            store.put(hb, new byte[0]);
            return store.stat(hb).map(ObjectInfo::lastModified);
        }, skewed);
        assertThat(b.tryAcquire(SHARD)).isEmpty();
        // And without any own write there is no "now": an expired lease is still never taken.
        bucketClock.advance(120);
        LeaseManager blind = new LeaseManager(store, layout, "node-c", TTL, LeaseManager.BucketNow.NONE, bucketClock);
        assertThat(blind.tryAcquire(SHARD)).isEmpty();
        assertThat(b.tryAcquire(SHARD)).isPresent();
    }

    /** Review Focus 2: many writers see the lease expired at once; exactly one conditional put wins. */
    @Test
    void concurrentTakeoverExactlyOneWins() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int round = 0; round < 25; round++) {
                bucketClock.advance(31); // the previous round's winner dies too
                LeaseManager dead = writer("dead-" + round);
                dead.tryAcquire(SHARD).orElseThrow();
                bucketClock.advance(31);

                List<LeaseManager> writers = new ArrayList<>();
                for (int i = 0; i < threads; i++) writers.add(writer("w" + round + "-" + i));
                CountDownLatch ready = new CountDownLatch(threads);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Optional<Lease>>> results = new ArrayList<>();
                for (LeaseManager w : writers) {
                    results.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return w.tryAcquire(SHARD);
                    }));
                }
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                List<Lease> winners = new ArrayList<>();
                for (Future<Optional<Lease>> f : results) f.get(10, TimeUnit.SECONDS).ifPresent(winners::add);

                assertThat(winners).as("round " + round).hasSize(1);
                Lease winner = winners.getFirst();
                assertThat(leaseBody().get("writer").asText()).isEqualTo(winner.writer());
                assertThat(store.get(leaseKey).orElseThrow().etag()).isEqualTo(winner.etag());
                for (LeaseManager w : writers) {
                    assertThat(w.isHeld(SHARD)).isEqualTo(w.writer().equals(winner.writer()));
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void renewFailsAfterTakeover() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();
        bucketClock.advance(31); // A paused past its TTL
        Lease taken = b.tryAcquire(SHARD).orElseThrow();

        assertThat(a.renew(lease)).isFalse();

        assertThat(a.isHeld(SHARD)).isFalse();
        assertThat(a.held()).isEmpty();
        assertThatThrownBy(() -> a.assertHeld(SHARD)).isInstanceOf(PublishConflictException.class);
        assertThat(a.renew(lease)).isFalse(); // stays lost
        assertThat(store.get(leaseKey).orElseThrow().etag()).isEqualTo(taken.etag());
        b.assertHeld(SHARD);
    }

    @Test
    void renewerMarksLeaseLostWhenRenewalFails() throws Exception {
        AtomicBoolean failPuts = new AtomicBoolean();
        ObjectStore flaky = failingConditionalPuts(failPuts);
        LeaseManager a = writer(flaky, "node-a");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();

        failPuts.set(true);
        a.renewAll(); // an I/O failure loses the lease at once

        assertThat(a.isHeld(SHARD)).isFalse();
        assertThatThrownBy(() -> a.assertHeld(SHARD)).isInstanceOf(PublishConflictException.class);
        // Nobody wrote the lease since: once the bucket answers again it is reclaimed without waiting out the TTL.
        failPuts.set(false);
        Lease again = a.tryAcquire(SHARD).orElseThrow();
        assertThat(again.etag()).isNotEqualTo(lease.etag());
        a.assertHeld(SHARD);
    }

    @Test
    void renewerThreadRenewsAtAThirdOfTheTtl() throws Exception {
        InMemoryObjectStore real = new InMemoryObjectStore(); // the system clock: real time passes
        Duration ttl = Duration.ofSeconds(3);
        String hb = "pharos/test-heartbeats/node-b.json";
        LeaseManager a = new LeaseManager(real, layout, "node-a", ttl, LeaseManager.BucketNow.NONE, Clock.systemUTC());
        LeaseManager b = new LeaseManager(real, layout, "node-b", ttl, () -> {
            real.put(hb, new byte[0]);
            return real.stat(hb).map(ObjectInfo::lastModified);
        }, Clock.systemUTC());
        try {
            Lease first = a.tryAcquire(SHARD).orElseThrow();
            a.start();
            long until = System.nanoTime() + Duration.ofMillis(3000).toNanos();
            while (System.nanoTime() < until) {
                assertThat(b.tryAcquire(SHARD)).isEmpty();
                Thread.sleep(100);
            }
            assertThat(a.held().getFirst().etag()).isNotEqualTo(first.etag());
            a.assertHeld(SHARD);
            // Without the renewer the lease expires and is taken over.
            a.close();
            Thread.sleep(ttl.toMillis() + 1500);
            assertThat(b.tryAcquire(SHARD)).isPresent();
            assertThatThrownBy(() -> a.assertHeld(SHARD)).isInstanceOf(PublishConflictException.class);
        } finally {
            a.close();
        }
    }

    /** Fix round 1, I3: a hung store call on one lease never delays another lease's renewal, and is bounded. */
    @Test
    void slowStoreCallOnOneLeaseDoesNotDelayAnothersRenewal() throws Exception {
        ShardId slow = new ShardId("slow");
        CountDownLatch unblock = new CountDownLatch(1);
        AtomicBoolean block = new AtomicBoolean();
        ObjectStore s = (ObjectStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ObjectStore.class}, (proxy, m, args) -> {
                    if (block.get() && (m.getName().equals("get") || m.getName().equals("putIfMatch"))
                            && ((String) args[0]).contains("/shards/slow/")) {
                        unblock.await(); // hangs like an S3 call stuck in socket timeouts and retries
                    }
                    try {
                        return m.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        String hb = "pharos/test-heartbeats/node-a.json";
        LeaseManager a = new LeaseManager(s, layout, "node-a", TTL, () -> {
            s.put(hb, new byte[0]);
            return s.stat(hb).map(ObjectInfo::lastModified);
        }, bucketClock, Duration.ofSeconds(1));
        try {
            a.tryAcquire(slow).orElseThrow();
            Lease alpha = a.tryAcquire(SHARD).orElseThrow();
            block.set(true);

            // A publisher thread is stuck re-reading the slow lease (holding that shard's lock).
            AtomicReference<Throwable> publishCheck = new AtomicReference<>();
            Thread publisher = new Thread(() -> {
                try {
                    a.assertHeld(slow);
                } catch (Throwable t) {
                    publishCheck.set(t);
                }
            });
            publisher.start();
            Thread.sleep(100);

            long t0 = System.nanoTime();
            assertThat(a.isHeld(slow)).isTrue(); // in-memory reads never wait on the store
            assertThat(a.renew(alpha)).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(500));
            assertThat(a.held()).extracting(Lease::shard).contains(SHARD);

            // The stuck check gives up at the deadline ("cannot confirm"), without losing the lease.
            publisher.join(5000);
            assertThat(publishCheck.get()).isInstanceOf(PublishConflictException.class)
                    .hasMessageContaining("timed out");

            // A renewal round: the slow lease's renewal times out and is lost; alpha still renews.
            String alphaBefore = a.held().stream().filter(l -> l.shard().equals(SHARD)).findFirst().orElseThrow().etag();
            t0 = System.nanoTime();
            a.renewAll();
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(3));
            assertThat(a.isHeld(slow)).isFalse();
            assertThat(a.isHeld(SHARD)).isTrue();
            assertThat(a.held().getFirst().etag()).isNotEqualTo(alphaBefore);
            a.assertHeld(SHARD);
        } finally {
            unblock.countDown();
            a.close();
        }
    }

    @Test
    void acquireGivesUpAtTheDeadline() throws Exception {
        CountDownLatch unblock = new CountDownLatch(1);
        ObjectStore s = (ObjectStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ObjectStore.class}, (proxy, m, args) -> {
                    if (m.getName().equals("get")) unblock.await();
                    try {
                        return m.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        LeaseManager a = new LeaseManager(s, layout, "node-a", TTL, LeaseManager.BucketNow.NONE, bucketClock,
                Duration.ofMillis(200));
        try {
            long t0 = System.nanoTime();
            assertThatThrownBy(() -> a.tryAcquire(SHARD)).isInstanceOf(IOException.class).hasMessageContaining("timed out");
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(2));
            assertThat(a.isHeld(SHARD)).isFalse();
            assertThat(store.get(leaseKey)).isEmpty();
        } finally {
            unblock.countDown();
            a.close();
        }
    }

    @Test
    void releaseAfterCloseLeavesTheLeaseToExpire() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();
        a.close();
        a.release(lease); // documented misuse: nothing is written
        assertThat(leaseBody().has("released")).isFalse();
        assertThat(b.tryAcquire(SHARD)).isEmpty();
        bucketClock.advance(31);
        assertThat(b.tryAcquire(SHARD)).isPresent();
    }

    @Test
    void assertHeldFailsOnceExpiredByTheBucketClock() throws Exception {
        LeaseManager a = writer("node-a");
        a.tryAcquire(SHARD).orElseThrow();
        bucketClock.advance(31); // its heartbeat runs on, its renewals did not

        assertThatThrownBy(() -> a.assertHeld(SHARD))
                .isInstanceOf(PublishConflictException.class).hasMessageContaining("expired");
        assertThat(a.isHeld(SHARD)).isFalse();
    }

    @Test
    void assertHeldRefusesWhenTheLeaseCannotBeRead() throws Exception {
        LeaseManager a = writer("node-a");
        a.tryAcquire(SHARD).orElseThrow();
        store.failGetOn("/lease");
        try {
            assertThatThrownBy(() -> a.assertHeld(SHARD))
                    .isInstanceOf(PublishConflictException.class).hasMessageContaining("cannot confirm");
        } finally {
            store.clearFailures();
        }
        a.assertHeld(SHARD); // a read failure alone does not lose the lease
    }

    @Test
    void releaseLetsAnotherWriterTakeItAtOnce() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();

        a.release(lease);

        assertThat(a.isHeld(SHARD)).isFalse();
        assertThat(leaseBody().get("released").asBoolean()).isTrue();
        assertThat(b.tryAcquire(SHARD)).isPresent();
        assertThat(leaseBody().has("released")).isFalse();
    }

    @Test
    void releaseAfterTakeoverDoesNotClobberTheNewOwner() throws Exception {
        LeaseManager a = writer("node-a");
        LeaseManager b = writer("node-b");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();
        bucketClock.advance(31);
        Lease taken = b.tryAcquire(SHARD).orElseThrow();

        a.release(lease);

        assertThat(store.get(leaseKey).orElseThrow().etag()).isEqualTo(taken.etag());
        assertThat(leaseBody().get("writer").asText()).isEqualTo("node-b");
        b.assertHeld(SHARD);
    }

    @Test
    void unreadableLeaseIsJudgedByItsAge() throws Exception {
        store.put(leaseKey, "not json".getBytes(StandardCharsets.UTF_8));
        LeaseManager b = writer("node-b");
        assertThat(b.tryAcquire(SHARD)).isEmpty();
        bucketClock.advance(31);
        assertThat(b.tryAcquire(SHARD)).isPresent();
    }

    @Test
    void implicitGuardAlwaysPasses() throws Exception {
        LeaseGuard.IMPLICIT.assertHeld(SHARD);
    }

    /** The in-memory bucket, whose conditional puts can be made to fail with an I/O error. */
    /**
     * 1.11.0 review: a lease lost without a definite answer was never re-read (the renewer skipped
     * it, and only a tick's acquire reclaimed it), so during a run longer than the TTL it stayed lost.
     * Each renewal round re-reads it: past the TTL too, an untouched lease is reclaimed and held again.
     */
    @Test
    void renewalRoundReclaimsAnUnconfirmedLossPastTheTtl() throws Exception {
        AtomicBoolean failPuts = new AtomicBoolean();
        LeaseManager a = writer(failingConditionalPuts(failPuts), "node-a");
        List<LeaseManager.Lost> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        a.onLost(seen::add);
        Lease lease = a.tryAcquire(SHARD).orElseThrow();

        failPuts.set(true);
        a.renewAll();
        assertThat(a.lostUnconfirmed(SHARD)).isTrue();
        bucketClock.advance(31); // the bucket stays unanswering past the TTL
        a.renewAll();
        assertThat(a.lostUnconfirmed(SHARD)).as("still unanswered: stays unconfirmed").isTrue();

        failPuts.set(false);
        a.renewAll();

        assertThat(a.isHeld(SHARD)).as("reclaimed").isTrue();
        assertThat(a.lostUnconfirmed(SHARD)).isFalse();
        assertThat(store.get(leaseKey).orElseThrow().etag()).isNotEqualTo(lease.etag());
        a.assertHeld(SHARD);
        assertThat(seen).extracting(LeaseManager.Lost::definite).containsExactly(false);
    }

    /** A renewal whose answer was lost after the bucket applied it: the lease carries our own body. */
    @Test
    void renewalRoundReclaimsALeaseItsOwnUnansweredRenewalWrote() throws Exception {
        AtomicBoolean loseAnswers = new AtomicBoolean();
        ObjectStore flaky = (ObjectStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ObjectStore.class}, (proxy, m, args) -> {
                    Object out;
                    try {
                        out = m.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (m.getName().equals("putIfMatch") && loseAnswers.get()) throw new IOException("answer lost");
                    return out;
                });
        LeaseManager a = writer(flaky, "node-a");
        Lease lease = a.tryAcquire(SHARD).orElseThrow();
        bucketClock.advance(1);

        loseAnswers.set(true);
        a.renewAll();
        assertThat(a.lostUnconfirmed(SHARD)).isTrue();
        assertThat(store.get(leaseKey).orElseThrow().etag()).as("applied").isNotEqualTo(lease.etag());

        loseAnswers.set(false);
        a.renewAll();

        assertThat(a.isHeld(SHARD)).as("its own write: reclaimed, not taken for another writer's").isTrue();
        a.assertHeld(SHARD);
    }

    /**
     * An unconfirmed loss whose lease another writer took meanwhile is upgraded to a definite loss on
     * the next round that reads the bucket, and the listener is told again, as definite.
     */
    @Test
    void renewalRoundUpgradesAnUnconfirmedLossToDefiniteOnceTaken() throws Exception {
        AtomicBoolean failPuts = new AtomicBoolean();
        LeaseManager a = writer(failingConditionalPuts(failPuts), "node-a");
        List<LeaseManager.Lost> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        a.onLost(seen::add);
        a.tryAcquire(SHARD).orElseThrow();

        failPuts.set(true);
        a.renewAll();
        bucketClock.advance(31);
        Lease taken = writer("node-b").tryAcquire(SHARD).orElseThrow();
        failPuts.set(false);
        a.renewAll();

        assertThat(a.isHeld(SHARD)).isFalse();
        assertThat(a.lostUnconfirmed(SHARD)).as("definite now").isFalse();
        assertThat(seen).extracting(LeaseManager.Lost::definite).containsExactly(false, true);
        assertThat(store.get(leaseKey).orElseThrow().etag()).as("the new owner untouched").isEqualTo(taken.etag());
        assertThatThrownBy(() -> a.assertHeld(SHARD)).isInstanceOf(PublishConflictException.class);
        a.renewAll();
        assertThat(seen).as("a definite loss is reported once").hasSize(2);
    }

    private ObjectStore failingConditionalPuts(AtomicBoolean fail) {
        return (ObjectStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ObjectStore.class}, (proxy, m, args) -> {
                    if (m.getName().startsWith("putIf") && fail.get()) throw new IOException("injected put failure");
                    try {
                        return m.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
