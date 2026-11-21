package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BucketClockTest {

    private static final Instant T = Instant.parse("2026-11-21T10:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(30);

    @Test
    void ageIsInWholeSecondsOfBothInstants() {
        // A listing stamp with milliseconds against a HEAD stamp of the same write (seconds only).
        assertThat(BucketClock.age(T.plusMillis(999), T)).isZero();
        assertThat(BucketClock.age(T, T.plusMillis(999))).isZero();
        assertThat(BucketClock.age(T.plusMillis(900), T.plusSeconds(1).plusMillis(100))).isEqualTo(Duration.ofSeconds(1));
        assertThat(BucketClock.age(T.plusSeconds(5), T)).isEqualTo(Duration.ofSeconds(-5));
    }

    @Test
    void mixedPrecisionDoesNotExpireEarly() {
        // Lease stat'ed by HEAD at :00 (really :00.900), heartbeat listed at :30.100: 29.2 s apart, not expired.
        Instant leaseHead = T;
        Instant heartbeatListed = T.plusSeconds(30).plusMillis(100);
        assertThat(BucketClock.expired(leaseHead, heartbeatListed, TTL)).isFalse();
        assertThat(BucketClock.expired(T.plusMillis(900), heartbeatListed, TTL)).isFalse();
    }

    @Test
    void expiryBoundaryIsStrictlyOlderThanTheTtl() {
        assertThat(BucketClock.expired(T, T.plusSeconds(30), TTL)).isFalse();
        assertThat(BucketClock.expired(T, T.plusSeconds(30).plusMillis(999), TTL)).isFalse();
        assertThat(BucketClock.expired(T, T.plusSeconds(31), TTL)).isTrue();
    }

    @Test
    void nowIsTheOwnKeyOrTheNewestForAnObserver() {
        List<ObjectInfo> infos = List.of(new ObjectInfo("a", 1, T.plusSeconds(3)),
                new ObjectInfo("b", 1, T.plusSeconds(9)), new ObjectInfo("c", 1, null));
        assertThat(BucketClock.nowFrom(infos, "a")).contains(T.plusSeconds(3));
        assertThat(BucketClock.nowFrom(infos, null)).contains(T.plusSeconds(9));
        assertThat(BucketClock.nowFrom(infos, "c")).isEmpty();
        assertThat(BucketClock.nowFrom(infos, "missing")).isEmpty();
    }
}
