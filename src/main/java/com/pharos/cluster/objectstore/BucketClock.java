package com.pharos.cluster.objectstore;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

/**
 * Liveness and expiry judged by the bucket's clock, never a node's.
 *
 * <p>The bucket stamps every write with its own LastModified. A node that has just written an
 * object of its own (a heartbeat, a lease renewal) takes that object's LastModified as the bucket's
 * "now", and compares other objects' LastModified against it. Clock skew between nodes therefore
 * cannot expire or resurrect anything.
 *
 * <p>Shared by {@code ReaderRegistry} (reader heartbeats) and the writer's {@code LeaseManager}
 * (lease expiry).
 */
public final class BucketClock {

    private BucketClock() {}

    /**
     * The bucket's "now" as seen in a listing: the LastModified of {@code ownKey} (this node's own
     * latest write), or, with no own key ({@code null}: an observer that never writes), the newest
     * LastModified in the listing. Empty when the own key is not listed, or the listing has no
     * timestamps. Entries without a LastModified are ignored.
     */
    public static Optional<Instant> nowFrom(Collection<ObjectInfo> infos, String ownKey) {
        if (ownKey == null) {
            return infos.stream().map(ObjectInfo::lastModified).filter(java.util.Objects::nonNull)
                    .max(Comparator.naturalOrder());
        }
        return infos.stream().filter(i -> i.key().equals(ownKey)).map(ObjectInfo::lastModified)
                .filter(java.util.Objects::nonNull).findFirst();
    }

    /** Age of a write stamped {@code lastModified} at the bucket's {@code now} (negative when it is newer). */
    public static Duration age(Instant lastModified, Instant now) {
        return Duration.between(lastModified, now);
    }

    /** True when a write stamped {@code lastModified} is older than {@code ttl} at the bucket's {@code now}. */
    public static boolean expired(Instant lastModified, Instant now, Duration ttl) {
        return age(lastModified, now).compareTo(ttl) > 0;
    }
}
