package com.pharos.cluster.writer;

import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardId;

/**
 * The publisher's {@link LeaseGuard} on a work-queue writer ({@code cluster.writer.workQueue: true}).
 * Only the {@link WorkLoop} of this process publishes, and only the shard it is running, on its own
 * thread, while its {@link LeaseManager} holds that shard's lease. Everything else is refused with
 * {@link #refusal}: {@code pharos publish}, a hand-run {@code pharos index} (its auto-publish), a
 * run in a process without a work loop, and a run after a graceful stop released the leases.
 */
public final class WorkQueueLeaseGuard implements LeaseGuard {

    private volatile WorkLoop loop;

    /** The work loop whose runs may publish; null detaches it. */
    public void attach(WorkLoop loop) {
        this.loop = loop;
    }

    @Override
    public void assertHeld(ShardId shard) throws PublishConflictException {
        WorkLoop l = loop;
        if (l == null) throw new PublishConflictException(refusal(shard.value()));
        l.assertPublishing(shard);
    }

    /** Why a publish outside the work loop is refused. */
    public static String refusal(String shard) {
        return "this writer runs in work-queue mode (cluster.writer.workQueue: true) and does not hold the lease of"
                + " shard " + shard + " here: shards are indexed and published by the work loop of 'pharos web' on"
                + " the writer holding the lease. Set cluster.writer.workQueue to false to index and publish by hand.";
    }
}
