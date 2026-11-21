package com.pharos.cluster.writer;

import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardId;

/**
 * The lease check a publisher runs right before its {@code LATEST} CAS: a writer that does not
 * hold the shard's lease must not publish it.
 *
 * <p>The {@code LATEST} CAS still guards publishing on its own; the lease check stops a writer
 * that lost its lease (paused past the TTL, taken over) from even trying.
 */
@FunctionalInterface
public interface LeaseGuard {

    /** Always passes: the writer holds an implicit lease on every shard it indexes (non-work-queue mode). */
    LeaseGuard IMPLICIT = shard -> {};

    /** @throws PublishConflictException when this writer does not (or can no longer show that it does) hold the lease */
    void assertHeld(ShardId shard) throws PublishConflictException;
}
