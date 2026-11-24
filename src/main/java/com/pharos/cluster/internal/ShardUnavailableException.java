package com.pharos.cluster.internal;

import com.pharos.cluster.ShardId;

/** No live reader answered for a shard (none serves it, or the candidates failed). */
public class ShardUnavailableException extends RuntimeException {
    private final ShardId shard;
    private final String detail;

    public ShardUnavailableException(ShardId shard, String detail, Throwable cause) {
        super("shard '" + shard + "' is unavailable" + (detail == null ? "" : ": " + detail), cause);
        this.shard = shard;
        this.detail = detail;
    }

    /** Why, without the shard name (null when unknown). */
    public String detail() { return detail; }

    public ShardId shard() { return shard; }
}
