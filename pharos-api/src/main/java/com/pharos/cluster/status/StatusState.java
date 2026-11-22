package com.pharos.cluster.status;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The small shared object behind {@code /cluster/status}'s hysteresis, {@code catalog/status/state.json}
 * (an in-memory copy on a single node). It holds only what a snapshot cannot show by itself, so that
 * every node agrees on it:
 * <ul>
 *   <li>{@code rules}: per proposal id, since when its condition holds ({@code since}), since when
 *       it no longer does ({@code falseSince}, while the clear grace runs), and, once the rule's
 *       window has passed, the proposal in force ({@code active} with its {@code reason},
 *       {@code action} and {@code delta});</li>
 *   <li>{@code pendingSince}: per shard, the bucket time its oldest unpublished change was first
 *       seen. It is kept while no live writer reports the shard, so the backlog age survives a
 *       takeover;</li>
 *   <li>{@code layout}: the scale summary last recorded, for the {@code layout} event.</li>
 * </ul>
 * All instants are epoch seconds of bucket time. {@code asOf} is the snapshot time of the last
 * write: a node whose snapshot is older never overwrites the state (see {@code Proposals.advance}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"version", "asOf", "rules", "pendingSince", "layout"})
public record StatusState(int version, long asOf, SortedMap<String, Rule> rules, SortedMap<String, Long> pendingSince,
                          @JsonInclude(JsonInclude.Include.NON_NULL) Summary layout) {

    public static final int VERSION = 1;
    public static final StatusState EMPTY = new StatusState(VERSION, 0, null, null, null);

    public StatusState {
        rules = Collections.unmodifiableSortedMap(rules == null ? new TreeMap<>() : new TreeMap<>(rules));
        pendingSince = Collections.unmodifiableSortedMap(pendingSince == null ? new TreeMap<>()
                : new TreeMap<>(pendingSince));
    }

    /** True when both hold the same content, {@code asOf} aside (nothing worth a write changed). */
    public boolean sameContent(StatusState o) {
        return o != null && rules.equals(o.rules) && pendingSince.equals(o.pendingSince)
                && java.util.Objects.equals(layout, o.layout);
    }

    /**
     * One rule's hysteresis. {@code reason}, {@code action} and {@code delta} (the signed node count
     * of a scaling proposal, see {@code Proposals}) are set only while {@code active}. A node older
     * than {@code delta} drops it when it rewrites the state; it is back on the next write by a newer
     * node, and meanwhile {@code Proposals.proposal} fills it from the current candidate.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"kind", "target", "since", "falseSince", "active", "reason", "action", "delta"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Rule(String kind, String target, long since, Long falseSince, boolean active, String reason,
                       String action, Integer delta) {}

    /** The scale of the cluster: shard, live writer and live reader counts. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"shards", "writers", "readers"})
    public record Summary(int shards, int writers, int readers) {}

    public byte[] toJson() throws IOException {
        return ClusterStatus.MAPPER.writeValueAsBytes(this);
    }

    public static StatusState read(byte[] json) throws IOException {
        return ClusterStatus.MAPPER.readValue(json, StatusState.class); // unknown fields of a newer node ignored
    }
}
