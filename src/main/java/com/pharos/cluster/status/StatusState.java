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
 *       window has passed, the proposal in force ({@code active} with its {@code reason} and
 *       {@code action});</li>
 *   <li>{@code pendingSince}: per shard, the bucket time its oldest unpublished change was first
 *       seen. It is kept while no live writer reports the shard, so the backlog age survives a
 *       takeover;</li>
 *   <li>{@code layout}: the scale summary last recorded, for the {@code layout} event;</li>
 *   <li>{@code reshards}: per project, the reshard phase and coarse progress step last recorded, for
 *       the {@code reshard} event (absent while no reshard is visible).</li>
 * </ul>
 * All instants are epoch seconds of bucket time. {@code asOf} is the snapshot time of the last
 * write: a node whose snapshot is older never overwrites the state (see {@link Proposals#advance}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"version", "asOf", "rules", "pendingSince", "layout", "reshards"})
public record StatusState(int version, long asOf, SortedMap<String, Rule> rules, SortedMap<String, Long> pendingSince,
                          @JsonInclude(JsonInclude.Include.NON_NULL) Summary layout,
                          @JsonInclude(JsonInclude.Include.NON_EMPTY) SortedMap<String, ReshardMark> reshards) {

    public static final int VERSION = 1;
    public static final StatusState EMPTY = new StatusState(VERSION, 0, null, null, null);

    public StatusState {
        rules = Collections.unmodifiableSortedMap(rules == null ? new TreeMap<>() : new TreeMap<>(rules));
        pendingSince = Collections.unmodifiableSortedMap(pendingSince == null ? new TreeMap<>()
                : new TreeMap<>(pendingSince));
        reshards = Collections.unmodifiableSortedMap(reshards == null ? new TreeMap<>() : new TreeMap<>(reshards));
    }

    /** A state without reshard marks (as before Phase 6). */
    public StatusState(int version, long asOf, SortedMap<String, Rule> rules, SortedMap<String, Long> pendingSince,
                       Summary layout) {
        this(version, asOf, rules, pendingSince, layout, null);
    }

    /** True when both hold the same content, {@code asOf} aside (nothing worth a write changed). */
    public boolean sameContent(StatusState o) {
        return o != null && rules.equals(o.rules) && pendingSince.equals(o.pendingSince)
                && java.util.Objects.equals(layout, o.layout) && reshards.equals(o.reshards);
    }

    /**
     * A project's reshard as last recorded: its spec version, phase and coarse progress step, with the
     * counts seen when that step was recorded. A new mark (and a {@code reshard} event) is recorded only
     * when the version, phase or step changes, never for every file or shard.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"specVersion", "phase", "step", "shardsDone", "shardsTotal", "filesDone", "filesTotal"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ReshardMark(int specVersion, String phase, int step, Integer shardsDone, Integer shardsTotal,
                              Integer filesDone, Integer filesTotal) {

        /** True when {@code o} is the same version, phase and step (the counts aside). */
        public boolean sameStep(ReshardMark o) {
            return o != null && o.specVersion == specVersion && java.util.Objects.equals(o.phase, phase)
                    && o.step == step;
        }
    }

    /**
     * One rule's hysteresis. {@code reason} and {@code action} are set only while {@code active}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"kind", "target", "since", "falseSince", "active", "reason", "action"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Rule(String kind, String target, long since, Long falseSince, boolean active, String reason,
                       String action) {}

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
