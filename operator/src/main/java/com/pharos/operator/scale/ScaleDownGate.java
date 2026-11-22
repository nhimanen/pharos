package com.pharos.operator.scale;

import com.pharos.cluster.status.ClusterStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Whether the writers may step down once more. Kubernetes removes the highest ordinals, so every
 * writer pod {@code <sts>-<i>} with {@code i >= replicas} has been (or is being) removed. The next
 * step waits until, in {@code /cluster/status}, each of them
 * <ul>
 *   <li>has no heartbeat left in {@code layout.writers} (it finished its drain and stopped), and</li>
 *   <li>holds no shard lease: every lease naming it is released, or expired and so free for
 *       another writer to take over.</li>
 * </ul>
 * Without a status (no pharos node answered) nothing can be checked, so it never passes.
 */
public final class ScaleDownGate {

    private ScaleDownGate() {}

    /**
     * @param writersSts the writer StatefulSet's name (its pods' names, and so pharos node ids, are
     *                   {@code <writersSts>-<ordinal>})
     * @param replicas   the writer StatefulSet's current {@code replicas}
     * @param status     the latest {@code /cluster/status}, or null when no node answered
     * @return null when the next writer may be removed, otherwise why not
     */
    public static String writerBlocker(String writersSts, int replicas, ClusterStatus status) {
        if (status == null) return "no pharos node answered, so the removed writers' drain cannot be checked";
        List<String> blockers = new ArrayList<>();
        ClusterStatus.Layout layout = status.layout();
        if (layout != null && layout.writers() != null) {
            for (ClusterStatus.Writer w : layout.writers()) {
                if (w != null && removed(writersSts, replicas, w.node())) {
                    blockers.add("writer " + w.node() + " still has a heartbeat"
                            + (w.leases() == null || w.leases().isEmpty() ? "" : " (leases " + w.leases() + ")"));
                }
            }
        }
        if (layout != null && layout.shards() != null) {
            for (ClusterStatus.Shard s : layout.shards()) {
                ClusterStatus.Lease l = s == null ? null : s.lease();
                if (l != null && !l.released() && !l.expired() && removed(writersSts, replicas, l.writer())) {
                    blockers.add("writer " + l.writer() + " still holds the lease of shard " + s.shard());
                }
            }
        }
        return blockers.isEmpty() ? null : String.join("; ", blockers);
    }

    /** True when {@code node} is a pod of the writer StatefulSet with an ordinal at or above {@code replicas}. */
    static boolean removed(String writersSts, int replicas, String node) {
        if (node == null || !node.startsWith(writersSts + "-")) return false;
        String ordinal = node.substring(writersSts.length() + 1);
        if (ordinal.isEmpty() || ordinal.length() > 6 || !ordinal.chars().allMatch(Character::isDigit)) return false;
        return Integer.parseInt(ordinal) >= replicas;
    }
}
