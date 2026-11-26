package com.pharos.cluster.membership;

import com.pharos.cluster.ArithmeticProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Warns when the live readers do not all score vectors with the same bits (ruling CPU-R1): their heartbeats'
 * {@link ArithmeticProfile arithmetic profiles} differ, so a fan-out can order exact ties differently depending
 * on which replica answers. Typically a reader started without the deterministic flags (a custom launcher, or
 * {@code JAVA_OPTS} overriding them). Each membership source keeps one and feeds it every refreshed list; it warns
 * once per distinct set of profiles, so a steady mixed cluster logs once, not every heartbeat.
 * Heartbeats without a profile (an older reader's) are left out.
 */
public final class ArithmeticDivergence {

    private static final Logger log = LoggerFactory.getLogger(ArithmeticDivergence.class);

    private Set<String> warned = Set.of();

    /** Logs the WARN of {@link #describe} when the set of profiles is new; true when it did. */
    public synchronized boolean observe(Collection<ReaderHeartbeat> live) {
        Map<String, List<String>> groups = groups(live);
        if (groups.size() <= 1) {
            warned = Set.of();
            return false;
        }
        if (groups.keySet().equals(warned)) return false;
        warned = Set.copyOf(groups.keySet());
        log.warn(describe(groups));
        return true;
    }

    /** The readers by arithmetic, as {@code summary -> nodes}, sorted; readers without a profile left out. */
    static Map<String, List<String>> groups(Collection<ReaderHeartbeat> live) {
        Map<String, List<String>> out = new TreeMap<>();
        for (ReaderHeartbeat hb : live) {
            if (hb == null || hb.arithmetic() == null) continue;
            out.computeIfAbsent(hb.arithmetic().summary(), k -> new ArrayList<>()).add(hb.node());
        }
        out.values().forEach(l -> l.sort(null));
        return out;
    }

    /** The WARN text for more than one group: which nodes run which arithmetic, and what to do. */
    static String describe(Map<String, List<String>> groups) {
        StringBuilder sb = new StringBuilder("Live readers score vectors with different arithmetic, so replicas can"
                + " order exact score ties differently:");
        groups.forEach((summary, nodes) -> sb.append(' ').append(nodes).append(" ").append(summary).append(';'));
        sb.setLength(sb.length() - 1);
        sb.append(". Start every reader with PHAROS_ARITHMETIC=deterministic (JVM flags ")
                .append(String.join(" ", ArithmeticProfile.DETERMINISTIC_FLAGS.subList(0, 2)))
                .append(", without --add-modules ").append(ArithmeticProfile.VECTOR_MODULE)
                .append("); see the README, \"Multi-node CPU determinism\".");
        return sb.toString();
    }
}
