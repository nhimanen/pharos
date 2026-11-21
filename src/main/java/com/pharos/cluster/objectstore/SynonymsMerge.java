package com.pharos.cluster.objectstore;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Merges a writer's local {@code synonyms.txt} into the published cluster-wide one, replacing only
 * the publishing project's auto-mined section ({@code ConceptMiner}: a {@code # ── Auto-mined from
 * '<project>' …} header and rule lines tagged {@code # auto:<project>:}). Every other project's
 * section is kept from the published file, so a publish never drops another shard's rules.
 *
 * <p>The output is canonical, whatever order writers publish in: the hand-written lines (every
 * non-blank line no project owns, from the publishing writer's file, as before: hand rules are
 * configuration and must be the same on every writer), then each project's section in project-name
 * order, one blank line before each, duplicate lines dropped.
 *
 * <p>The publishing project's section comes from the local file only when the local file has one:
 * a writer that has not mined the project (it took the shard over and its runs mined nothing yet)
 * keeps the published rules rather than dropping them. A first publish to a bucket without the
 * file takes every section of the local file (the Phase 1–3 behaviour).
 */
public final class SynonymsMerge {

    private static final Pattern RULE = Pattern.compile("# auto:([^:\\s]+):");
    private static final Pattern HEADER = Pattern.compile("# ── Auto-mined from '([^']+)'");

    private SynonymsMerge() {}

    /** One parsed file: hand-written lines and each project's section. */
    private record Parsed(List<String> hand, Map<String, Set<String>> sections) {}

    /** The project that owns {@code line}; null for a hand-written (or blank) line. */
    static String owner(String line) {
        Matcher m = RULE.matcher(line);
        if (m.find()) return m.group(1);
        m = HEADER.matcher(line);
        return m.find() ? m.group(1) : null;
    }

    private static Parsed parse(String text) {
        List<String> hand = new ArrayList<>();
        Map<String, Set<String>> sections = new TreeMap<>();
        if (text == null) return new Parsed(hand, sections);
        for (String raw : text.split("\\R", -1)) {
            String line = raw.stripTrailing();
            if (line.isBlank()) continue;
            String project = owner(line);
            if (project == null) {
                hand.add(line);
            } else {
                sections.computeIfAbsent(project, p -> new LinkedHashSet<>()).add(line);
            }
        }
        return new Parsed(hand, sections);
    }

    /**
     * The file reduced to its hand-written lines and the sections of {@code projects}, in the same
     * canonical layout as {@link #merge}: what a peer-mode writer serves for the shards it owns
     * (ruling P5-R4), so every reader can merge the writers' files with {@link #merge}'s rules.
     *
     * @param text     a synonyms file; null is treated as empty
     * @param projects the projects whose sections are kept
     */
    public static String retain(String text, java.util.Collection<String> projects) {
        Parsed parsed = parse(text);
        StringBuilder out = new StringBuilder();
        for (String line : new LinkedHashSet<>(parsed.hand())) out.append(line).append('\n');
        for (Map.Entry<String, Set<String>> e : parsed.sections().entrySet()) {
            if (!projects.contains(e.getKey())) continue;
            if (!out.isEmpty()) out.append('\n');
            for (String line : e.getValue()) out.append(line).append('\n');
        }
        return out.toString();
    }

    /**
     * @param published the cluster's current file; null when the bucket has none
     * @param local     the publishing writer's file
     * @param project   the project being published
     * @return the file to publish
     */
    public static String merge(String published, String local, String project) {
        Parsed mine = parse(local);
        Map<String, Set<String>> sections;
        if (published == null) {
            sections = mine.sections();
        } else {
            sections = new TreeMap<>(parse(published).sections());
            Set<String> own = mine.sections().get(project);
            if (own != null) sections.put(project, own);
        }
        StringBuilder out = new StringBuilder();
        for (String line : new LinkedHashSet<>(mine.hand())) out.append(line).append('\n');
        for (Set<String> section : sections.values()) {
            if (!out.isEmpty()) out.append('\n');
            for (String line : section) out.append(line).append('\n');
        }
        return out.toString();
    }
}
