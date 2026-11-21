package com.pharos.cluster;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.util.Locale;
import java.util.Optional;

/**
 * Where a writer gets a shard's source code: a directory every writer can read ({@code path}) or a
 * git clone URL plus a ref ({@code git}). Stored in the catalog entry, so it never holds
 * credentials: a git URL with a password is refused; credentials come from the writer's environment.
 * {@code embed} false (JSON {@code "embed": false}, recorded by {@code pharos index --no-embed}) makes
 * work-queue writers index the shard without embeddings, as the hand run that recorded it did.
 */
public record ShardSource(Kind kind, String location, String ref, boolean embed) {

    public enum Kind {
        PATH, GIT;

        public String json() { return name().toLowerCase(Locale.ROOT); }

        static Kind parse(String s) {
            for (Kind k : values()) if (k.json().equals(s)) return k;
            throw new IllegalArgumentException("unknown source kind '" + s + "' (expected 'path' or 'git')");
        }
    }

    public ShardSource(Kind kind, String location, String ref) {
        this(kind, location, ref, true);
    }

    public ShardSource {
        if (kind == null) throw new IllegalArgumentException("source kind is required");
        if (location == null || location.isBlank()) throw new IllegalArgumentException("source location is required");
        if (kind == Kind.GIT) {
            if (ref == null || ref.isBlank()) throw new IllegalArgumentException("a git source needs a ref (branch, tag or commit)");
            if (ref.startsWith("-")) throw new IllegalArgumentException("invalid git ref '" + ref + "'");
            if (location.startsWith("-")) throw new IllegalArgumentException("invalid git url");
            rejectCredentials(location);
        } else {
            ref = null;
        }
    }

    public static ShardSource path(String directory) { return new ShardSource(Kind.PATH, directory, null); }

    public static ShardSource git(String url, String ref) { return new ShardSource(Kind.GIT, url, ref); }

    /** This source with embeddings on or off for the shard's work-queue runs. */
    public ShardSource withEmbed(boolean embed) { return new ShardSource(kind, location, ref, embed); }

    /** A URL with a password, or a token as the user name, would be written to the catalog. */
    private static void rejectCredentials(String location) {
        if (!location.contains("://")) return; // scp-style git@host:path carries no password
        String userInfo;
        try {
            userInfo = new URI(location).getRawUserInfo();
        } catch (java.net.URISyntaxException e) {
            // never echo the value: it may hold a secret
            throw new IllegalArgumentException("git url is not a valid URI");
        }
        if (userInfo == null) return;
        String scheme = location.substring(0, location.indexOf("://")).toLowerCase(Locale.ROOT);
        boolean sshUser = scheme.startsWith("ssh") && !userInfo.contains(":");
        if (!sshUser) {
            throw new IllegalArgumentException("git url must not embed credentials; provide them through"
                    + " the writer's environment (credential helper, ssh key or GIT_ASKPASS)");
        }
    }

    public void writeTo(ObjectNode node) {
        node.put("kind", kind.json());
        node.put("location", location);
        if (ref != null) node.put("ref", ref);
        if (!embed) node.put("embed", false); // absent = true: catalogs written before the field read the same
    }

    /** Parses a catalog {@code source} object; empty when absent or not usable. */
    public static Optional<ShardSource> fromJson(com.fasterxml.jackson.databind.JsonNode n) {
        if (n == null || !n.isObject()) return Optional.empty();
        try {
            return Optional.of(new ShardSource(Kind.parse(n.path("kind").asText()), n.path("location").asText(null),
                    n.hasNonNull("ref") ? n.get("ref").asText() : null, n.path("embed").asBoolean(true)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return kind.json() + ":" + location + (ref == null ? "" : "@" + ref) + (embed ? "" : " (no embeddings)");
    }
}
