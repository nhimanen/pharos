package com.pharos.cluster.peer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * A parsed {@code cluster.peers.discovery}: {@code dns:<name>[:port]} or {@code seeds:<url>,<url>}.
 * Only the syntax lives here, so {@code ClusterConfig} validates without the peer runtime;
 * {@code PeerDiscovery} does the lookups.
 */
public sealed interface DiscoverySpec permits DiscoverySpec.Dns, DiscoverySpec.Seeds {

    /** The writer port when a {@code dns:} spec names none (the {@code pharos web} default). */
    int DEFAULT_PORT = 7070;

    /** The value as configured (stripped). */
    String text();

    record Dns(String host, int port, String text) implements DiscoverySpec {}

    record Seeds(List<URI> urls, String text) implements DiscoverySpec {}

    /**
     * @throws IllegalArgumentException naming what is wrong (never the whole value: it is config)
     */
    static DiscoverySpec parse(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("cluster.peers.discovery is empty (expected dns:<name>[:port] or"
                    + " seeds:<url>,<url>)");
        }
        String t = text.strip();
        String lower = t.toLowerCase(Locale.ROOT);
        if (lower.startsWith("dns:")) {
            String rest = t.substring(4).strip();
            String host = rest;
            int port = DEFAULT_PORT;
            int colon = rest.lastIndexOf(':');
            if (colon >= 0) {
                host = rest.substring(0, colon);
                try {
                    port = Integer.parseInt(rest.substring(colon + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("cluster.peers.discovery: invalid port in '" + t + "'");
                }
                if (port < 1 || port > 65535) {
                    throw new IllegalArgumentException("cluster.peers.discovery: port out of range in '" + t + "'");
                }
            }
            if (host.isEmpty() || !host.matches("[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?\\.?")) {
                throw new IllegalArgumentException("cluster.peers.discovery: invalid DNS name in '" + t + "'");
            }
            return new Dns(host, port, t);
        }
        if (lower.startsWith("seeds:")) {
            List<URI> urls = new ArrayList<>();
            for (String s : t.substring(6).split(",")) {
                if (s.isBlank()) continue;
                try {
                    urls.add(baseUrl(s.strip()));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("cluster.peers.discovery: " + e.getMessage());
                }
            }
            if (urls.isEmpty()) throw new IllegalArgumentException("cluster.peers.discovery: no seed URL in '" + t + "'");
            return new Seeds(List.copyOf(new LinkedHashSet<>(urls)), t);
        }
        throw new IllegalArgumentException("cluster.peers.discovery must start with dns: or seeds:, got '" + t + "'");
    }

    /** A writer's base URL: http(s), a host, no user info, no query; path and trailing slash dropped. */
    static URI baseUrl(String s) {
        URI u;
        try {
            u = new URI(s);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("not a valid writer URL: '" + s + "'");
        }
        String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
        if (u.getRawUserInfo() != null) { // never echo it: it may hold a password
            throw new IllegalArgumentException("a writer URL must not carry user info (the cluster token authenticates)");
        }
        if (!(scheme.equals("http") || scheme.equals("https")) || u.getHost() == null || u.getRawUserInfo() != null
                || u.getRawQuery() != null) {
            throw new IllegalArgumentException("a writer URL must be http(s)://host[:port], got '" + s + "'");
        }
        try {
            return new URI(scheme, null, u.getHost(), u.getPort(), null, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("not a valid writer URL: '" + s + "'");
        }
    }
}
