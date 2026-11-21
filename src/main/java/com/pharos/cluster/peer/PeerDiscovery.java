package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

/**
 * Finds the writers of a peer-mode cluster ({@code cluster.peers.discovery}), once per refresh:
 * <ul>
 *   <li>{@code dns:<name>[:port]}: every A/AAAA record of {@code name} is a writer at
 *       {@code <scheme>://<address>:<port>} (port default {@value #DEFAULT_PORT}, the {@code pharos web}
 *       default), such as a Kubernetes headless service. The name is looked up afresh on every
 *       refresh: a direct DNS query ({@link #SYSTEM_RESOLVER}), not the JVM's address cache, whose
 *       TTL ({@code networkaddress.cache.ttl}) would hide writers that came or went.</li>
 *   <li>{@code seeds:<url>,<url>}: the seeds, plus every writer they and the writers they name list
 *       at {@code GET /replication/peers} (gossip, followed transitively within one refresh). A
 *       learned URL nobody lists any more is forgotten after {@link #FORGET_AFTER} refreshes.</li>
 * </ul>
 * Discovery only proposes candidates; {@link PeerCatalog} decides which answer and are live.
 */
public final class PeerDiscovery {

    private static final Logger log = LoggerFactory.getLogger(PeerDiscovery.class);
    public static final int DEFAULT_PORT = 7070;
    /** Learned (gossiped) URLs no reachable writer listed for this many refreshes are forgotten. */
    static final int FORGET_AFTER = 3;
    /** The most URLs one gossip round follows. */
    static final int MAX_PEERS = 256;

    /** A parsed {@code cluster.peers.discovery}. */
    public sealed interface Spec permits Dns, Seeds {
        String text();
    }

    public record Dns(String host, int port, String text) implements Spec {}

    public record Seeds(List<URI> urls, String text) implements Spec {}

    /** Looks a host name up; every address is a writer. */
    @FunctionalInterface
    public interface Resolver {
        List<InetAddress> resolve(String host) throws IOException;
    }

    /**
     * A DNS query for A and AAAA records through JNDI's DNS provider, which keeps no address cache;
     * names DNS does not know (such as {@code localhost} or an {@code /etc/hosts} entry) fall back to
     * {@link InetAddress#getAllByName}.
     */
    public static final Resolver SYSTEM_RESOLVER = PeerDiscovery::lookUp;

    /**
     * @throws IllegalArgumentException naming what is wrong (never the whole value: it is config)
     */
    public static Spec parse(String text) {
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

    private final Spec spec;
    private final String scheme;
    private final Resolver resolver;
    private final PeerHttp http;
    /** Gossiped URLs and the refreshes since a reachable writer last listed them (seeds mode). */
    private final Map<URI, Integer> learned = new HashMap<>();

    /**
     * @param scheme {@code https} when this node has a truststore, else {@code http} (DNS mode; a
     *               seed URL carries its own)
     */
    public PeerDiscovery(Spec spec, String scheme, Resolver resolver, PeerHttp http) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.scheme = Objects.requireNonNull(scheme, "scheme");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.http = Objects.requireNonNull(http, "http");
    }

    public Spec spec() { return spec; }

    /**
     * The candidate writers of this refresh, sorted.
     *
     * @throws IOException when the DNS name cannot be resolved
     */
    public synchronized Set<URI> candidates() throws IOException {
        return switch (spec) {
            case Dns d -> {
                Set<URI> out = new TreeSet<>();
                for (InetAddress a : resolver.resolve(d.host())) {
                    String host = a instanceof Inet6Address ? "[" + a.getHostAddress().replaceAll("%.*$", "") + "]"
                            : a.getHostAddress();
                    out.add(URI.create(scheme + "://" + host + ":" + d.port()));
                }
                yield out;
            }
            case Seeds s -> gossip(s);
        };
    }

    /** Seeds plus everything reachable writers list, followed breadth-first within this refresh. */
    private Set<URI> gossip(Seeds s) {
        Set<URI> all = new TreeSet<>(s.urls());
        Set<URI> listed = new TreeSet<>();
        List<URI> frontier = new ArrayList<>(all);
        frontier.addAll(learned.keySet());
        Set<URI> asked = new java.util.HashSet<>();
        while (!frontier.isEmpty() && asked.size() < MAX_PEERS) {
            List<URI> round = frontier.stream().filter(asked::add).limit(MAX_PEERS).toList();
            frontier = new ArrayList<>();
            List<CompletableFuture<List<URI>>> calls = new ArrayList<>();
            for (URI u : round) calls.add(CompletableFuture.supplyAsync(() -> peersOf(u), PeerHttp.VIRTUAL));
            for (CompletableFuture<List<URI>> c : calls) {
                for (URI p : c.join()) {
                    listed.add(p);
                    if (!asked.contains(p)) frontier.add(p);
                }
            }
        }
        all.addAll(learned.keySet());
        all.addAll(listed);
        // age the learned URLs: listed ones are fresh, others count towards being forgotten
        for (URI u : listed) if (!s.urls().contains(u)) learned.put(u, 0);
        learned.replaceAll((u, n) -> listed.contains(u) ? 0 : n + 1);
        learned.values().removeIf(n -> n >= FORGET_AFTER);
        return all;
    }

    /** The writers {@code u} lists; empty when it does not answer (the catalog counts it missing). */
    private List<URI> peersOf(URI u) {
        try {
            JsonNode n = http.getJson(u, ReplicationEndpoint.PREFIX + "/peers");
            List<URI> out = new ArrayList<>();
            JsonNode peers = n == null ? null : n.get("peers");
            if (peers != null && peers.isArray()) {
                for (JsonNode p : peers) {
                    try {
                        out.add(baseUrl(p.asText()));
                    } catch (IllegalArgumentException bad) {
                        log.debug("writer {} lists an invalid peer URL: {}", u, bad.getMessage());
                    }
                }
            }
            return out;
        } catch (IOException | RuntimeException e) {
            log.debug("gossip with {} failed: {}", u, e.toString());
            return List.of();
        }
    }

    private static List<InetAddress> lookUp(String host) throws IOException {
        List<InetAddress> out = new ArrayList<>();
        try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            javax.naming.directory.DirContext ctx = new javax.naming.directory.InitialDirContext(env);
            try {
                javax.naming.directory.Attributes attrs = ctx.getAttributes(host, new String[]{"A", "AAAA"});
                for (String type : new String[]{"A", "AAAA"}) {
                    javax.naming.directory.Attribute a = attrs.get(type);
                    if (a == null) continue;
                    for (int i = 0; i < a.size(); i++) {
                        out.add(InetAddress.getByName(String.valueOf(a.get(i)))); // a literal: no lookup
                    }
                }
            } finally {
                ctx.close();
            }
        } catch (javax.naming.NamingException | RuntimeException e) {
            log.debug("DNS query for {} failed, using the system resolver: {}", host, e.toString());
        }
        if (!out.isEmpty()) return out;
        try {
            return List.of(InetAddress.getAllByName(host));
        } catch (java.net.UnknownHostException e) {
            throw new IOException("cannot resolve " + host + ": " + e.getMessage(), e);
        }
    }
}
