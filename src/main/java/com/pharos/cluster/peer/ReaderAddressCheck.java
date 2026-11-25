package com.pharos.cluster.peer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which reader heartbeats a peer-mode writer admits (P5-R12). Coordinators and {@code pharos router}
 * send shard calls, users' queries and the cluster token to every advertised reader URL, so the token
 * alone must not let a client name any URL it likes: a heartbeat is admitted only when the host of its
 * {@code advertiseUrl}
 * <ul>
 *   <li>resolves to the address the heartbeat came from (any of its addresses; two loopback addresses
 *       count as one host), or</li>
 *   <li>is listed in {@code cluster.peers.allowHosts} (a reader behind NAT or a proxy, whose
 *       heartbeats arrive from another address).</li>
 * </ul>
 * A host that does not resolve is refused. A heartbeat without an {@code advertiseUrl} names no URL
 * and is admitted (nobody routes to it). Each refused host is logged at WARN once.
 */
public final class ReaderAddressCheck {

    private static final Logger log = LoggerFactory.getLogger(ReaderAddressCheck.class);

    /** The JVM resolver ({@link InetAddress#getAllByName}, with its short positive cache). */
    public static final PeerDiscovery.Resolver JVM_RESOLVER = host -> {
        try {
            return List.of(InetAddress.getAllByName(host));
        } catch (UnknownHostException e) {
            throw new IOException("cannot resolve " + host, e);
        }
    };

    private final Set<String> allowHosts;
    private final PeerDiscovery.Resolver resolver;
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /**
     * @param allowHosts {@code cluster.peers.allowHosts} (null: none)
     * @param resolver   resolves an advertised host name
     */
    public ReaderAddressCheck(Collection<String> allowHosts, PeerDiscovery.Resolver resolver) {
        Set<String> hosts = new HashSet<>();
        if (allowHosts != null) allowHosts.forEach(h -> hosts.add(normalize(h)));
        this.allowHosts = Set.copyOf(hosts);
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    /** No {@code allowHosts}, the JVM resolver. */
    public static ReaderAddressCheck strict() {
        return new ReaderAddressCheck(null, JVM_RESOLVER);
    }

    /**
     * Whether a heartbeat advertising {@code advertiseUrl} and received from {@code remoteAddr} (an IP
     * literal, as the server reports the peer address) is admitted.
     *
     * @return null when admitted, else why not (never echoes more than the host)
     */
    public String refusal(String advertiseUrl, String remoteAddr) {
        if (advertiseUrl == null || advertiseUrl.isBlank()) return null;
        String host;
        try {
            host = URI.create(advertiseUrl.strip()).getHost();
        } catch (IllegalArgumentException e) {
            return refuse("(unparseable)", "its advertiseUrl is not a URL", remoteAddr);
        }
        if (host == null || host.isEmpty()) return refuse("(none)", "its advertiseUrl has no host", remoteAddr);
        String h = normalize(host);
        if (allowHosts.contains(h)) return null;
        InetAddress remote;
        try {
            remote = literal(remoteAddr);
        } catch (IOException e) {
            return refuse(h, "the request's remote address is unknown", remoteAddr);
        }
        List<InetAddress> resolved;
        try {
            resolved = isLiteral(h) ? List.of(literal(h)) : resolver.resolve(h);
        } catch (IOException | RuntimeException e) {
            return refuse(h, "its host does not resolve", remoteAddr);
        }
        for (InetAddress a : resolved) {
            if (a.equals(remote) || (a.isLoopbackAddress() && remote.isLoopbackAddress())) return null;
        }
        return refuse(h, "its host does not resolve to the address the heartbeat came from and is not in"
                + " cluster.peers.allowHosts", remoteAddr);
    }

    private String refuse(String host, String why, String remoteAddr) {
        String msg = "reader heartbeat refused: advertised host " + host + ": " + why;
        if (warned.add(host)) {
            log.warn("Refusing reader heartbeats advertising host {} (from {}): {}. A reader behind NAT or a proxy"
                    + " needs its host in cluster.peers.allowHosts (logged once per host)", host, remoteAddr, why);
        }
        return msg;
    }

    private static InetAddress literal(String addr) throws IOException {
        if (addr == null || addr.isBlank()) throw new IOException("no address");
        String a = addr.strip();
        if (a.startsWith("[") && a.endsWith("]")) a = a.substring(1, a.length() - 1);
        try {
            return InetAddress.ofLiteral(a);
        } catch (IllegalArgumentException e) {
            throw new IOException("not an IP literal", e);
        }
    }

    private static boolean isLiteral(String host) {
        return host.indexOf(':') >= 0 || host.matches("[0-9.]+");
    }

    private static String normalize(String host) {
        String h = host.strip().toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) h = h.substring(1, h.length() - 1);
        return h;
    }
}
