package com.pharos.operator.status;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.Names;
import com.pharos.operator.render.ConfigRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Reads one {@code PharosCluster}'s {@code /cluster/status} straight from its pods, with the cluster
 * token: first every address of the readers' headless Service, then the writers' (every role serves
 * the status). Never through the router, which strips the token.
 *
 * <p>Two threads per cluster, never the reconciler's: a poller that re-reads the status every
 * {@link #REFRESH} (and at once when an event arrives), and one {@code /cluster/events} SSE
 * subscription, whose events only wake the poller. After every read the {@code onUpdate} callback
 * runs (the event source turns it into a reconcile). Each request is bounded by timeouts, and a
 * stream silent for {@link #SSE_IDLE} (pharos sends keep-alive comments) is dropped and reopened, so
 * a hung pod delays nothing but this cluster's next read.
 *
 * <p>The token lives only in memory ({@link #token(String)}) and in the request header; it is never
 * logged, and no message built here contains it.
 */
public final class ClusterStatusClient implements ClusterStatusEventSource.Watch {

    /** The header pharos checks for the token view ({@code InternalApi.TOKEN_HEADER}). */
    public static final String TOKEN_HEADER = "X-Pharos-Cluster-Token";
    public static final Duration REFRESH = Duration.ofSeconds(60);
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    static final Duration SSE_IDLE = Duration.ofSeconds(120);
    static final Duration SSE_RETRY_FIRST = Duration.ofSeconds(5);
    static final Duration SSE_RETRY_MAX = Duration.ofSeconds(60);

    private static final Logger LOG = LoggerFactory.getLogger(ClusterStatusClient.class);

    /** The base URLs ({@code http://ip:port}) behind a Service host name, in the order to try them. */
    @FunctionalInterface
    public interface Resolver {
        List<URI> resolve(String host) throws IOException;
    }

    /** DNS: every address of the (headless) Service, on the pharos web port. */
    public static final Resolver DNS = host -> Arrays.stream(InetAddress.getAllByName(host))
            .map(a -> URI.create("http://" + (a instanceof Inet6Address ? "[" + a.getHostAddress() + "]"
                    : a.getHostAddress()) + ":" + ConfigRenderer.WEB_PORT))
            .toList();

    private final String label;
    private final List<String> services;
    private final Resolver resolver;
    private final HttpClient http;
    private final Clock clock;
    private final Duration refresh;
    private final Runnable onUpdate;
    private final Semaphore wake = new Semaphore(0);

    private volatile String token;
    private volatile StatusReading latest;
    private volatile String problem = "not read yet";
    private volatile boolean closed;
    private volatile Thread poller;
    private volatile Thread events;
    private volatile InputStream stream;
    private volatile Instant streamActivity;

    /**
     * @param label    {@code namespace/name}, for logs
     * @param services the Service host names to try, in order (readers' headless, then writers')
     */
    public ClusterStatusClient(String label, List<String> services, Resolver resolver, HttpClient http, Clock clock,
                               Duration refresh, Runnable onUpdate) {
        this.label = label;
        this.services = List.copyOf(services);
        this.resolver = resolver;
        this.http = http;
        this.clock = clock;
        this.refresh = refresh;
        this.onUpdate = onUpdate == null ? () -> { } : onUpdate;
    }

    /** The client of cluster {@code name} in {@code namespace}: its readers', then its writers' headless Service. */
    public static ClusterStatusClient forCluster(String name, String namespace, HttpClient http, Clock clock,
                                                 Runnable onUpdate) {
        return new ClusterStatusClient(namespace + "/" + name,
                List.of(Names.serviceHost(Names.headless(name, "reader"), namespace),
                        Names.serviceHost(Names.headless(name, "writer"), namespace)),
                DNS, http, clock, REFRESH, onUpdate);
    }

    /** A JDK HTTP client fit for this: HTTP/1.1 (SSE), short connect timeout, no redirects. */
    public static HttpClient httpClient() {
        return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
    }

    @Override
    public void token(String token) {
        String was = this.token;
        this.token = token == null || token.isBlank() ? null : token.strip();
        if (was == null && this.token != null) wake.release(); // read now rather than at the next refresh
    }

    /** The latest read when it succeeded; empty when the last attempt found no node answering. */
    @Override
    public Optional<StatusReading> latest() {
        return Optional.ofNullable(latest);
    }

    @Override
    public String problem() {
        return latest != null ? null : problem;
    }

    /** Starts the poller and the event subscription (virtual threads). */
    public ClusterStatusClient start() {
        wake.drainPermits(); // the first read happens anyway
        poller = Thread.ofVirtual().name("pharos-status-" + label).start(this::pollLoop);
        events = Thread.ofVirtual().name("pharos-events-" + label).start(this::eventLoop);
        return this;
    }

    @Override
    public void close() {
        closed = true;
        closeStream();
        Thread p = poller;
        Thread e = events;
        if (p != null) p.interrupt();
        if (e != null) e.interrupt();
    }

    /**
     * One read: each address of each Service in turn until one answers 200 with a status. Without a
     * token nothing is tried (the status must come with the token).
     */
    public Optional<StatusReading> fetch() {
        String t = token;
        if (t == null) {
            latest = null;
            problem = "the token Secret is not available yet";
            return Optional.empty();
        }
        List<String> failures = new ArrayList<>();
        for (String service : services) {
            for (URI base : addresses(service, failures)) {
                try {
                    HttpRequest req = HttpRequest.newBuilder(base.resolve("/cluster/status"))
                            .timeout(REQUEST_TIMEOUT).header(TOKEN_HEADER, t).header("Accept", "application/json")
                            .GET().build();
                    HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
                    if (resp.statusCode() != 200) {
                        failures.add(base.getAuthority() + " HTTP " + resp.statusCode());
                        continue;
                    }
                    ClusterStatus status = ClusterStatus.MAPPER.readValue(resp.body(), ClusterStatus.class);
                    StatusReading r = new StatusReading(status, clock.instant(), base.getAuthority());
                    latest = r;
                    problem = null;
                    return Optional.of(r);
                } catch (IOException e) {
                    failures.add(base.getAuthority() + " " + e.getClass().getSimpleName());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }
        latest = null;
        problem = "no pharos node answered /cluster/status";
        LOG.debug("PharosCluster {}: no pharos node answered /cluster/status: {}", label, failures);
        return Optional.empty();
    }

    private List<URI> addresses(String service, List<String> failures) {
        try {
            return resolver.resolve(service);
        } catch (UnknownHostException e) {
            failures.add(service + " has no address");
        } catch (IOException | RuntimeException e) {
            failures.add(service + " " + e.getClass().getSimpleName());
        }
        return List.of();
    }

    private void pollLoop() {
        while (!closed) {
            fetch();
            if (closed) return;
            try {
                onUpdate.run();
            } catch (RuntimeException e) {
                LOG.warn("PharosCluster {}: status update handler failed: {}", label, e.toString());
            }
            try {
                wake.tryAcquire(refresh.toMillis(), TimeUnit.MILLISECONDS);
                wake.drainPermits();
            } catch (InterruptedException e) {
                return;
            }
            Instant activity = streamActivity;
            if (stream != null && activity != null && clock.instant().isAfter(activity.plus(SSE_IDLE))) {
                LOG.debug("PharosCluster {}: /cluster/events silent for {}; reopening", label, SSE_IDLE);
                closeStream();
            }
        }
    }

    private void eventLoop() {
        Duration backoff = SSE_RETRY_FIRST;
        while (!closed) {
            boolean streamed = subscribeOnce();
            if (closed) return;
            if (streamed) backoff = SSE_RETRY_FIRST;
            try {
                Thread.sleep(streamed ? 1000 : backoff.toMillis());
            } catch (InterruptedException e) {
                return;
            }
            if (!streamed) backoff = backoff.multipliedBy(2).compareTo(SSE_RETRY_MAX) > 0 ? SSE_RETRY_MAX
                    : backoff.multipliedBy(2);
        }
    }

    /** Opens one stream (first address that accepts) and reads it to its end; true when one was open. */
    boolean subscribeOnce() {
        String t = token;
        if (t == null) return false;
        List<String> failures = new ArrayList<>();
        for (String service : services) {
            for (URI base : addresses(service, failures)) {
                if (closed) return false;
                try {
                    HttpRequest req = HttpRequest.newBuilder(base.resolve("/cluster/events"))
                            .timeout(REQUEST_TIMEOUT).header(TOKEN_HEADER, t).header("Accept", "text/event-stream")
                            .GET().build();
                    HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
                    if (resp.statusCode() != 200) {
                        resp.body().close();
                        failures.add(base.getAuthority() + " HTTP " + resp.statusCode());
                        continue;
                    }
                    read(resp.body());
                    return true;
                } catch (IOException e) {
                    failures.add(base.getAuthority() + " " + e.getClass().getSimpleName());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        LOG.debug("PharosCluster {}: no pharos node accepted /cluster/events: {}", label, failures);
        return false;
    }

    /** Reads SSE until the stream ends; every complete event wakes the poller. */
    private void read(InputStream body) {
        stream = body;
        streamActivity = clock.instant();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            boolean pending = false;
            String line;
            while (!closed && (line = in.readLine()) != null) {
                streamActivity = clock.instant();
                if (line.isEmpty()) {
                    if (pending) wake.release();
                    pending = false;
                } else if (line.startsWith("event:") || line.startsWith("data:")) {
                    pending = true;
                }
            }
        } catch (IOException e) {
            // closed by close(), the idle watchdog, or the pod
        } finally {
            stream = null;
        }
    }

    private void closeStream() {
        InputStream s = stream;
        if (s == null) return;
        try {
            s.close();
        } catch (IOException e) {
            // already gone
        }
    }
}
