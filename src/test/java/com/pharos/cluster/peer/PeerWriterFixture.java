package com.pharos.cluster.peer;

import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.config.ClusterConfig;
import io.javalin.Javalin;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A peer-mode writer serving {@code /replication/*} (no shards) with a clock the test moves: the
 * reader registry's monotonic clock ({@link #advance}) and the wall clock it reports ({@code now}).
 */
public final class PeerWriterFixture implements AutoCloseable {

    public static final String TOKEN = "peer-secret";

    public final String node;
    public final AtomicLong nanos = new AtomicLong(System.nanoTime());
    public final AtomicLong wallMillis;
    public final PeerReaderRegistry readers;
    public final PeerStatusState statusState = new PeerStatusState();
    public final ReplicationEndpoint endpoint;
    private final PeerStore store;
    private Javalin app;
    private int port;

    public PeerWriterFixture(String node, Path dir, int readerTtlSeconds, long wallMillis) throws IOException {
        this.node = node;
        this.wallMillis = new AtomicLong(wallMillis);
        this.store = PeerStore.open(dir);
        this.readers = new PeerReaderRegistry(Duration.ofSeconds(readerTtlSeconds), nanos::get);
        this.endpoint = new ReplicationEndpoint(store, node, InternalApi.tokenMatcher(TOKEN), 8, 16, List::of,
                readers, statusState, this.wallMillis::get);
    }

    /** Starts listening (on the same port again after a {@link #stop}). */
    public PeerWriterFixture start() {
        app = Javalin.create();
        endpoint.mount(app);
        app.start(port);
        port = app.port();
        return this;
    }

    public void stop() {
        if (app != null) app.stop();
        app = null;
    }

    /** Moves both clocks of this writer. */
    public void advance(long seconds) {
        nanos.addAndGet(seconds * 1_000_000_000L);
        wallMillis.addAndGet(seconds * 1000);
    }

    public String url() { return "http://localhost:" + port; }

    /** The node ids this writer's registry lists as live. */
    public List<String> liveNodes() {
        return readers.live().stream().map(e -> e.heartbeat().node()).collect(Collectors.toList());
    }

    @Override
    public void close() {
        stop();
        store.store().close();
    }

    // ── reader-side helpers ─────────────────────────────────────────────────

    public static PeerHttp http() {
        return new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
    }

    /** A catalog over {@code seeds:} of the given writers, refreshed once. */
    public static PeerCatalog catalog(PeerHttp http, PeerWriterFixture... writers) {
        String seeds = "seeds:" + java.util.Arrays.stream(writers).map(PeerWriterFixture::url)
                .collect(Collectors.joining(","));
        PeerCatalog c = new PeerCatalog(new PeerDiscovery(PeerDiscovery.parse(seeds), "http",
                PeerDiscovery.SYSTEM_RESOLVER, http), http, Duration.ofSeconds(1), System::nanoTime);
        c.refresh();
        return c;
    }

    public static ClusterConfig cfg(int heartbeatSeconds, int readerTtlSeconds) {
        ClusterConfig c = new ClusterConfig();
        c.setHeartbeatSeconds(heartbeatSeconds);
        c.setReaderTtlSeconds(readerTtlSeconds);
        return c;
    }

    public static ReaderHeartbeat heartbeat(String node, List<String> shards, boolean draining, String state,
                                            List<String> degraded, int inFlight) {
        return new ReaderHeartbeat(node, "http://" + node + ":7070", shards, draining, Map.of("mode", "full"),
                inFlight, 1.5, 0L, state, degraded);
    }
}
