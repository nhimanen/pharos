package com.pharos.cli;

import com.pharos.cluster.ClusterRuntime;
import com.pharos.search.SearchEngine;
import com.pharos.web.WebServer;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "web",
        description = "Start the web UI for browsing the code knowledge graph in a browser",
        mixinStandardHelpOptions = true
)
public class WebCommand implements Callable<Integer> {

    @Option(names = {"--port"},
            description = "HTTP port to listen on (default: 7070)",
            defaultValue = "7070")
    private int port;

    @Option(names = {"--no-open"},
            description = "Start the server without opening a browser window (useful for daemon mode)")
    private boolean noOpen;

    /** The longest a reader's readiness waits for its search models before serving without them. */
    static final java.time.Duration MODEL_WARMUP_TIMEOUT = java.time.Duration.ofMinutes(5);

    private final WebServer webServer;
    /** Null in tests; when set, its models are warmed up after the server starts. */
    private final SearchEngine searchEngine;
    /**
     * Null in tests. Readers start syncing in the background and the port is bound at once;
     * {@code /health} answers 503 {@code warming} until the first sync is served and the search
     * models have loaded.
     */
    private final ClusterRuntime clusterRuntime;

    public WebCommand(WebServer webServer) {
        this(webServer, null);
    }

    public WebCommand(WebServer webServer, SearchEngine searchEngine) {
        this(webServer, searchEngine, null);
    }

    /** Whether the search-model warm-up is skipped: on {@code role: writer}. */
    boolean isWriter() {
        return clusterRuntime != null && clusterRuntime.isWriter();
    }

    public WebCommand(WebServer webServer, SearchEngine searchEngine, ClusterRuntime clusterRuntime) {
        this.webServer = webServer;
        this.searchEngine = searchEngine;
        this.clusterRuntime = clusterRuntime;
    }

    @Override
    public Integer call() {
        com.pharos.ShutdownSequence.install(); // a signal stops it in order (drain, model calls, hooks)
        try {
            // The model warm-up starts first and runs in parallel with a reader's first sync;
            // readiness waits for both (a hanging load is bounded by MODEL_WARMUP_TIMEOUT).
            // A writer serves no search: it never warms (loads) the search models, which on a local
            // ONNX model is ~1.1 GB per process. Its index runs load the embedding models they use.
            if (searchEngine != null && !isWriter()) searchEngine.warmUpInBackground();
            if (clusterRuntime != null) {
                clusterRuntime.setLoad(webServer.load()); // before the first heartbeat
                clusterRuntime.setInFlight(webServer::inFlightRequests); // what the shutdown drain waits for
                clusterRuntime.beginServing(searchEngine != null && !isWriter() // a writer warms nothing
                        ? searchEngine.modelsSettled(MODEL_WARMUP_TIMEOUT)
                        : java.util.concurrent.CompletableFuture.completedFuture(null), // no wait
                        true); // serves /internal/*: joins reader membership, drains on shutdown
                webServer.setReadiness(clusterRuntime::ready);
                webServer.setDraining(clusterRuntime::draining);
                webServer.setReaderCacheStats(clusterRuntime::readerCacheStats);
                webServer.setDegradation(clusterRuntime::degradation);
                webServer.setDegradedShards(clusterRuntime::degradedShards);
                webServer.setLiveReaders(clusterRuntime::liveReaders);
                // With the engine, this reader also coordinates queries over shards it does not hold.
                com.pharos.cluster.internal.InternalApi internal = searchEngine != null
                        ? clusterRuntime.internalApi(searchEngine) : clusterRuntime.internalApi();
                if (internal != null) webServer.setInternalApi(internal);
                // a work-queue writer indexes and publishes the shards it holds the lease of
                if (clusterRuntime.beginWriting()) {
                    System.out.println("Writer work queue running (cluster.writer.workQueue)");
                    webServer.setWriterDisk(clusterRuntime::writerDisk);
                }
                // /cluster/status and /cluster/events on every role (single node: from local state)
                webServer.setClusterStatus(clusterRuntime.statusService());
                webServer.setClusterToken(clusterRuntime.clusterTokenMatcher()); // token view: reader addresses
                // a peer-mode writer serves its published generations to readers (token-guarded)
                clusterRuntime.replicationEndpoint().ifPresent(e -> {
                    webServer.setReplicationEndpoint(e);
                    System.out.println("Serving /replication/* (cluster.backend: peer-pull)");
                });
                clusterRuntime.serverTls().ifPresent(t -> webServer.setTls(
                        new WebServer.Tls(t.getKeystore(), t.getKeystoreType(), t.password())));
            }
            webServer.start(port);
            if (!noOpen) {
                tryOpenBrowser("http://localhost:" + port);
            }
            // Block until Ctrl+C
            Thread.currentThread().join();
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            System.err.println("Web server error: " + e.getMessage());
            return 1;
        }
    }

    private static void tryOpenBrowser(String url) {
        try {
            String os = System.getProperty("os.name", "").toLowerCase();
            String[] cmd;
            if (os.contains("mac")) cmd = new String[]{"open", url};
            else if (os.contains("win")) cmd = new String[]{"cmd", "/c", "start", url};
            else cmd = new String[]{"xdg-open", url};
            new ProcessBuilder(cmd).start();
        } catch (Exception ignored) {}
    }
}
