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

    private final WebServer webServer;
    /** Null in tests; when set, its models are warmed up after the server starts. */
    private final SearchEngine searchEngine;
    /** Null in tests; readers sync their shards before the server starts. */
    private final ClusterRuntime clusterRuntime;

    public WebCommand(WebServer webServer) {
        this(webServer, null);
    }

    public WebCommand(WebServer webServer, SearchEngine searchEngine) {
        this(webServer, searchEngine, null);
    }

    public WebCommand(WebServer webServer, SearchEngine searchEngine, ClusterRuntime clusterRuntime) {
        this.webServer = webServer;
        this.searchEngine = searchEngine;
        this.clusterRuntime = clusterRuntime;
    }

    @Override
    public Integer call() {
        try {
            if (clusterRuntime != null) clusterRuntime.startServing();
            webServer.start(port);
            if (searchEngine != null) searchEngine.warmUpInBackground();
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
