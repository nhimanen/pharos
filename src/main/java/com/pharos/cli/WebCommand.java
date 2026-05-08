package com.pharos.cli;

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

    private final WebServer webServer;

    public WebCommand(WebServer webServer) {
        this.webServer = webServer;
    }

    @Override
    public Integer call() {
        try {
            webServer.start(port);
            boolean noOpen = false;
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
