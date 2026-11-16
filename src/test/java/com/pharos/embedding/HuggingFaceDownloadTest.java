package com.pharos.embedding;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class HuggingFaceDownloadTest {
    HttpServer server;
    String base;
    int dataStatus = 200;
    boolean truncateOnnx = false;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            byte[] body;
            int status = 200;
            if (path.endsWith("onnx/model.onnx")) body = "MODEL".getBytes();
            else if (path.endsWith("onnx/model.onnx_data")) { body = "DATA".getBytes(); status = dataStatus; }
            else if (path.endsWith("tokenizer.json")) body = "{}".getBytes();
            else if (path.endsWith("config.json")) body = "{}".getBytes();
            else { body = new byte[0]; status = 404; }
            if (truncateOnnx && path.endsWith("onnx/model.onnx")) {
                ex.sendResponseHeaders(200, 1000);
                ex.getResponseBody().write(body);
                ex.getResponseBody().flush();
                ex.close();
                return;
            }
            if (status != 200) { ex.sendResponseHeaders(status, -1); ex.close(); return; }
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort() + "/m/resolve/main/";
    }

    @AfterEach
    void stop() { server.stop(0); }

    @Test
    void fetchesOnnxDataWhenPresent(@TempDir Path dir) throws Exception {
        DjlEmbeddingProvider.ensureModelFiles(base, dir);
        assertEquals("DATA", Files.readString(dir.resolve("model.onnx_data")));
        assertTrue(Files.exists(dir.resolve(".complete")));
    }

    @Test
    void missingOnnxDataIs404AndFine(@TempDir Path dir) throws Exception {
        dataStatus = 404;
        DjlEmbeddingProvider.ensureModelFiles(base, dir);
        assertFalse(Files.exists(dir.resolve("model.onnx_data")));
        assertTrue(Files.exists(dir.resolve("tokenizer.json")));
        assertTrue(Files.exists(dir.resolve(".complete")));
    }

    @Test
    void onnxDataServerErrorFailsLoad(@TempDir Path dir) {
        dataStatus = 500;
        assertThrows(java.io.IOException.class, () -> DjlEmbeddingProvider.ensureModelFiles(base, dir));
        assertFalse(Files.exists(dir.resolve("model.onnx_data")));
        assertFalse(Files.exists(dir.resolve("model.onnx_data.part")));
        assertFalse(Files.exists(dir.resolve(".complete")));
    }

    @Test
    void interruptedDownloadLeavesNoPartialFile(@TempDir Path dir) {
        truncateOnnx = true;
        assertThrows(java.io.IOException.class, () -> DjlEmbeddingProvider.ensureModelFiles(base, dir));
        assertFalse(Files.exists(dir.resolve("model.onnx")));
        assertFalse(Files.exists(dir.resolve("model.onnx.part")));
    }

    @Test
    void dirWithoutMarkerAndPartFilesIsRedownloaded(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("model.onnx"), "HALF");
        Files.writeString(dir.resolve("model.onnx_data.part"), "x");
        DjlEmbeddingProvider.ensureModelFiles(base, dir);
        assertEquals("MODEL", Files.readString(dir.resolve("model.onnx")));
        assertFalse(Files.exists(dir.resolve("model.onnx_data.part")));
    }

    @Test
    void legacyDirIsAcceptedAndMarked(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("model.onnx"), "OLD");
        Files.writeString(dir.resolve("tokenizer.json"), "{}");
        Files.writeString(dir.resolve("config.json"), "{}");
        DjlEmbeddingProvider.ensureModelFiles(base, dir);
        assertEquals("OLD", Files.readString(dir.resolve("model.onnx")));
        assertTrue(Files.exists(dir.resolve(".complete")));
    }

    @Test
    void legacyDirWithoutOnnxDataFetchesIt(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("model.onnx"), "OLD");
        Files.writeString(dir.resolve("tokenizer.json"), "{}");
        DjlEmbeddingProvider.ensureModelFiles(base, dir);
        assertEquals("DATA", Files.readString(dir.resolve("model.onnx_data")));
    }

    @Test
    void followsRedirectHop(@TempDir Path dir) throws Exception {
        server.createContext("/m2/resolve/main/onnx/model.onnx", ex -> {
            ex.getResponseHeaders().add("Location", "/cdn/blob1");
            ex.sendResponseHeaders(302, -1);
            ex.close();
        });
        server.createContext("/cdn/blob1", ex -> {
            byte[] b = "CDNMODEL".getBytes();
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        String redirBase = base.replace("/m/", "/m2/");
        DjlEmbeddingProvider.ensureModelFiles(redirBase, dir);
        assertEquals("CDNMODEL", Files.readString(dir.resolve("model.onnx")));
    }

    @Test
    void chunkedResponseWithoutContentLengthPasses(@TempDir Path dir) throws Exception {
        server.createContext("/m3/resolve/main/onnx/model.onnx", ex -> {
            ex.sendResponseHeaders(200, 0);   // chunked
            ex.getResponseBody().write("CHUNKED".getBytes());
            ex.close();
        });
        DjlEmbeddingProvider.ensureModelFiles(base.replace("/m/", "/m3/"), dir);
        assertEquals("CHUNKED", Files.readString(dir.resolve("model.onnx")));
    }

    @Test
    void concurrentCallsDownloadOnce(@TempDir Path root) throws Exception {
        Path dir = root.resolve("model");
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            var fs = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 4; i++) fs.add(pool.submit(() -> {
                DjlEmbeddingProvider.ensureModelFiles(base, dir);
                return null;
            }));
            for (var f : fs) f.get();
        } finally { pool.shutdown(); }
        assertTrue(Files.exists(dir.resolve(".complete")));
        assertEquals("MODEL", Files.readString(dir.resolve("model.onnx")));
    }

    @Test
    void repoIdsThatEscapeTheCacheAreRejected(@TempDir Path cache) {
        for (String bad : new String[]{"..", ".", "", "a\\b", "../x", "a/..", "x/../../y"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> DjlEmbeddingProvider.resolveModelDir(cache, bad), "id: '" + bad + "'");
        }
        assertEquals(cache.resolve("jinaai-jina-v2"), DjlEmbeddingProvider.resolveModelDir(cache, "jinaai/jina-v2"));
    }

    @Test
    void legacyDirOfflineProbeIsToleratedWithoutMarker(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("model.onnx"), "OLD");
        Files.writeString(dir.resolve("tokenizer.json"), "{}");
        Files.writeString(dir.resolve("config.json"), "{}");
        String unreachable = "http://127.0.0.1:1/m/resolve/main/";
        DjlEmbeddingProvider.ensureModelFiles(unreachable, dir);   // must not throw
        assertFalse(Files.exists(dir.resolve(".complete")));
        assertEquals("OLD", Files.readString(dir.resolve("model.onnx")));

        DjlEmbeddingProvider.ensureModelFiles(base, dir);          // reachable on the next start
        assertTrue(Files.exists(dir.resolve(".complete")));
        assertEquals("DATA", Files.readString(dir.resolve("model.onnx_data")));
    }
}
