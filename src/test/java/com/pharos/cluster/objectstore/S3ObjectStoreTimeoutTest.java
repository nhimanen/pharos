package com.pharos.cluster.objectstore;

import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Final review I2: a bucket endpoint that accepts connections but never answers. Ranged reads give
 * up after {@code cluster.objectStore.readTimeoutSeconds} per attempt and three times that in total,
 * instead of the SDK's ~90 s of socket timeouts and retries.
 */
class S3ObjectStoreTimeoutTest {

    @Test
    void rangedReadOfAnUnansweringEndpointTimesOut() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            List<Socket> accepted = new ArrayList<>();
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) accepted.add(server.accept()); // read nothing, answer nothing
                } catch (IOException closed) {
                    // the test is over
                }
            }, "silent-endpoint");
            acceptor.setDaemon(true);
            acceptor.start();

            ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
            cfg.setBucket("b");
            cfg.setEndpoint("http://127.0.0.1:" + server.getLocalPort());
            cfg.setReadTimeoutSeconds(1);
            try (S3ObjectStore s3 = new S3ObjectStore(cfg,
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))) {
                long start = System.nanoTime();
                assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                        assertThatThrownBy(() -> s3.openRange("some/key", 0, 100).close())
                                .isInstanceOf(IOException.class)
                                .hasMessageContaining("some/key"));
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertThat(millis).as("bounded by the 3 s total, not the SDK's socket timeouts")
                        .isBetween(900L, 8_000L);
            } finally {
                for (Socket s : accepted) s.close();
            }
        }
    }

    /**
     * Phase 2 follow-up M1: the client-wide socket timeout (idle time between reads of any call, the
     * writer's uploads and whole-object downloads included) is at least 30 s; only ranged reads keep
     * the tight per-request bounds of {@code readTimeoutSeconds}.
     */
    @Test
    void clientSocketTimeoutIsAtLeastThirtySeconds() {
        ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
        cfg.setReadTimeoutSeconds(1);
        assertThat(S3ObjectStore.clientSocketTimeout(cfg)).isEqualTo(Duration.ofSeconds(30));
        cfg.setReadTimeoutSeconds(45);
        assertThat(S3ObjectStore.clientSocketTimeout(cfg)).isEqualTo(Duration.ofSeconds(45));
    }

    /** A whole-object GET whose answer takes longer than readTimeoutSeconds still succeeds. */
    @Test
    void slowWholeObjectGetIsNotCutByTheRangedReadTimeout() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = new Thread(() -> {
                try {
                    while (true) {
                        Socket s = server.accept();
                        Thread.ofVirtual().start(() -> {
                            try (s) {
                                var in = new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream()));
                                String line;
                                while ((line = in.readLine()) != null && !line.isEmpty()) { /* headers */ }
                                Thread.sleep(2_500); // longer than readTimeoutSeconds = 1
                                byte[] body = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                                s.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length
                                        + "\r\nETag: \"e1\"\r\nConnection: close\r\n\r\n")
                                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                                s.getOutputStream().write(body);
                                s.getOutputStream().flush();
                            } catch (Exception ignored) {
                                // the test is over
                            }
                        });
                    }
                } catch (IOException closed) {
                    // the test is over
                }
            }, "slow-endpoint");
            responder.setDaemon(true);
            responder.start();

            ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
            cfg.setBucket("b");
            cfg.setEndpoint("http://127.0.0.1:" + server.getLocalPort());
            cfg.setReadTimeoutSeconds(1);
            try (S3ObjectStore s3 = new S3ObjectStore(cfg,
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))) {
                assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                        assertThat(s3.get("some/key")).hasValueSatisfying(o ->
                                assertThat(new String(o.data(), java.nio.charset.StandardCharsets.UTF_8))
                                        .isEqualTo("hello")));
            }
        }
    }

    /** An endpoint that accepts connections and never answers; closed with the returned handle. */
    private static AutoCloseable silentEndpoint(int[] port) throws IOException {
        ServerSocket server = new ServerSocket(0);
        List<Socket> accepted = new java.util.concurrent.CopyOnWriteArrayList<>();
        Thread acceptor = new Thread(() -> {
            try {
                while (true) accepted.add(server.accept());
            } catch (IOException closed) {
                // the test is over
            }
        }, "silent-endpoint");
        acceptor.setDaemon(true);
        acceptor.start();
        port[0] = server.getLocalPort();
        return () -> {
            server.close();
            for (Socket s : accepted) s.close();
        };
    }

    /**
     * Gate run 2 for 1.9.0: one heartbeat put on a saturated MinIO took about 4 minutes (a 30 s
     * socket timeout per attempt, 3 attempts, no call timeout). Every control-plane call (put, the
     * conditional puts, get, stat, list, delete) is now bounded by {@code callAttemptTimeoutSeconds}
     * per attempt and {@code callTimeoutSeconds} in total.
     */
    @Test
    void controlPlaneCallsToAnUnansweringEndpointTimeOut() throws Exception {
        int[] port = new int[1];
        try (AutoCloseable endpoint = silentEndpoint(port)) {
            ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
            cfg.setBucket("b");
            cfg.setEndpoint("http://127.0.0.1:" + port[0]);
            cfg.setCallAttemptTimeoutSeconds(1);
            cfg.setCallTimeoutSeconds(2);
            try (S3ObjectStore s3 = new S3ObjectStore(cfg,
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))) {
                byte[] body = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                List<org.junit.jupiter.api.function.Executable> calls = List.of(
                        () -> s3.put("hb/key", body),
                        () -> s3.putIfMatch("lease/key", body, "\"e\""),
                        () -> s3.putIfAbsent("lease/key", body),
                        () -> s3.get("catalog/key"),
                        () -> s3.stat("hb/key"),
                        () -> s3.list("hb/"),
                        () -> s3.delete("hb/key"));
                for (var call : calls) {
                    long start = System.nanoTime();
                    assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                            assertThatThrownBy(call::execute).isInstanceOf(IOException.class));
                    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                            .as("bounded by callTimeoutSeconds (2 s)").isLessThan(6_000L);
                }
            }
        }
    }

    /** An upload's bounds grow with its size at {@code uploadMinBytesPerSecond}. */
    @Test
    void uploadBoundsScaleWithTheBodySize() {
        ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
        assertThat(S3ObjectStore.uploadAttemptTimeout(cfg, 100)).isEqualTo(Duration.ofSeconds(21));
        assertThat(S3ObjectStore.uploadTotalTimeout(cfg, 100)).isEqualTo(Duration.ofSeconds(63));
        long fiveGib = 5L << 30; // Lucene's largest merged segment, one PutObject
        assertThat(S3ObjectStore.uploadAttemptTimeout(cfg, fiveGib)).isEqualTo(Duration.ofSeconds(20 + 5120));
        assertThat(S3ObjectStore.uploadTotalTimeout(cfg, fiveGib)).isEqualTo(Duration.ofSeconds(60 + 3 * 5120));
    }

    /**
     * A whole-object download whose body takes longer than {@code callTimeoutSeconds} still
     * succeeds: the call bounds end with the response headers, the body is bounded by the client's
     * idle-read timeout only (generation files can be gigabytes).
     */
    @Test
    void slowDownloadBodyIsNotCutByTheCallTimeout(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp)
            throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            int chunks = 8;
            Thread responder = new Thread(() -> {
                try {
                    while (true) {
                        Socket s = server.accept();
                        Thread.ofVirtual().start(() -> {
                            try (s) {
                                var in = new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream()));
                                String line;
                                while ((line = in.readLine()) != null && !line.isEmpty()) { /* headers */ }
                                var out = s.getOutputStream();
                                out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + chunks
                                        + "\r\nETag: \"e1\"\r\nConnection: close\r\n\r\n")
                                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                                out.flush();
                                for (int i = 0; i < chunks; i++) { // 4 s of body, twice callTimeoutSeconds
                                    Thread.sleep(500);
                                    out.write('x');
                                    out.flush();
                                }
                            } catch (Exception ignored) {
                                // the test is over
                            }
                        });
                    }
                } catch (IOException closed) {
                    // the test is over
                }
            }, "slow-body-endpoint");
            responder.setDaemon(true);
            responder.start();

            ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
            cfg.setBucket("b");
            cfg.setEndpoint("http://127.0.0.1:" + server.getLocalPort());
            cfg.setCallAttemptTimeoutSeconds(1);
            cfg.setCallTimeoutSeconds(2);
            try (S3ObjectStore s3 = new S3ObjectStore(cfg,
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))) {
                java.nio.file.Path target = tmp.resolve("f");
                assertTimeoutPreemptively(Duration.ofSeconds(20), () -> s3.download("some/key", target));
                assertThat(java.nio.file.Files.size(target)).isEqualTo(chunks);
            }
        }
    }
}
