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
}
