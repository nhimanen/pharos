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
}
