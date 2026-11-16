package com.pharos.cluster;

import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.ClusterConfig;
import com.pharos.search.SourceReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link ClusterRoundTripTest#readerServesSameResultsAsWriter} against a real S3 API.
 * <p>
 * Same two modes as {@code S3ObjectStoreMinioTest}: an external endpoint from
 * {@code PHAROS_TEST_S3_ENDPOINT} / {@code PHAROS_TEST_S3_ACCESS_KEY} / {@code PHAROS_TEST_S3_SECRET_KEY}
 * (a unique bucket per run), or a Testcontainers MinIO when Docker is available. With neither,
 * the test is SKIPPED. Docker is checked by hand instead of
 * {@code @Testcontainers(disabledWithoutDocker = true)} so that the external mode works without Docker.
 */
class ClusterRoundTripMinioTest {

    private static MinIOContainer container;
    private static S3Client admin;
    private static String bucket;
    private static String endpoint;
    private static StaticCredentialsProvider creds;

    @TempDir Path tmp;

    private ClusterFixture cluster;

    @BeforeAll
    static void setUp() {
        endpoint = System.getenv("PHAROS_TEST_S3_ENDPOINT");
        String ak = System.getenv("PHAROS_TEST_S3_ACCESS_KEY");
        String sk = System.getenv("PHAROS_TEST_S3_SECRET_KEY");
        int set = (endpoint != null ? 1 : 0) + (ak != null ? 1 : 0) + (sk != null ? 1 : 0);
        if (set > 0 && set < 3) {
            throw new IllegalStateException("Set all of PHAROS_TEST_S3_ENDPOINT, PHAROS_TEST_S3_ACCESS_KEY, "
                    + "PHAROS_TEST_S3_SECRET_KEY or none of them");
        }
        if (set == 0) {
            assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                    "no external S3 endpoint configured and Docker is unavailable");
            container = new MinIOContainer("minio/minio:latest");
            container.start();
            endpoint = container.getS3URL();
            ak = container.getUserName();
            sk = container.getPassword();
        }
        creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk));
        bucket = "pharos-rt-" + UUID.randomUUID().toString().substring(0, 8);
        admin = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(creds)
                .build();
        admin.createBucket(b -> b.bucket(bucket));
    }

    @AfterAll
    static void tearDown() {
        if (admin != null && bucket != null) {
            try (ObjectStore store = s3()) {
                for (ObjectInfo o : store.list("")) admin.deleteObject(b -> b.bucket(bucket).key(o.key()));
                admin.deleteBucket(b -> b.bucket(bucket));
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        if (admin != null) admin.close();
        if (container != null) container.stop();
    }

    @AfterEach
    void closeCluster() throws Exception {
        SourceReader.setEnabled(true);
        if (cluster != null) cluster.close();
    }

    private static ObjectStore s3() {
        ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
        cfg.setEndpoint(endpoint);
        cfg.setRegion("us-east-1");
        cfg.setBucket(bucket);
        cfg.setPathStyle(true);
        return TestBackends.s3(cfg, creds);
    }

    @Test
    void readerServesSameResultsAsWriter() throws Exception {
        // One S3 client per node, as on two machines; the backends close them.
        cluster = new ClusterFixture(tmp, s3(), s3(), bucket);
        cluster.indexBoth();
        cluster.readerRuntime.startServing();

        SourceReader.setEnabled(false);
        String writer = SingleNodeGoldenTest.snapshot(cluster.writerEngine, cluster.writerRegistry);
        String reader = SingleNodeGoldenTest.snapshot(cluster.readerEngine, cluster.readerRegistry);
        assertThat(reader).isEqualTo(writer);
    }
}
