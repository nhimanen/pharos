package com.pharos.cluster.objectstore;

import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the {@link ObjectStoreContract} against a real S3 API.
 * <p>
 * Two modes:
 * <ul>
 *   <li>External endpoint: set env vars {@code PHAROS_TEST_S3_ENDPOINT}, {@code PHAROS_TEST_S3_ACCESS_KEY},
 *       {@code PHAROS_TEST_S3_SECRET_KEY} (e.g. a local MinIO binary); a unique bucket is created per run.</li>
 *   <li>Testcontainers MinIO: needs a Docker daemon; run {@code mvn test -Dtest=S3ObjectStoreMinioTest}.</li>
 * </ul>
 * With neither available the test is SKIPPED. (Docker availability is checked manually rather than via
 * {@code @Testcontainers(disabledWithoutDocker = true)} so that external mode works without Docker.)
 */
class S3ObjectStoreMinioTest extends ObjectStoreContract {

    private static MinIOContainer container;
    private static S3ObjectStore store;
    private static S3Client admin;
    private static String bucket;

    @BeforeAll
    static void setUp() {
        String endpoint = System.getenv("PHAROS_TEST_S3_ENDPOINT");
        String ak = System.getenv("PHAROS_TEST_S3_ACCESS_KEY");
        String sk = System.getenv("PHAROS_TEST_S3_SECRET_KEY");
        if (endpoint == null || ak == null || sk == null) {
            assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                    "no external S3 endpoint configured and Docker is unavailable");
            container = new MinIOContainer("minio/minio:latest");
            container.start();
            endpoint = container.getS3URL();
            ak = container.getUserName();
            sk = container.getPassword();
        }
        StaticCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk));
        bucket = "pharos-test-" + UUID.randomUUID().toString().substring(0, 8);
        admin = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(creds)
                .build();
        admin.createBucket(b -> b.bucket(bucket));

        ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
        cfg.setEndpoint(endpoint);
        cfg.setRegion("us-east-1");
        cfg.setBucket(bucket);
        cfg.setPathStyle(true);
        store = new S3ObjectStore(cfg, creds);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (store != null) store.close();
        if (admin != null) admin.close();
        if (container != null) container.stop();
    }

    @Override
    ObjectStore store() { return store; }
}
