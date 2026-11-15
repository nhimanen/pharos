package com.pharos.cluster.objectstore;

import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Runs the {@link ObjectStoreContract} against a real S3 API (MinIO).
 * Needs a local Docker daemon: with Docker running, execute
 * {@code mvn test -Dtest=S3ObjectStoreMinioTest}. Without Docker the test is SKIPPED.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3ObjectStoreMinioTest extends ObjectStoreContract {

    private static final String BUCKET = "pharos-test";

    @Container
    static final MinIOContainer MINIO = new MinIOContainer("minio/minio:latest");

    private static S3ObjectStore store;

    @BeforeAll
    static void setUp() {
        try (S3Client admin = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .endpointOverride(URI.create(MINIO.getS3URL()))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
                .build()) {
            admin.createBucket(b -> b.bucket(BUCKET));
        }
        ClusterConfig.ObjectStoreConfig cfg = new ClusterConfig.ObjectStoreConfig();
        cfg.setEndpoint(MINIO.getS3URL());
        cfg.setRegion("us-east-1");
        cfg.setBucket(BUCKET);
        cfg.setPathStyle(true);
        store = new S3ObjectStore(cfg, StaticCredentialsProvider.create(
                AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (store != null) store.close();
    }

    @Override
    ObjectStore store() { return store; }
}
