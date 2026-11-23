package com.pharos.operator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** The deploy/examples clusters as test fixtures (parsed strictly, as a user's manifest would be). */
public final class TestClusters {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private TestClusters() {}

    public static Path examples() {
        return Path.of(System.getProperty("pharos.crdCommitted", "../deploy/crd"), "..", "examples");
    }

    /** deploy/examples/minio.yaml: {@code demo} in namespace {@code pharos}, operator-managed MinIO. */
    public static PharosCluster minio() {
        return load("minio.yaml");
    }

    /** deploy/examples/s3.yaml: {@code prod} in namespace {@code pharos}, credentials Secret {@code pharos-s3}. */
    public static PharosCluster s3() {
        return load("s3.yaml");
    }

    public static PharosCluster load(String file) {
        try (InputStream in = Files.newInputStream(examples().resolve(file))) {
            return YAML.readValue(in, PharosCluster.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code cluster} with its spec replaced. */
    public static PharosCluster withSpec(PharosCluster cluster, PharosClusterSpec spec) {
        PharosCluster c = new PharosCluster();
        c.setMetadata(cluster.getMetadata());
        c.setSpec(spec);
        return c;
    }

    /** A copy of {@code spec} with another {@code pharosConfig}. */
    public static PharosClusterSpec withPharosConfig(PharosClusterSpec s,
                                                     java.util.Map<String, com.fasterxml.jackson.databind.JsonNode> config) {
        return new PharosClusterSpec(s.version(), s.storage(), s.embeddings(), s.projects(), s.writers(), s.readers(),
                s.router(), s.autoscaling(), config);
    }
}
