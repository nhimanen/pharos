package com.pharos.operator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.generator.annotation.Required;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.List;
import java.util.Map;

/**
 * The desired cluster. Field names are the CRD's (see the spec's {@code PharosCluster} example).
 * The CRD only checks types; {@link com.pharos.operator.validate.SpecValidator} checks everything
 * that spans fields (exactly one storage kind, min/max, trigger consistency, schedule, cron).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PharosClusterSpec(
        @Required @JsonPropertyDescription("pharos image tag, e.g. 1.6.0") String version,
        @Required @JsonPropertyDescription("exactly one of minio (operator-managed) or s3") Storage storage,
        Embeddings embeddings,
        List<Project> projects,
        @Required RoleSpec writers,
        @Required RoleSpec readers,
        RouterSpec router,
        Autoscaling autoscaling,
        @JsonPropertyDescription("extra keys merged into the rendered cluster.* config (validated by ClusterConfig); "
                + "never put the token here")
        Map<String, JsonNode> pharosConfig) {

    /** Exactly one of {@code minio} or {@code s3}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Storage(Minio minio, S3 s3) {}

    /** Operator-managed MinIO; {@code storage} is its PVC size (e.g. {@code 20Gi}). */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Minio(String storage) {}

    /**
     * An existing S3-compatible bucket. {@code credentialsSecret} names a Secret in the cluster's
     * namespace with the keys {@code accessKey} and {@code secretKey}; the keys reach pods only as env.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record S3(String endpoint, String bucket, String region, Boolean pathStyle, String credentialsSecret) {}

    /** {@code model}: an embedding model name, or {@code none}; same choices as {@code pharos embeddings enable}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Embeddings(String model) {}

    /** A project indexed from git; {@code embed: false} indexes it without embeddings. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Project(@Required String name, @Required Git git, Boolean embed) {}

    /** {@code credentialsSecret}: optional Secret with the git credentials, mounted on every writer. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Git(@Required String url, String ref, String credentialsSecret) {}

    /**
     * Writers or readers: the replica range and pod shape. {@code workVolume} applies to writers;
     * {@code mode} ({@code lazy} | {@code full}) and {@code cacheVolume} to readers.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RoleSpec(Integer min, Integer max, ResourceRequirements resources,
                           String workVolume, String cacheVolume, String mode) {}

    /** The router Deployment and its Service. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RouterSpec(Integer replicas, RouterService service) {}

    /** {@code type}: {@code ClusterIP} | {@code NodePort} | {@code LoadBalancer}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RouterService(String type) {}

    /**
     * Defaults ({@link com.pharos.operator.Defaults}): {@code cooldownSeconds} 300, {@code maxStepUp} 2,
     * {@code maxStepDown} 1.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Autoscaling(Boolean enabled, Integer cooldownSeconds, Integer maxStepUp, Integer maxStepDown,
                              Triggers triggers, List<ScheduleEntry> schedule) {}

    /** Rendered into pharos's {@code cluster.status.*} config; pharos raises the proposals. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Triggers(ReaderTriggers readers, WriterTriggers writers) {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record ReaderTriggers(ReaderScaleUp scaleUp, ReaderScaleDown scaleDown) {}

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record ReaderScaleUp(Integer warmP95Millis, Integer qpsPerReader, Integer forSeconds) {}

        /** {@code loadBelowPercent}: QPS per reader as a percentage of {@code scaleUp.qpsPerReader}. */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record ReaderScaleDown(Integer loadBelowPercent, Integer forSeconds) {}

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record WriterTriggers(WriterScaleUp scaleUp, WriterScaleDown scaleDown) {}

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record WriterScaleUp(Integer backlogShards, Integer oldestPendingSeconds, Integer forSeconds) {}

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public record WriterScaleDown(Integer idleForSeconds) {}
    }

    /**
     * Moves the {@code min} floor of readers and/or writers from {@code cron} on (5 fields, evaluated
     * in {@code timezone}, default UTC) until the next entry fires. Never above the role's {@code max}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ScheduleEntry(@Required String cron, String timezone, Floor readers, Floor writers) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Floor(@Required Integer min) {}
}
