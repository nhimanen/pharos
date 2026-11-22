package com.pharos.operator.render;

import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.PodFailurePolicyBuilder;

import java.util.List;
import java.util.Map;

/**
 * The pre-flight Job of an upgrade, {@code <name>-preflight-<version>}: the <b>new</b> image runs
 * {@code pharos upgrade-indexes --check} against the bucket with the writer config, token and
 * object-store keys, on the same writer-shaped pod as the project Jobs ({@link ProjectJobRenderer}).
 * It reads every catalog shard's {@code LATEST} manifest and changes nothing.
 *
 * <p>Exit code {@value #EXIT_REFUSED} means a shard's index format would be refused by the new
 * version (older than N-1, or newer); a pod failure policy fails the Job on it at once (reason
 * {@value #REASON_REFUSED}), since a retry would read the same bucket. Any other failure (the bucket
 * unreachable, an image without {@code --check}) is retried up to the back-off limit.
 */
public final class PreflightJobRenderer {

    /** {@code pharos upgrade-indexes --check}: a shard would be refused. */
    public static final int EXIT_REFUSED = 3;
    /** The Job's {@code Failed} reason when the pod failure policy matched {@link #EXIT_REFUSED}. */
    public static final String REASON_REFUSED = "PodFailurePolicy";

    private PreflightJobRenderer() {}

    /** The pre-flight Job of an upgrade of {@code cluster} to {@code version}. */
    public static Job render(PharosCluster cluster, OperatorSettings settings, String version) {
        return render(cluster, settings, version, Names.preflightJob(cluster.getMetadata().getName(), version));
    }

    /**
     * The post-check Job of a downgrade to {@code version} (ruling OP-R6): the same check, run once the
     * writers run {@code version} and before the readers move, so a format published by the writers of
     * the version being left is caught.
     */
    public static Job renderPostCheck(PharosCluster cluster, OperatorSettings settings, String version) {
        return render(cluster, settings, version, Names.preflightPostJob(cluster.getMetadata().getName(), version));
    }

    private static Job render(PharosCluster cluster, OperatorSettings settings, String version, String name) {
        Job job = ProjectJobRenderer.job(cluster, settings, version, name, Labels.PREFLIGHT,
                Map.of(Labels.VERSION_ANNOTATION, version), List.of("upgrade-indexes", "--check"));
        job.getSpec().setPodFailurePolicy(new PodFailurePolicyBuilder()
                .addNewRule().withAction("FailJob")
                .withNewOnExitCodes().withContainerName("pharos").withOperator("In").withValues(EXIT_REFUSED)
                .endOnExitCodes()
                .endRule()
                .build());
        return job;
    }
}
