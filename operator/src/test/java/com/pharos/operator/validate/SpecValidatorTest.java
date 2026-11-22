package com.pharos.operator.validate;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.Git;
import com.pharos.operator.api.PharosClusterSpec.Minio;
import com.pharos.operator.api.PharosClusterSpec.Project;
import com.pharos.operator.api.PharosClusterSpec.RoleSpec;
import com.pharos.operator.api.PharosClusterSpec.S3;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;
import com.pharos.operator.api.PharosClusterSpec.Storage;
import com.pharos.operator.api.PharosClusterSpec.Triggers;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SpecValidatorTest {

    private static final RoleSpec WRITERS = new RoleSpec(1, 4, null, "50Gi", null, null);
    private static final RoleSpec READERS = new RoleSpec(2, 8, null, null, "20Gi", "lazy");
    private static final Storage MINIO = new Storage(new Minio("20Gi"), null);
    private static final S3 S3_BUCKET = new S3("https://s3.eu-north-1.amazonaws.com", "my-pharos", "eu-north-1",
            false, "pharos-s3");

    private static PharosClusterSpec spec(Storage storage, List<Project> projects, RoleSpec writers, RoleSpec readers,
                                          Autoscaling autoscaling) {
        return new PharosClusterSpec("1.6.0", storage, null, projects, writers, readers, null, autoscaling, null);
    }

    private static PharosClusterSpec valid() {
        return spec(MINIO, List.of(project("lucene")), WRITERS, READERS, null);
    }

    private static Project project(String name) {
        return new Project(name, new Git("https://github.com/apache/lucene.git", "main", null), false);
    }

    private static Autoscaling withTriggers(Triggers triggers) {
        return new Autoscaling(true, 300, 2, 1, triggers, null);
    }

    private static Autoscaling withSchedule(ScheduleEntry... entries) {
        return new Autoscaling(true, 300, 2, 1, null, List.of(entries));
    }

    private static Triggers readerTriggers(Integer qpsPerReader, Integer loadBelowPercent) {
        return new Triggers(new Triggers.ReaderTriggers(
                new Triggers.ReaderScaleUp(500, qpsPerReader, 300),
                new Triggers.ReaderScaleDown(loadBelowPercent, 1800)), null);
    }

    @Test
    void validSpecHasNoErrors() {
        assertThat(SpecValidator.validate(valid())).isEmpty();
        assertThat(SpecValidator.validate(spec(new Storage(null, S3_BUCKET), List.of(), WRITERS, READERS,
                withTriggers(readerTriggers(50, 20))))).isEmpty();
    }

    @Test
    void minAboveMaxRejected() {
        List<String> errors = SpecValidator.validate(spec(MINIO, List.of(),
                new RoleSpec(5, 2, null, null, null, null), new RoleSpec(9, 8, null, null, null, null), null));
        assertThat(errors).containsExactlyInAnyOrder(
                "spec.writers: min (5) is above max (2)",
                "spec.readers: min (9) is above max (8)");
    }

    @Test
    void scaleDownLoadAtOrAboveScaleUpRejected() {
        // At the scale-up load (100% of qpsPerReader) and above: rejected, whatever the target.
        for (int pct : new int[] {100, 150}) {
            for (Integer qps : new Integer[] {50, 10, null}) {
                assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                        withTriggers(readerTriggers(qps, pct)))))
                        .as("loadBelowPercent %d, qpsPerReader %s", pct, qps)
                        .singleElement().asString()
                        .startsWith("spec.autoscaling.triggers.readers.scaleDown.loadBelowPercent (" + pct + "%)")
                        .contains("must be below the scale-up load");
            }
        }
        // Just below it, and the defaults' 20% of 50 qps: fine.
        assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withTriggers(readerTriggers(50, 99))))).isEmpty();
        assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withTriggers(readerTriggers(null, 20))))).isEmpty();
        // Not a load at all.
        assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withTriggers(readerTriggers(50, 0)))))
                .containsExactly("spec.autoscaling.triggers.readers.scaleDown.loadBelowPercent: must be at least 1, got 0");
    }

    @Test
    void exactlyOneStorageKind() {
        String msg = "spec.storage: set exactly one of minio or s3";
        assertThat(SpecValidator.validate(spec(new Storage(null, null), List.of(), WRITERS, READERS, null)))
                .containsExactly(msg);
        assertThat(SpecValidator.validate(spec(null, List.of(), WRITERS, READERS, null))).containsExactly(msg);
        assertThat(SpecValidator.validate(spec(new Storage(new Minio("20Gi"), S3_BUCKET), List.of(), WRITERS,
                READERS, null))).containsExactly(msg + ", not both");
        assertThat(SpecValidator.validate(spec(new Storage(null, new S3("s3.example", null, null, null, null)),
                List.of(), WRITERS, READERS, null))).containsExactlyInAnyOrder(
                "spec.storage.s3.endpoint: must be an http:// or https:// URL",
                "spec.storage.s3.bucket: is required",
                "spec.storage.s3.credentialsSecret: is required (a Secret with the keys accessKey and secretKey)");
    }

    @Test
    void scheduleMinAboveMaxRejected() {
        List<String> errors = SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS, withSchedule(
                new ScheduleEntry("0 8 * * 1-5", "Europe/Helsinki", new Floor(9), null),
                new ScheduleEntry("0 18 * * 1-5", "Europe/Helsinki", new Floor(8), new Floor(5)))));
        assertThat(errors).containsExactlyInAnyOrder(
                "spec.autoscaling.schedule[0].readers.min (9) is above spec.readers.max (8)",
                "spec.autoscaling.schedule[1].writers.min (5) is above spec.writers.max (4)");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0 8 * *", "0 8 * * 1-5 *", "60 8 * * *", "0 24 * * *", "0 8 0 * *",
            "0 8 * 13 *", "0 8 * * 8", "0 8 * * MON-", "*/0 * * * *", "0 18-8 * * *", "0 8,,9 * * *",
            "@daily", "0 8 * * FOO", "a b c d e"})
    void badCronRejected(String cron) {
        List<String> errors = SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withSchedule(new ScheduleEntry(cron, null, new Floor(2), null))));
        assertThat(errors).singleElement().asString().startsWith("spec.autoscaling.schedule[0].cron: '" + cron + "'");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0 8 * * 1-5", "*/15 * * * *", "0 0 1 JAN *", "30 6 * * MON-FRI", "0 8-18/2 * * 0,7",
            "5 4 * * sun"})
    void goodCronAccepted(String cron) {
        assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withSchedule(new ScheduleEntry(cron, "UTC", new Floor(2), null))))).isEmpty();
    }

    @Test
    void unknownTimezoneRejected() {
        assertThat(SpecValidator.validate(spec(MINIO, List.of(), WRITERS, READERS,
                withSchedule(new ScheduleEntry("0 8 * * *", "Mars/Olympus", new Floor(2), null)))))
                .containsExactly("spec.autoscaling.schedule[0].timezone: unknown time zone 'Mars/Olympus'");
    }

    @Test
    void duplicateProjectNamesRejected() {
        List<String> errors = SpecValidator.validate(spec(MINIO,
                List.of(project("lucene"), project("mypal"), project("lucene")), WRITERS, READERS, null));
        assertThat(errors).containsExactly("spec.projects[2].name: duplicate project name 'lucene'");
    }

    @Test
    void projectNamesMustBeShardIds() {
        assertThat(SpecValidator.validate(spec(MINIO, List.of(project("../etc")), WRITERS, READERS, null)))
                .singleElement().asString().startsWith("spec.projects[0].name: '../etc' is not a valid");
    }

    @Test
    void roleSpecificFieldsAndModeChecked() {
        List<String> errors = SpecValidator.validate(spec(MINIO, List.of(),
                new RoleSpec(1, 2, null, "50Gi", "20Gi", "lazy"), new RoleSpec(0, 2, null, "5Gi", "lots", "eager"),
                null));
        assertThat(errors).containsExactlyInAnyOrder(
                "spec.writers.mode: applies to readers only",
                "spec.writers.cacheVolume: applies to readers only",
                "spec.readers.min: must be at least 1, got 0",
                "spec.readers.workVolume: applies to writers only",
                "spec.readers.mode: must be lazy or full, got 'eager'",
                "spec.readers.cacheVolume: must be a size such as 20Gi, got 'lots'");
    }

    @Test
    void tokenInPharosConfigRejected() {
        PharosClusterSpec s = new PharosClusterSpec("1.6.0", MINIO, null, List.of(), WRITERS, READERS, null, null,
                Map.of("token", TextNode.valueOf("x")));
        assertThat(SpecValidator.validate(s)).singleElement().asString().startsWith("spec.pharosConfig.token:");
    }

    @Test
    void allErrorsReportedAtOnce() {
        PharosClusterSpec s = new PharosClusterSpec("not a tag", null, null, List.of(project("a"), project("a")),
                new RoleSpec(3, 1, null, null, null, null), READERS, null, null, null);
        assertThat(SpecValidator.validate(s)).hasSize(4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"minio.yaml", "s3.yaml"})
    void deployExamplesAreValid(String file) throws Exception {
        Path path = Path.of(System.getProperty("pharos.crdCommitted", "../deploy/crd"), "..", "examples", file);
        PharosCluster cluster;
        try (InputStream in = Files.newInputStream(path)) {
            // Strict: a misspelt field in an example must fail here, not be silently dropped.
            cluster = new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(in, PharosCluster.class);
        }
        assertThat(cluster.getMetadata().getName()).isNotBlank();
        assertThat(SpecValidator.validate(cluster.getSpec())).isEmpty();
    }
}
