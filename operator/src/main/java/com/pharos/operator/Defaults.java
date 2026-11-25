package com.pharos.operator;

import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.RoleSpec;

/**
 * The defaults of every optional {@code PharosCluster} field, in one place: {@code SpecValidator}
 * checks against them and the renderers and the autoscaler apply them. The CRD carries no defaults,
 * so a field left out always means the value here.
 */
public final class Defaults {

    /** {@code autoscaling.cooldownSeconds}. */
    public static final int COOLDOWN_SECONDS = 300;
    /** {@code autoscaling.maxStepUp}. */
    public static final int MAX_STEP_UP = 2;
    /** {@code autoscaling.maxStepDown}. */
    public static final int MAX_STEP_DOWN = 1;
    /** {@code writers.min} / {@code readers.min}; {@code max} defaults to the role's {@code min}. */
    public static final int ROLE_MIN = 1;
    /** {@code autoscaling.triggers.readers.scaleUp.qpsPerReader}; also pharos's own default. */
    public static final int QPS_PER_READER = 50;

    /** {@code router.replicas}. */
    public static final int ROUTER_REPLICAS = 2;
    /** {@code router.service.type}. */
    public static final String ROUTER_SERVICE_TYPE = "ClusterIP";
    /** {@code readers.mode}. */
    public static final String READER_MODE = "lazy";
    /** {@code writers.workVolume}: the per-writer PVC (work dir and index). */
    public static final String WORK_VOLUME = "20Gi";
    /** {@code readers.cacheVolume}: the per-reader PVC (block cache). */
    public static final String CACHE_VOLUME = "10Gi";
    /** {@code storage.minio.storage}: the MinIO PVC. */
    public static final String MINIO_STORAGE = "20Gi";

    private Defaults() {}

    /** The role's floor: {@code min}, default {@value #ROLE_MIN}. */
    public static int min(RoleSpec role) {
        return role == null || role.min() == null ? ROLE_MIN : role.min();
    }

    /** The role's ceiling: {@code max}, default its {@code min}. */
    public static int max(RoleSpec role) {
        return role == null || role.max() == null ? min(role) : role.max();
    }

    /**
     * {@code autoscaling.enabled}: on unless set to {@code false}. Off, the operator applies no
     * proposal and no schedule floor; replicas stay within {@code [min, max]}.
     */
    public static boolean autoscalingEnabled(Autoscaling a) {
        return a == null || a.enabled() == null || a.enabled();
    }

    public static int cooldownSeconds(Autoscaling a) {
        return a == null || a.cooldownSeconds() == null ? COOLDOWN_SECONDS : a.cooldownSeconds();
    }

    public static int maxStepUp(Autoscaling a) {
        return a == null || a.maxStepUp() == null ? MAX_STEP_UP : a.maxStepUp();
    }

    public static int maxStepDown(Autoscaling a) {
        return a == null || a.maxStepDown() == null ? MAX_STEP_DOWN : a.maxStepDown();
    }

    /** The reader scale-up target: {@code triggers.readers.scaleUp.qpsPerReader}, default {@value #QPS_PER_READER}. */
    public static int qpsPerReader(PharosClusterSpec spec) {
        var a = spec == null ? null : spec.autoscaling();
        var t = a == null ? null : a.triggers();
        var r = t == null ? null : t.readers();
        var up = r == null ? null : r.scaleUp();
        return up == null || up.qpsPerReader() == null ? QPS_PER_READER : up.qpsPerReader();
    }

    public static int routerReplicas(PharosClusterSpec spec) {
        return spec.router() == null || spec.router().replicas() == null ? ROUTER_REPLICAS : spec.router().replicas();
    }

    public static String routerServiceType(PharosClusterSpec spec) {
        return spec.router() == null || spec.router().service() == null || spec.router().service().type() == null
                ? ROUTER_SERVICE_TYPE : spec.router().service().type();
    }

    public static String readerMode(PharosClusterSpec spec) {
        return spec.readers() == null || spec.readers().mode() == null ? READER_MODE : spec.readers().mode();
    }

    public static String workVolume(PharosClusterSpec spec) {
        return spec.writers() == null || spec.writers().workVolume() == null ? WORK_VOLUME : spec.writers().workVolume();
    }

    public static String cacheVolume(PharosClusterSpec spec) {
        return spec.readers() == null || spec.readers().cacheVolume() == null ? CACHE_VOLUME : spec.readers().cacheVolume();
    }

    public static String minioStorage(PharosClusterSpec spec) {
        var m = spec.storage() == null ? null : spec.storage().minio();
        return m == null || m.storage() == null ? MINIO_STORAGE : m.storage();
    }
}
