package com.pharos.operator;

import java.util.Map;

/**
 * The images the operator deploys, from its own environment (deploy/operator.yaml):
 * <ul>
 *   <li>{@code PHAROS_IMAGE}: the pharos image repository; the tag is {@code spec.version};</li>
 *   <li>{@code MINIO_IMAGE} and {@code MC_IMAGE}: the MinIO server and client images of minio mode,
 *       pinned to a release tag by default.</li>
 * </ul>
 * MinIO no longer publishes images: {@code quay.io/minio/*} and {@code docker.io/minio/*} refuse anonymous
 * pulls since late 2025. The defaults are the community builds of the same AGPL sources published as
 * {@code docker.io/pgsty/minio} and {@code docker.io/pgsty/mc}; set the env vars to use a mirror of your own.
 */
public record OperatorSettings(String pharosImage, String minioImage, String mcImage) {

    public static final String DEFAULT_PHAROS_IMAGE = "pharos";
    public static final String DEFAULT_MINIO_IMAGE = "docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z";
    public static final String DEFAULT_MC_IMAGE = "docker.io/pgsty/mc:RELEASE.2026-11-12T00-00-00Z";

    public static OperatorSettings defaults() {
        return new OperatorSettings(DEFAULT_PHAROS_IMAGE, DEFAULT_MINIO_IMAGE, DEFAULT_MC_IMAGE);
    }

    public static OperatorSettings fromEnv(Map<String, String> env) {
        return new OperatorSettings(
                or(env.get("PHAROS_IMAGE"), DEFAULT_PHAROS_IMAGE),
                or(env.get("MINIO_IMAGE"), DEFAULT_MINIO_IMAGE),
                or(env.get("MC_IMAGE"), DEFAULT_MC_IMAGE));
    }

    /** The pharos image of release {@code version}. */
    public String pharosImage(String version) {
        return pharosImage + ":" + version;
    }

    private static String or(String v, String fallback) {
        return v == null || v.isBlank() ? fallback : v.trim();
    }
}
