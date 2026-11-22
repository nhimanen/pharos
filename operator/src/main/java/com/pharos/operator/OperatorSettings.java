package com.pharos.operator;

import java.util.Map;

/**
 * The images the operator deploys, from its own environment (deploy/operator.yaml):
 * <ul>
 *   <li>{@code PHAROS_IMAGE}: the pharos image repository; the tag is {@code spec.version};</li>
 *   <li>{@code MINIO_IMAGE} and {@code MC_IMAGE}: the MinIO server and client images of minio mode,
 *       pinned to a release tag by default.</li>
 * </ul>
 */
public record OperatorSettings(String pharosImage, String minioImage, String mcImage) {

    public static final String DEFAULT_PHAROS_IMAGE = "pharos";
    public static final String DEFAULT_MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z";
    public static final String DEFAULT_MC_IMAGE = "quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z";

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
