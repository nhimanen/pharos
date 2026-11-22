package com.pharos.operator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Every Kubernetes object name the operator derives (ruling OP-R4). A cluster's objects are
 * {@code <name>-<suffix>}; {@code SpecValidator} caps {@code metadata.name} at {@link #MAX_CLUSTER_NAME}
 * so every derived name, including the per-project Job names, stays within {@link #MAX_NAME}.
 * A project name is valid for pharos but not necessarily for Kubernetes, so it only ever appears
 * through {@link #k8sName(String, int)}.
 */
public final class Names {

    /** Kubernetes' limit for a label value, a Service and most other object names (DNS-1035 label). */
    public static final int MAX_NAME = 63;
    /** The longest {@code PharosCluster} name: {@code <name>-register-<k8sName>} must still fit. */
    public static final int MAX_CLUSTER_NAME = 40;
    /** Hex characters of the sha1 of the raw project name that {@link #k8sName} appends. */
    static final int HASH_LENGTH = 6;

    private Names() {}

    public static String token(String cluster) { return cluster + "-token"; }

    /** The ConfigMap of one role's rendered config: {@code <name>-config-writer|reader|router}. */
    public static String config(String cluster, String role) { return cluster + "-config-" + role; }

    public static String writers(String cluster) { return cluster + "-writers"; }

    public static String readers(String cluster) { return cluster + "-readers"; }

    public static String router(String cluster) { return cluster + "-router"; }

    /** The writers' or readers' headless Service ({@code role}: {@code writer} | {@code reader}). */
    public static String headless(String cluster, String role) { return cluster + "-" + role + "s-hl"; }

    /** The MinIO StatefulSet, Service, PVC and credentials Secret (one name, four kinds). */
    public static String minio(String cluster) { return cluster + "-minio"; }

    public static String minioBucketJob(String cluster) { return cluster + "-minio-bucket"; }

    /** Hex characters of the spec hash in a registration Job's name. */
    public static final int SPEC_HASH_LENGTH = 6;

    /**
     * A project's registration Job for one source: {@code <name>-register-<k8sName>-<specHash>}, so a
     * changed source gets a new Job (Jobs are create-only). {@code specHash}: {@value #SPEC_HASH_LENGTH}
     * hex characters.
     */
    public static String registerJob(String cluster, String project, String specHash) {
        return withProject(cluster + "-register-", project, "-" + specHash);
    }

    /** A project's removal Job: {@code <name>-remove-<k8sName>}. */
    public static String removeJob(String cluster, String project) {
        return withProject(cluster + "-remove-", project, "");
    }

    /**
     * The pre-flight Job of an upgrade to {@code version}: {@code <name>-preflight-<version>}. A tag that
     * is not a valid name part as written (upper case, {@code _}) or too long becomes {@link #k8sName}
     * text plus a hash, so every version still gets its own Job.
     */
    public static String preflightJob(String cluster, String version) {
        String prefix = cluster + "-preflight-";
        int room = MAX_NAME - prefix.length();
        boolean plain = version.matches("[a-z0-9]([a-z0-9.-]*[a-z0-9])?") && version.length() <= room;
        return prefix + (plain ? version : k8sName(version, room));
    }

    /**
     * The post-check Job of a downgrade to {@code version}, run once the writers run it and before the
     * readers move: {@code <name>-preflight-<version>-post}, fitted like {@link #preflightJob}.
     */
    public static String preflightPostJob(String cluster, String version) {
        String prefix = cluster + "-preflight-";
        String suffix = "-post";
        int room = MAX_NAME - prefix.length() - suffix.length();
        boolean plain = version.matches("[a-z0-9]([a-z0-9.-]*[a-z0-9])?") && version.length() <= room;
        return prefix + (plain ? version : k8sName(version, room)) + suffix;
    }

    /** A project's git credentials volume on the writers: {@code git-<k8sName>}. */
    public static String gitCredentialsVolume(String project) {
        return "git-" + k8sName(project, MAX_NAME - 4);
    }

    /** The NetworkPolicy fencing the writers' and readers' port. */
    public static String networkPolicy(String cluster) { return cluster + "-internal"; }

    /** The in-cluster DNS name of a Service in {@code namespace}. */
    public static String serviceHost(String service, String namespace) { return service + "." + namespace + ".svc"; }

    /** {@link #k8sName(String, int)} with room for the whole name. */
    public static String k8sName(String project) {
        return k8sName(project, MAX_NAME);
    }

    /**
     * A project name as a DNS-1123 label of at most {@code maxLength} characters: lower-cased, every
     * character outside {@code [a-z0-9-]} mapped to {@code -}, leading and trailing {@code -} trimmed,
     * truncated, then {@code -} plus 6 hex characters of the sha1 of the raw name, so two names that map
     * to the same text still differ. With no room for any of the text, only the hash is left.
     */
    public static String k8sName(String project, int maxLength) {
        if (maxLength < HASH_LENGTH) {
            throw new IllegalArgumentException("no room for a project name: " + maxLength + " characters");
        }
        String hash = sha1Hex(project).substring(0, HASH_LENGTH);
        String text = trimDashes(project.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9-]", "-"));
        int room = maxLength - HASH_LENGTH - 1;
        if (text.length() > room) text = trimDashes(text.substring(0, Math.max(0, room)));
        return text.isEmpty() ? hash : text + "-" + hash;
    }

    /**
     * {@code <prefix><k8sName(project)><suffix>}, fitted into {@link #MAX_NAME} by truncating only the
     * project's text (e.g. {@code withProject("demo-register-", "lucene", "-1a2b3c")}).
     */
    public static String withProject(String prefix, String project, String suffix) {
        String s = suffix == null ? "" : suffix;
        return prefix + k8sName(project, MAX_NAME - prefix.length() - s.length()) + s;
    }

    private static String trimDashes(String s) {
        int a = 0;
        int b = s.length();
        while (a < b && s.charAt(a) == '-') a++;
        while (b > a && s.charAt(b - 1) == '-') b--;
        return s.substring(a, b);
    }

    static String sha1Hex(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is not available", e);
        }
    }
}
