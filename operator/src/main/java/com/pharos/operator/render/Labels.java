package com.pharos.operator.render;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The labels on everything the operator creates. {@link #CLUSTER} names the {@code PharosCluster}
 * a pod belongs to (the NetworkPolicy admits by it); {@link #MANAGED_BY} is what the operator's
 * informers select on, so they never cache objects (Secrets above all) they do not own.
 */
public final class Labels {

    public static final String NAME = "app.kubernetes.io/name";
    public static final String INSTANCE = "app.kubernetes.io/instance";
    public static final String COMPONENT = "app.kubernetes.io/component";
    public static final String MANAGED_BY = "app.kubernetes.io/managed-by";
    public static final String CLUSTER = "pharos.dev/cluster";

    public static final String APP = "pharos";
    public static final String OPERATOR = "pharos-operator";
    /** The informers' label selector: only objects this operator manages. */
    public static final String MANAGED_SELECTOR = MANAGED_BY + "=" + OPERATOR;
    /** The operator's own pod label (deploy/operator.yaml); the NetworkPolicy admits it to read {@code /cluster/status}. */
    public static final String OPERATOR_POD_NAME = OPERATOR;

    /** Components. */
    public static final String WRITER = "writer";
    public static final String READER = "reader";
    public static final String ROUTER = "router";
    public static final String MINIO = "minio";
    public static final String MINIO_BUCKET = "minio-bucket";
    public static final String TOKEN = "token";
    public static final String CONFIG = "config";
    public static final String NETWORK = "network";
    /** A project's registration and removal Jobs. */
    public static final String REGISTER = "register";
    public static final String REMOVE = "remove";
    /** The pre-flight Job of an upgrade ({@code pharos upgrade-indexes --check} with the new image). */
    public static final String PREFLIGHT = "preflight";

    /**
     * Annotation on a project Job: the project's name as pharos knows it. A label could not hold every
     * valid project name; object names only carry {@code Names.k8sName}.
     */
    public static final String PROJECT_ANNOTATION = "pharos.dev/project";

    /** On the writer and reader StatefulSets: the autoscaler's last application ({@code scale.LastScale}). */
    public static final String LAST_SCALE = "pharos.dev/last-scale";

    /**
     * On the writer and reader StatefulSets and the router Deployment: the version their pods were last
     * moved to and when ({@code upgrade.VersionSince}), applied together with the new image so the
     * upgrade gate knows which heartbeats come from the new pods.
     */
    public static final String VERSION_SINCE = "pharos.dev/version-since";
    /** On a pre-flight Job: the version it checks. */
    public static final String VERSION_ANNOTATION = "pharos.dev/version";

    private Labels() {}

    /** Every object's labels: app, instance, component, managed-by and the cluster. */
    public static Map<String, String> of(String cluster, String component) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(NAME, APP);
        m.put(INSTANCE, cluster);
        m.put(COMPONENT, component);
        m.put(MANAGED_BY, OPERATOR);
        m.put(CLUSTER, cluster);
        return m;
    }

    /** A workload's pod selector: stable for the object's life (selectors are immutable). */
    public static Map<String, String> selector(String cluster, String component) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(NAME, APP);
        m.put(INSTANCE, cluster);
        m.put(COMPONENT, component);
        return m;
    }
}
