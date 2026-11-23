package com.pharos.operator.api;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * A multi-node pharos cluster: object-store storage (operator-managed MinIO or an existing S3 bucket),
 * writers, readers and the router. {@code .status} is written only by the operator's reconciler.
 */
@Group(PharosCluster.GROUP)
@Version(PharosCluster.VERSION)
@Kind("PharosCluster")
@Plural("pharosclusters")
@Singular("pharoscluster")
@ShortNames("pharos")
public class PharosCluster extends CustomResource<PharosClusterSpec, PharosClusterStatus> implements Namespaced {
    public static final String GROUP = "pharos.dev";
    public static final String VERSION = "v1alpha1";
}
