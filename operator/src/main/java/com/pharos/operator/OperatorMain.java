package com.pharos.operator;

import com.pharos.operator.reconcile.PharosClusterReconciler;
import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point of the pharos operator. It watches {@code PharosCluster}s in one namespace only:
 * {@code WATCH_NAMESPACE} (set from the pod's own namespace in deploy/operator.yaml), so the RBAC can
 * stay a namespaced Role. Without it, the client's current namespace is used.
 */
public final class OperatorMain {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorMain.class);

    private OperatorMain() {}

    public static void main(String[] args) {
        // Fail fast when the CRD is missing or differs from these classes (RBAC: get on that one CRD).
        Operator operator = new Operator(o -> o
                .withStopOnInformerErrorDuringStartup(true)
                .checkingCRDAndValidateLocalModel(true));
        String namespace = System.getenv("WATCH_NAMESPACE");
        if (namespace == null || namespace.isBlank()) {
            namespace = operator.getKubernetesClient().getNamespace();
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalStateException("set WATCH_NAMESPACE: the operator watches one namespace only");
        }
        String ns = namespace;
        OperatorSettings settings = OperatorSettings.fromEnv(System.getenv());
        operator.register(new PharosClusterReconciler(settings), c -> c.settingNamespace(ns));
        operator.installShutdownHook(Duration.ofSeconds(30));
        operator.start();
        LOG.info("pharos operator started, watching namespace {} (pharos image {})", ns, settings.pharosImage());
    }
}
