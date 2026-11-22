package com.pharos.operator.reconcile;

import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.render.Labels;
import com.pharos.operator.render.StorageRenderer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.api.reconciler.dependent.GarbageCollected;
import io.javaoperatorsdk.operator.processing.dependent.Creator;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.security.SecureRandom;

/**
 * The JOSDK dependent resources of a {@link PharosCluster}: one class per Kubernetes kind, one
 * instance per object, named after the object's key in {@link DesiredState}. They apply the desired
 * state with server-side apply (JOSDK's default for these kinds; ConfigMaps and Secrets use its
 * create/update path) and carry owner references, so deleting the cluster deletes them.
 *
 * <p>The informers select only objects labelled {@link Labels#MANAGED_SELECTOR}: the operator never
 * caches Secrets or other objects it does not own. The first dependent of each kind owns the
 * informer; the others share it by name.
 */
public final class Dependents {

    private Dependents() {}

    /** An object rendered into the {@link DesiredState} the reconciler put into the context. */
    abstract static class Rendered<R extends HasMetadata> extends CRUDKubernetesDependentResource<R, PharosCluster> {

        Rendered(Class<R> type) {
            super(type);
        }

        @Override
        protected R desired(PharosCluster primary, Context<PharosCluster> context) {
            DesiredState state = context.managedWorkflowAndDependentResourceContext()
                    .getMandatory(DesiredState.KEY, DesiredState.class);
            R desired = state.get(name(), resourceType());
            if (desired == null) {
                throw new IllegalStateException("no desired " + resourceType().getSimpleName() + " for '" + name() + "'");
            }
            return desired;
        }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class StatefulSets extends Rendered<StatefulSet> {
        public StatefulSets() { super(StatefulSet.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Deployments extends Rendered<Deployment> {
        public Deployments() { super(Deployment.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Services extends Rendered<Service> {
        public Services() { super(Service.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class ConfigMaps extends Rendered<ConfigMap> {
        public ConfigMaps() { super(ConfigMap.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Pvcs extends Rendered<PersistentVolumeClaim> {
        public Pvcs() { super(PersistentVolumeClaim.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Pdbs extends Rendered<PodDisruptionBudget> {
        public Pdbs() { super(PodDisruptionBudget.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class NetworkPolicies extends Rendered<NetworkPolicy> {
        public NetworkPolicies() { super(NetworkPolicy.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Jobs extends Rendered<Job> {
        public Jobs() { super(Job.class); }
    }

    /**
     * A generated Secret (the token, the MinIO credentials). Create-only: it is never updated, so a
     * reconcile, an operator restart or a spec change never regenerates or rotates it. Its random value
     * is generated only when the Secret is missing, and is never logged.
     */
    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Secrets extends KubernetesDependentResource<Secret, PharosCluster>
            implements Creator<Secret, PharosCluster>, GarbageCollected<PharosCluster> {

        private static final SecureRandom RANDOM = new SecureRandom();

        public Secrets() { super(Secret.class); }

        @Override
        protected Secret desired(PharosCluster primary, Context<PharosCluster> context) {
            return DesiredState.secret(name(), primary, RANDOM);
        }

        /** By name, so the random value is not generated just to find the existing Secret. */
        @Override
        protected ResourceID targetSecondaryResourceID(PharosCluster primary, Context<PharosCluster> context) {
            return new ResourceID(DesiredState.secretName(name(), primary), primary.getMetadata().getNamespace());
        }
    }

    /** Reconcile precondition of the minio-mode objects; in s3 mode they are left alone (never deleted). */
    public static class MinioMode implements Condition<HasMetadata, PharosCluster> {
        @Override
        public boolean isMet(DependentResource<HasMetadata, PharosCluster> dependentResource, PharosCluster primary,
                             Context<PharosCluster> context) {
            return StorageRenderer.minioMode(primary.getSpec());
        }
    }
}
