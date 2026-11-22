package com.pharos.operator.reconcile;

import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.render.Labels;
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
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.dependent.ReconcileResult;
import io.javaoperatorsdk.operator.processing.dependent.Creator;
import io.javaoperatorsdk.operator.processing.dependent.Updater;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;

/**
 * The JOSDK dependent resources of a {@link PharosCluster}: one class per Kubernetes kind, one
 * instance per object, named after the object's key in {@link DesiredState}. They apply the desired
 * state with server-side apply (JOSDK's default for these kinds; ConfigMaps and Secrets use its
 * create/update path).
 *
 * <p><b>None of them can delete anything.</b> They implement neither {@code Deleter} nor
 * {@code GarbageCollected}: JOSDK deletes a {@code Deleter} whose precondition stops holding, which
 * would lose data-bearing objects (the MinIO PVC, the generated Secrets). Instead each object carries
 * an owner reference to the cluster, so Kubernetes removes them only when the cluster is deleted. An
 * object the desired state does not contain (the minio objects in s3 mode) is left alone.
 *
 * <p>The informers select only objects labelled {@link Labels#MANAGED_SELECTOR}: the operator never
 * caches Secrets or other objects it does not own. The first dependent of each kind owns the
 * informer; the others share it by name.
 */
public final class Dependents {

    /** Every dependent class, for the test that none of them can delete. */
    static final List<Class<?>> ALL = List.of(StatefulSets.class, Deployments.class, Services.class,
            ConfigMaps.class, Pvcs.class, Pdbs.class, NetworkPolicies.class, Jobs.class, Secrets.class);

    private Dependents() {}

    /** Owned by the cluster (owner reference), never deleted by the operator. */
    abstract static class Owned<R extends HasMetadata> extends KubernetesDependentResource<R, PharosCluster> {

        Owned(Class<R> type) {
            super(type);
        }

        @Override
        protected boolean addOwnerReference() {
            return true;
        }

        @Override
        protected Optional<SecondaryToPrimaryMapper<R>> getSecondaryToPrimaryMapper(
                EventSourceContext<PharosCluster> context) {
            return Optional.of(Mappers.fromOwnerReferences(context.getPrimaryResourceClass(), false));
        }

        static DesiredState state(Context<PharosCluster> context) {
            return context.managedWorkflowAndDependentResourceContext().getMandatory(DesiredState.KEY, DesiredState.class);
        }
    }

    /** An object rendered into the {@link DesiredState}; skipped when the state has none for it. */
    abstract static class Rendered<R extends HasMetadata> extends Owned<R> implements Creator<R, PharosCluster> {

        Rendered(Class<R> type) {
            super(type);
        }

        @Override
        public ReconcileResult<R> reconcile(PharosCluster primary, Context<PharosCluster> context) {
            if (state(context).get(name(), resourceType()) == null) return ReconcileResult.noOperation(null);
            return super.reconcile(primary, context);
        }

        @Override
        protected R desired(PharosCluster primary, Context<PharosCluster> context) {
            return state(context).get(name(), resourceType());
        }
    }

    /** Created and kept in line with the desired state. */
    abstract static class Applied<R extends HasMetadata> extends Rendered<R> implements Updater<R, PharosCluster> {
        Applied(Class<R> type) {
            super(type);
        }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class StatefulSets extends Applied<StatefulSet> {
        public StatefulSets() { super(StatefulSet.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Deployments extends Applied<Deployment> {
        public Deployments() { super(Deployment.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Services extends Applied<Service> {
        public Services() { super(Service.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class ConfigMaps extends Applied<ConfigMap> {
        public ConfigMaps() { super(ConfigMap.class); }
    }

    /** The MinIO PVC: its desired size is never below the current one (see the reconciler). */
    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Pvcs extends Applied<PersistentVolumeClaim> {
        public Pvcs() { super(PersistentVolumeClaim.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Pdbs extends Applied<PodDisruptionBudget> {
        public Pdbs() { super(PodDisruptionBudget.class); }
    }

    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class NetworkPolicies extends Applied<NetworkPolicy> {
        public NetworkPolicies() { super(NetworkPolicy.class); }
    }

    /**
     * Jobs are create-only: a Job's pod template is immutable, so a changed image (a new
     * {@code MC_IMAGE}) never fails a reconcile; it applies when the Job is next created.
     */
    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Jobs extends Rendered<Job> {
        public Jobs() { super(Job.class); }
    }

    /**
     * A generated Secret (the token, the MinIO credentials). Create-only: it is never updated, so a
     * reconcile, an operator restart or a spec change never regenerates or rotates it. Its random value
     * is generated only when the Secret is missing, and never reaches a log or an error message.
     */
    @KubernetesDependent(informer = @Informer(labelSelector = Labels.MANAGED_SELECTOR))
    public static class Secrets extends Owned<Secret> implements Creator<Secret, PharosCluster> {

        private static final SecureRandom RANDOM = new SecureRandom();

        public Secrets() { super(Secret.class); }

        @Override
        public ReconcileResult<Secret> reconcile(PharosCluster primary, Context<PharosCluster> context) {
            if (!state(context).secrets().contains(name())) return ReconcileResult.noOperation(null);
            return super.reconcile(primary, context);
        }

        @Override
        protected Secret desired(PharosCluster primary, Context<PharosCluster> context) {
            return DesiredState.secret(name(), primary, RANDOM);
        }

        /** By name, so the random value is not generated just to find the existing Secret. */
        @Override
        protected ResourceID targetSecondaryResourceID(PharosCluster primary, Context<PharosCluster> context) {
            return new ResourceID(DesiredState.secretName(name(), primary), primary.getMetadata().getNamespace());
        }

        /** The API server's error could echo the request body: report only the name and the HTTP code. */
        @Override
        public Secret create(Secret desired, PharosCluster primary, Context<PharosCluster> context) {
            try {
                return super.create(desired, primary, context);
            } catch (KubernetesClientException e) {
                throw new IllegalStateException("creating Secret " + desired.getMetadata().getName()
                        + " failed (HTTP " + e.getCode() + ")");
            }
        }
    }
}
