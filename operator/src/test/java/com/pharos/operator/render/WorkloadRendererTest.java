package com.pharos.operator.render;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class WorkloadRendererTest {

    private static final OperatorSettings SETTINGS = new OperatorSettings("ghcr.io/example/pharos", "minio:x", "mc:y");

    private static DesiredState state(PharosCluster c) {
        ConfigRenderer.Result configs = ConfigRenderer.render(c);
        assertThat(configs.errors()).isEmpty();
        return DesiredState.render(c, configs, SETTINGS, 1, 2);
    }

    private static List<PodTemplateSpec> pharosPods(DesiredState s) {
        return List.of(s.get(DesiredState.WRITERS, StatefulSet.class).getSpec().getTemplate(),
                s.get(DesiredState.READERS, StatefulSet.class).getSpec().getTemplate(),
                s.get(DesiredState.ROUTER, Deployment.class).getSpec().getTemplate());
    }

    private static Container main(PodTemplateSpec t) {
        return t.getSpec().getContainers().stream().filter(c -> c.getName().equals("pharos")).findFirst().orElseThrow();
    }

    private static EnvVar env(Container c, String name) {
        return c.getEnv().stream().filter(e -> e.getName().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void producesTheNamedObjects() {
        DesiredState s = state(TestClusters.minio());
        assertThat(s.get(DesiredState.CONFIG_WRITER, ConfigMap.class).getMetadata().getName()).isEqualTo("demo-config-writer");
        assertThat(s.get(DesiredState.CONFIG_READER, ConfigMap.class).getMetadata().getName()).isEqualTo("demo-config-reader");
        assertThat(s.get(DesiredState.CONFIG_ROUTER, ConfigMap.class).getMetadata().getName()).isEqualTo("demo-config-router");
        assertThat(s.get(DesiredState.WRITERS, StatefulSet.class).getMetadata().getName()).isEqualTo("demo-writers");
        assertThat(s.get(DesiredState.READERS, StatefulSet.class).getMetadata().getName()).isEqualTo("demo-readers");
        assertThat(s.get(DesiredState.ROUTER, Deployment.class).getMetadata().getName()).isEqualTo("demo-router");
        assertThat(s.get(DesiredState.ROUTER_SERVICE, Service.class).getMetadata().getName()).isEqualTo("demo-router");
        assertThat(s.get(DesiredState.WRITERS_HEADLESS, Service.class).getMetadata().getName()).isEqualTo("demo-writers-hl");
        assertThat(s.get(DesiredState.READERS_HEADLESS, Service.class).getMetadata().getName()).isEqualTo("demo-readers-hl");
        for (HasMetadata o : s.objects().values()) {
            assertThat(o.getMetadata().getNamespace()).isEqualTo("pharos");
            assertThat(o.getMetadata().getLabels()).containsEntry(Labels.MANAGED_BY, Labels.OPERATOR)
                    .containsEntry(Labels.CLUSTER, "demo");
        }
    }

    @Test
    void statefulSetsUseHeadlessServicesAndTheirVolumeClaims() {
        DesiredState s = state(TestClusters.minio());
        StatefulSet w = s.get(DesiredState.WRITERS, StatefulSet.class);
        StatefulSet r = s.get(DesiredState.READERS, StatefulSet.class);
        assertThat(w.getSpec().getServiceName()).isEqualTo("demo-writers-hl");
        assertThat(r.getSpec().getServiceName()).isEqualTo("demo-readers-hl");
        assertThat(w.getSpec().getReplicas()).isEqualTo(1);
        assertThat(r.getSpec().getReplicas()).isEqualTo(2);
        assertThat(w.getSpec().getVolumeClaimTemplates()).singleElement().satisfies(v -> {
            assertThat(v.getMetadata().getName()).isEqualTo("work");
            assertThat(v.getSpec().getResources().getRequests().get("storage").toString()).isEqualTo("50Gi");
        });
        assertThat(r.getSpec().getVolumeClaimTemplates()).singleElement().satisfies(v -> {
            assertThat(v.getMetadata().getName()).isEqualTo("cache");
            assertThat(v.getSpec().getResources().getRequests().get("storage").toString()).isEqualTo("20Gi");
        });
        // the claim holds PHAROS_HOME and the work dir (/data/work) or the cache (/data/cache)
        assertThat(main(w.getSpec().getTemplate()).getVolumeMounts())
                .anySatisfy(m -> { assertThat(m.getName()).isEqualTo("work"); assertThat(m.getMountPath()).isEqualTo("/data"); });
        assertThat(main(r.getSpec().getTemplate()).getVolumeMounts())
                .anySatisfy(m -> { assertThat(m.getName()).isEqualTo("cache"); assertThat(m.getMountPath()).isEqualTo("/data"); });
        assertThat(env(main(w.getSpec().getTemplate()), "PHAROS_HOME").getValue()).isEqualTo("/data/pharos");
        assertThat(main(w.getSpec().getTemplate()).getResources().getRequests().get("cpu").toString()).isEqualTo("1");

        Service hl = s.get(DesiredState.WRITERS_HEADLESS, Service.class);
        assertThat(hl.getSpec().getClusterIP()).isEqualTo("None");
        assertThat(hl.getSpec().getSelector()).isEqualTo(w.getSpec().getSelector().getMatchLabels());
    }

    @Test
    void eachPodFillsInItsOwnNameAsNodeIdAndAdvertisedHost() {
        DesiredState s = state(TestClusters.minio());
        PodSpec pod = s.get(DesiredState.READERS, StatefulSet.class).getSpec().getTemplate().getSpec();
        Container init = pod.getInitContainers().getFirst();
        assertThat(init.getEnv()).singleElement().satisfies(e -> {
            assertThat(e.getName()).isEqualTo("POD_NAME");
            assertThat(e.getValueFrom().getFieldRef().getFieldPath()).isEqualTo("metadata.name");
        });
        String script = init.getCommand().get(2);
        assertThat(script).doesNotContain("$(") // Kubernetes would expand it itself
                .contains("${POD_NAME}").contains("/etc/pharos-template/config.json").contains("> /etc/pharos/config.json");
        assertThat(s.get(DesiredState.CONFIG_READER, ConfigMap.class).getData().get("config.json"))
                .contains("\"advertiseUrl\" : \"http://$(POD_NAME).demo-readers-hl.pharos.svc:7070\"")
                .contains("\"nodeId\" : \"$(POD_NAME)\"");
        // the router has nothing per pod: its ConfigMap is mounted directly
        assertThat(s.get(DesiredState.ROUTER, Deployment.class).getSpec().getTemplate().getSpec().getInitContainers()).isEmpty();
    }

    @Test
    void gracePeriodIsDrainPlus30() {
        for (PodTemplateSpec t : pharosPods(state(TestClusters.minio()))) {
            assertThat(t.getSpec().getTerminationGracePeriodSeconds()).isEqualTo(20L + 30); // pharos's default drain
        }
        PharosCluster base = TestClusters.minio();
        PharosCluster drain90 = TestClusters.withSpec(base, TestClusters.withPharosConfig(base.getSpec(),
                Map.of("drainSeconds", JsonNodeFactory.instance.numberNode(90))));
        for (PodTemplateSpec t : pharosPods(state(drain90))) {
            assertThat(t.getSpec().getTerminationGracePeriodSeconds()).isEqualTo(120L);
        }
    }

    @Test
    void podsRunAsTheImageUserWithProbesOnHealth() {
        for (PodTemplateSpec t : pharosPods(state(TestClusters.minio()))) {
            var sc = t.getSpec().getSecurityContext();
            assertThat(sc.getRunAsUser()).isEqualTo(10001L);
            assertThat(sc.getRunAsGroup()).isEqualTo(10001L);
            assertThat(sc.getFsGroup()).isEqualTo(10001L);
            assertThat(sc.getRunAsNonRoot()).isTrue();
            assertThat(t.getSpec().getAutomountServiceAccountToken()).isFalse();
            Container c = main(t);
            assertThat(c.getSecurityContext().getAllowPrivilegeEscalation()).isFalse();
            assertThat(c.getReadinessProbe().getHttpGet().getPath()).isEqualTo("/health");
            assertThat(c.getLivenessProbe().getExec().getCommand().get(2)).contains("GET /health");
            assertThat(c.getLivenessProbe().getTimeoutSeconds()).isGreaterThanOrEqualTo(10);
            assertThat(c.getStartupProbe().getExec().getCommand().get(2)).contains("GET /health");
            assertThat(t.getMetadata().getAnnotations()).containsKey(WorkloadRenderer.CONFIG_HASH);
        }
    }

    @Test
    void secretsReachPodsOnlyAsEnvReferences() {
        for (PodTemplateSpec t : pharosPods(state(TestClusters.minio()))) {
            Container c = main(t);
            assertThat(env(c, "PHAROS_TOKEN").getValueFrom().getSecretKeyRef().getName()).isEqualTo("demo-token");
            assertThat(env(c, "PHAROS_TOKEN").getValueFrom().getSecretKeyRef().getKey()).isEqualTo("token");
            assertThat(env(c, "PHAROS_S3_ACCESS_KEY").getValueFrom().getSecretKeyRef().getName()).isEqualTo("demo-minio");
            assertThat(env(c, "PHAROS_S3_SECRET_KEY").getValueFrom().getSecretKeyRef().getKey()).isEqualTo("secretKey");
            assertThat(c.getEnv()).allSatisfy(e -> {
                if (e.getName().startsWith("PHAROS_TOKEN") || e.getName().startsWith("PHAROS_S3")) {
                    assertThat(e.getValue()).isNull();
                }
            });
            assertThat(c.getImage()).isEqualTo("ghcr.io/example/pharos:1.6.0");
        }
    }

    @Test
    void pdbsAreMaxUnavailableOne() {
        DesiredState s = state(TestClusters.minio());
        for (String key : List.of(DesiredState.PDB_WRITERS, DesiredState.PDB_READERS)) {
            PodDisruptionBudget pdb = s.get(key, PodDisruptionBudget.class);
            assertThat(pdb.getSpec().getMaxUnavailable().getIntVal()).isEqualTo(1);
            assertThat(pdb.getSpec().getMinAvailable()).isNull();
        }
        assertThat(s.get(DesiredState.PDB_WRITERS, PodDisruptionBudget.class).getSpec().getSelector().getMatchLabels())
                .isEqualTo(s.get(DesiredState.WRITERS, StatefulSet.class).getSpec().getSelector().getMatchLabels());
        assertThat(s.get(DesiredState.PDB_READERS, PodDisruptionBudget.class).getSpec().getSelector().getMatchLabels())
                .isEqualTo(s.get(DesiredState.READERS, StatefulSet.class).getSpec().getSelector().getMatchLabels());
    }

    @Test
    void networkPolicyLimitsReplicationAndInternalPorts() {
        DesiredState s = state(TestClusters.minio());
        NetworkPolicy np = s.get(DesiredState.NETWORK_POLICY, NetworkPolicy.class);
        // selects the writers and readers, whose port serves /internal and /replication ...
        var selector = np.getSpec().getPodSelector();
        assertThat(selector.getMatchLabels()).containsEntry(Labels.INSTANCE, "demo");
        assertThat(selector.getMatchExpressions()).singleElement().satisfies(e -> {
            assertThat(e.getKey()).isEqualTo(Labels.COMPONENT);
            assertThat(e.getOperator()).isEqualTo("In");
            assertThat(e.getValues()).containsExactlyInAnyOrder("writer", "reader");
        });
        assertThat(np.getSpec().getPolicyTypes()).containsExactly("Ingress");
        // ... admitting only pods of this cluster (the router among them) and the operator, on that port
        NetworkPolicyIngressRule rule = np.getSpec().getIngress().getFirst();
        assertThat(np.getSpec().getIngress()).hasSize(1);
        assertThat(rule.getPorts()).singleElement().satisfies(p -> assertThat(p.getPort().getIntVal()).isEqualTo(7070));
        assertThat(rule.getFrom()).extracting(f -> f.getPodSelector().getMatchLabels())
                .containsExactlyInAnyOrder(Map.of(Labels.CLUSTER, "demo"), Map.of(Labels.NAME, "pharos-operator"));
        assertThat(rule.getFrom()).allSatisfy(f -> assertThat(f.getNamespaceSelector()).isNull());
        PodTemplateSpec router = s.get(DesiredState.ROUTER, Deployment.class).getSpec().getTemplate();
        assertThat(router.getMetadata().getLabels()).containsEntry(Labels.CLUSTER, "demo");
        // the router's pods are not selected: its port stays open
        assertThat(router.getMetadata().getLabels().get(Labels.COMPONENT)).isNotIn(selector.getMatchExpressions().getFirst().getValues());
        for (PodTemplateSpec t : pharosPods(s)) {
            assertThat(t.getMetadata().getLabels()).containsEntry(Labels.CLUSTER, "demo");
        }
    }

    @Test
    void routerServiceIsThePublicEndpoint() {
        DesiredState s = state(TestClusters.s3());
        Service svc = s.get(DesiredState.ROUTER_SERVICE, Service.class);
        assertThat(svc.getSpec().getType()).isEqualTo("LoadBalancer");
        assertThat(svc.getSpec().getPorts()).singleElement().satisfies(p -> assertThat(p.getPort()).isEqualTo(7080));
        assertThat(s.get(DesiredState.ROUTER, Deployment.class).getSpec().getReplicas()).isEqualTo(2);
        assertThat(env(main(s.get(DesiredState.ROUTER, Deployment.class).getSpec().getTemplate()), "PHAROS_ROLE").getValue())
                .isEqualTo("router");
    }

    @Test
    void everyNameFitsWithALongClusterName() {
        PharosCluster c = TestClusters.minio();
        c.getMetadata().setName("a".repeat(Names.MAX_CLUSTER_NAME));
        DesiredState s = state(c);
        Stream.concat(s.objects().values().stream(), Stream.of(
                        WorkloadRenderer.tokenSecret(c, new java.util.Random(1)),
                        StorageRenderer.minioCredentials(c, new java.util.Random(1))))
                .forEach(o -> assertThat(o.getMetadata().getName()).hasSizeLessThanOrEqualTo(Names.MAX_NAME));
        // a StatefulSet's pods carry controller-revision-hash = <name>-<10 chars>, also a 63-char label value
        assertThat(s.get(DesiredState.WRITERS, StatefulSet.class).getMetadata().getName().length() + 11)
                .isLessThanOrEqualTo(Names.MAX_NAME);
    }
}
