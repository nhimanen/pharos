package com.pharos.operator.status;

import com.pharos.operator.api.PharosCluster;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.ResourceEventAware;

import java.net.http.HttpClient;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The JOSDK event source behind autoscaling: one status {@link Watch} per {@code PharosCluster}
 * (opened by the first reconcile of a valid spec, closed when the resource is deleted or the
 * operator stops). A watch reads {@code /cluster/status} on its own threads and, after each read,
 * triggers a reconcile of its cluster; the reconciler only takes {@link Watch#latest()}, so a hung
 * pod never blocks a reconcile.
 */
public class ClusterStatusEventSource extends AbstractEventSource<Void, PharosCluster>
        implements ResourceEventAware<PharosCluster> {

    public static final String NAME = "cluster-status";

    /** One cluster's status reads. */
    public interface Watch extends AutoCloseable {

        /** The cluster token, from its Secret (memory only); null while the Secret does not exist. */
        void token(String token);

        /** The latest read, when the last attempt succeeded. */
        Optional<StatusReading> latest();

        /** Why there is no reading (no secret in it), or null. */
        String problem();

        @Override
        void close();
    }

    /** Opens a watch of cluster {@code name} in {@code namespace}; {@code onUpdate} runs after each read. */
    @FunctionalInterface
    public interface WatchFactory {
        Watch open(String name, String namespace, Runnable onUpdate);
    }

    /** A watch that never gets an answer: for tests that run no pharos node. */
    public static final WatchFactory NEVER_ANSWERS = (name, namespace, onUpdate) -> new Watch() {
        @Override public void token(String token) { }

        @Override public Optional<StatusReading> latest() { return Optional.empty(); }

        @Override public String problem() { return "no pharos node answered /cluster/status"; }

        @Override public void close() { }
    };

    /** Real clients: the pods' headless Services, a shared HTTP client. */
    public static WatchFactory http(Clock clock) {
        HttpClient http = ClusterStatusClient.httpClient();
        return (name, namespace, onUpdate) -> ClusterStatusClient.forCluster(name, namespace, http, clock, onUpdate)
                .start();
    }

    private final WatchFactory factory;
    private final Map<ResourceID, Watch> watches = new ConcurrentHashMap<>();

    public ClusterStatusEventSource(WatchFactory factory) {
        super(Void.class, NAME);
        this.factory = factory;
    }

    /** The cluster's watch, opened on first use. */
    public Watch watch(PharosCluster cluster) {
        ResourceID id = ResourceID.fromResource(cluster);
        return watches.computeIfAbsent(id, k -> factory.open(cluster.getMetadata().getName(),
                cluster.getMetadata().getNamespace(), () -> trigger(k)));
    }

    private void trigger(ResourceID id) {
        if (isRunning() && getEventHandler() != null) { // a closed watch reads no more
            getEventHandler().handleEvent(new Event(id));
        }
    }

    @Override
    public void onResourceDeleted(PharosCluster resource) {
        Watch w = watches.remove(ResourceID.fromResource(resource));
        if (w != null) w.close();
    }

    @Override
    public void stop() {
        watches.values().forEach(Watch::close);
        watches.clear();
        super.stop();
    }

    @Override
    public Set<Void> getSecondaryResources(PharosCluster primary) {
        return Set.of();
    }
}
