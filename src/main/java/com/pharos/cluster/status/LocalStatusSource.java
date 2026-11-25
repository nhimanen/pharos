package com.pharos.cluster.status;

import com.pharos.cluster.membership.LoadCounter;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.writer.WriterHeartbeat;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The single-node ({@code role: all}) snapshot: one node, both writer and reader, holding every
 * registered project as a shard. Doc counts come from the open index, sizes from the Lucene
 * directory, ages from the last index run; there are no generations or leases. Nothing is read from
 * or written to a bucket, and {@code asOf} is the node's clock.
 */
public final class LocalStatusSource implements StatusSource {

    private final String node;
    private final ProjectRegistry registry;
    private final LuceneIndexer lucene;
    private final Supplier<LoadCounter> load;
    private final Clock clock;
    private final long startedAt;

    public LocalStatusSource(String node, ProjectRegistry registry, LuceneIndexer lucene, Supplier<LoadCounter> load,
                             Clock clock) {
        this.node = node;
        this.registry = registry;
        this.lucene = lucene;
        this.load = load;
        this.clock = clock;
        this.startedAt = clock.millis();
    }

    @Override
    public StatusSnapshot read() {
        Instant now = clock.instant();
        List<StatusSnapshot.ShardObs> shards = new ArrayList<>();
        TreeSet<String> names = new TreeSet<>();
        for (ProjectMeta m : registry.listAll()) {
            if (m.getName() == null || !names.add(m.getName())) continue;
            shards.add(new StatusSnapshot.ShardObs(m.getName(), false, null, docCount(m.getName()),
                    sizeBytes(m.getName()), node, m.getLastIndexed(), m.getSourceRevision(), null));
        }
        List<String> all = List.copyOf(names);
        WriterHeartbeat writer = new WriterHeartbeat(node, false, all, List.of(), null, null, startedAt, false);
        LoadCounter l = load.get();
        ReaderHeartbeat reader = new ReaderHeartbeat(node, null, all, false, null, l.inFlight(), l.qps(), startedAt,
                ReaderHeartbeat.READY, List.of(), l.warmP95Millis(), null);
        return new StatusSnapshot(StatusSnapshot.SINGLE_NODE, now, shards,
                List.of(new StatusSnapshot.WriterObs(writer, now)), List.of(new StatusSnapshot.ReaderObs(reader, now)),
                null);
    }

    private Long docCount(String project) {
        if (lucene == null) return null;
        long n = lucene.committedDocCount(project); // segment infos only: no reader opened or refreshed
        return n < 0 ? null : n;
    }

    private Long sizeBytes(String project) {
        if (lucene == null) return null;
        Path dir = lucene.getLucenePath(project);
        if (!Files.isDirectory(dir)) return null;
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0;
                }
            }).sum();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
