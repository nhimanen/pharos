package com.pharos.cluster;

import com.pharos.graph.GraphViews;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reader cache: each shard is served from its active generation directory
 * ({@code <cacheDir>/<shard>/gen-N}, the shard id URL-safe encoded: {@link ShardId#pathSegment()}), switched by {@link #activate}. {@code LuceneIndexer} notices
 * the changed path on its next {@code openReader} and swaps readers. Activating another generation
 * evicts the previous one's cached compact graph ({@link GraphViews#evict}).
 *
 * <p><b>The previous generation (ruling P6-R19).</b> With a {@link #setPreviousGenerationGrace grace}, the
 * generation an activation replaces stays answerable ({@link #retainedDir}) for the grace: a coordinator that
 * still routes the shard at that generation (it planned the query before this reader switched, or its
 * catalog pins it) is answered from it instead of being refused. One previous generation per shard is kept;
 * after the grace (or when a newer activation replaces it) it is released: its graph evicted and every
 * {@link #onRelease} listener told (the {@code LuceneIndexer} retires its reader), after which its directory
 * is like any other old generation's (pruned by the syncer).
 */
public class ReaderLocalCache implements LocalCache {

    private static final Pattern GEN_DIR = Pattern.compile("gen-(\\d{1,18})");

    /** A shard's previous generation, answerable until {@code untilMillis}. */
    public record Retained(Path dir, long generation, long untilMillis) {}

    private final Path cacheDir;
    private final Map<String, Path> active = new ConcurrentHashMap<>();
    private final Map<String, Retained> retained = new ConcurrentHashMap<>();
    private final List<Consumer<String>> releaseListeners = new CopyOnWriteArrayList<>();
    private volatile Duration grace = Duration.ZERO;
    private volatile LongSupplier clock = System::currentTimeMillis;

    public ReaderLocalCache(Path cacheDir) {
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir");
    }

    /** How long a replaced generation stays answerable (P6-R19); zero (the default) keeps none. */
    public void setPreviousGenerationGrace(Duration grace) {
        this.grace = Objects.requireNonNull(grace, "grace");
    }

    public Duration previousGenerationGrace() {
        return grace;
    }

    /** The wall clock (millis) the grace is timed with (tests). */
    public void setClock(LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Called with the shard whenever its retained previous generation is released. */
    public void onRelease(Consumer<String> listener) {
        releaseListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    public void activate(String project, Path generationDir) {
        Objects.requireNonNull(generationDir, "generationDir");
        Path previous;
        Retained dropped = null;
        boolean keep = false;
        synchronized (this) {
            previous = active.put(project, generationDir);
            Retained was = retained.get(project);
            if (was != null && was.dir().equals(generationDir)) { // the retained one is active again
                retained.remove(project);
                was = null;
            }
            if (previous != null && !previous.equals(generationDir)) {
                long gen = generationOf(previous);
                if (!grace.isZero() && !grace.isNegative() && gen >= 0) {
                    retained.put(project, new Retained(previous, gen, clock.getAsLong() + grace.toMillis()));
                    keep = true;
                    dropped = was;
                } else {
                    dropped = retained.remove(project);
                }
            }
        }
        if (previous != null && !previous.equals(generationDir) && !keep) GraphViews.evict(previous);
        if (dropped != null) release(project, dropped);
    }

    /** Stops serving {@code project}; its generation dirs stay until the caller removes them. */
    public void deactivate(String project) {
        Path previous;
        Retained dropped;
        synchronized (this) {
            previous = active.remove(project);
            dropped = retained.remove(project);
        }
        if (previous != null) GraphViews.evict(previous);
        if (dropped != null) release(project, dropped);
    }

    /** Projects with an active generation. */
    public java.util.Set<String> activeProjects() {
        return java.util.Set.copyOf(active.keySet());
    }

    /** The active generation dir, or {@code <cacheDir>/<project>/none} (never created) before the first. */
    @Override
    public Path shardDir(String project) {
        Path dir = active.get(project);
        return dir != null ? dir : cacheDir.resolve(ShardId.pathSegmentOf(project)).resolve("none");
    }

    public Optional<Path> active(String project) {
        return Optional.ofNullable(active.get(project));
    }

    @Override
    public Optional<Path> retainedDir(String project, long generation) {
        Retained r = retained.get(project);
        if (r == null || r.generation() != generation || clock.getAsLong() >= r.untilMillis()) return Optional.empty();
        return Optional.of(r.dir());
    }

    /** The retained previous generation of {@code project} while its grace lasts. */
    public Optional<Retained> retained(String project) {
        Retained r = retained.get(project);
        return r == null || clock.getAsLong() >= r.untilMillis() ? Optional.empty() : Optional.of(r);
    }

    /** Every shard's retained previous generation number, while its grace lasts. */
    public Map<String, Long> retainedGenerations() {
        long now = clock.getAsLong();
        Map<String, Long> out = new HashMap<>();
        retained.forEach((p, r) -> { if (now < r.untilMillis()) out.put(p, r.generation()); });
        return out;
    }

    /**
     * Releases every retained generation whose grace is over (the syncer calls it each pass); returns the
     * shards released. Their directories are old generations from then on.
     */
    public List<String> expireRetained() {
        long now = clock.getAsLong();
        List<Map.Entry<String, Retained>> gone = new ArrayList<>();
        synchronized (this) {
            for (Map.Entry<String, Retained> e : List.copyOf(retained.entrySet())) {
                if (now >= e.getValue().untilMillis() && retained.remove(e.getKey(), e.getValue())) gone.add(e);
            }
        }
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Retained> e : gone) {
            release(e.getKey(), e.getValue());
            out.add(e.getKey());
        }
        return out;
    }

    private void release(String project, Retained r) {
        if (!r.dir().equals(active.get(project))) GraphViews.evict(r.dir());
        for (Consumer<String> l : releaseListeners) {
            try {
                l.accept(project);
            } catch (RuntimeException e) {
                org.slf4j.LoggerFactory.getLogger(ReaderLocalCache.class)
                        .warn("Releasing the previous generation of shard {} failed: {}", project, e.toString());
            }
        }
    }

    /** {@code gen-N} → N, else -1. */
    static long generationOf(Path dir) {
        if (dir == null || dir.getFileName() == null) return -1;
        Matcher m = GEN_DIR.matcher(dir.getFileName().toString());
        return m.matches() ? Long.parseLong(m.group(1)) : -1;
    }
}
