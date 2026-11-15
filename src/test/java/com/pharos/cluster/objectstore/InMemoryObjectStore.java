package com.pharos.cluster.objectstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe in-memory {@link ObjectStore} test double with UUID ETags and an injectable clock. */
public class InMemoryObjectStore implements ObjectStore {

    private record Entry(byte[] data, String etag, Instant lastModified) {}

    private final Map<String, Entry> map = new ConcurrentHashMap<>();
    private final Set<String> failSubstrings = ConcurrentHashMap.newKeySet();
    private final Clock clock;

    public InMemoryObjectStore() { this(Clock.systemUTC()); }

    public InMemoryObjectStore(Clock clock) { this.clock = clock; }

    /** Makes any later {@code download} whose key contains {@code keySubstring} throw IOException. */
    public void failOn(String keySubstring) { failSubstrings.add(keySubstring); }

    /** Removes all failure hooks. */
    public void clearFailures() { failSubstrings.clear(); }

    private Entry newEntry(byte[] data) {
        return new Entry(data.clone(), "\"" + UUID.randomUUID() + "\"", clock.instant());
    }

    @Override
    public Optional<StoredObject> get(String key) {
        Entry e = map.get(key);
        return e == null ? Optional.empty() : Optional.of(new StoredObject(e.data().clone(), e.etag()));
    }

    @Override
    public void download(String key, Path target) throws IOException {
        for (String s : failSubstrings) {
            if (key.contains(s)) throw new IOException("injected download failure for key: " + key);
        }
        Entry e = map.get(key);
        if (e == null) throw new NoSuchFileException(key, null, "object not found: " + key);
        Path abs = target.toAbsolutePath();
        Files.createDirectories(abs.getParent());
        Path tmp = Files.createTempFile(abs.getParent(), abs.getFileName().toString(), ".part");
        try {
            Files.write(tmp, e.data());
            Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public String put(String key, Path source) throws IOException {
        return put(key, Files.readAllBytes(source));
    }

    @Override
    public String put(String key, byte[] data) {
        Entry e = newEntry(data);
        map.put(key, e);
        return e.etag();
    }

    @Override
    public Optional<String> putIfAbsent(String key, byte[] data) {
        Entry e = newEntry(data);
        return map.putIfAbsent(key, e) == null ? Optional.of(e.etag()) : Optional.empty();
    }

    @Override
    public Optional<String> putIfMatch(String key, byte[] data, String etag) {
        Entry next = newEntry(data);
        boolean[] ok = {false};
        map.computeIfPresent(key, (k, cur) -> {
            if (cur.etag().equals(etag)) { ok[0] = true; return next; }
            return cur;
        });
        return ok[0] ? Optional.of(next.etag()) : Optional.empty();
    }

    @Override
    public List<ObjectInfo> list(String prefix) {
        List<ObjectInfo> out = new ArrayList<>();
        map.forEach((k, e) -> {
            if (k.startsWith(prefix)) out.add(new ObjectInfo(k, e.data().length, e.lastModified()));
        });
        out.sort(Comparator.comparing(ObjectInfo::key));
        return out;
    }

    @Override
    public void delete(String key) { map.remove(key); }

    @Override
    public void close() {}
}
