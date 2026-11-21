package com.pharos.cluster.peer;

import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectMissingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FsObjectStoreTest {

    @TempDir Path tmp;

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void conditionalWritesCompareContentEtags() throws Exception {
        FsObjectStore store = new FsObjectStore(tmp.resolve("store"));
        assertThat(store.get("shards/a/LATEST")).isEmpty();
        Optional<String> first = store.putIfAbsent("shards/a/LATEST", b("one"));
        assertThat(first).isPresent();
        assertThat(store.putIfAbsent("shards/a/LATEST", b("two"))).isEmpty();
        assertThat(store.get("shards/a/LATEST").orElseThrow().etag()).isEqualTo(first.get());

        assertThat(store.putIfMatch("shards/a/LATEST", b("two"), "stale")).isEmpty();
        Optional<String> second = store.putIfMatch("shards/a/LATEST", b("two"), first.get());
        assertThat(second).isPresent().isNotEqualTo(first);
        assertThat(new String(store.get("shards/a/LATEST").orElseThrow().data(), StandardCharsets.UTF_8)).isEqualTo("two");
        assertThat(store.putIfMatch("shards/a/missing", b("x"), second.get())).isEmpty();
    }

    @Test
    void racingConditionalWritesHaveOneWinner() throws Exception {
        FsObjectStore store = new FsObjectStore(tmp.resolve("store"));
        String etag = store.put("catalog/shards.json", b("v0"));
        AtomicInteger wins = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> fs = new java.util.ArrayList<>();
            for (int i = 0; i < 16; i++) {
                int n = i;
                fs.add(pool.submit(() -> {
                    if (store.putIfMatch("catalog/shards.json", b("v" + (n + 1)), etag).isPresent()) wins.incrementAndGet();
                    return null;
                }));
            }
            for (Future<?> f : fs) f.get();
        } finally {
            pool.shutdownNow();
        }
        assertThat(wins.get()).isEqualTo(1);
    }

    @Test
    void putOfAFileLinksOrCopiesAndStampsNow() throws Exception {
        FsObjectStore store = new FsObjectStore(tmp.resolve("store"));
        Path src = tmp.resolve("_0.cfs");
        Files.write(src, b("lucene bytes"));
        Files.setLastModifiedTime(src, java.nio.file.attribute.FileTime.from(Instant.parse("2020-01-01T00:00:00Z")));
        Instant before = Instant.now().minusSeconds(2);
        store.put("shards/a/lucene/_0.cfs@abc", src);
        ObjectInfo info = store.stat("shards/a/lucene/_0.cfs@abc").orElseThrow();
        assertThat(info.size()).isEqualTo(12);
        assertThat(info.lastModified()).as("GC age counts from the publish").isAfter(before);
        try (InputStream in = store.openRange("shards/a/lucene/_0.cfs@abc", 7, 5)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("bytes");
        }
        assertThatThrownBy(() -> store.openRange("shards/a/lucene/_0.cfs@abc", 7, 6)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> store.openRange("shards/a/lucene/gone@1", 0, 1)).isInstanceOf(ObjectMissingException.class);
        Path dl = tmp.resolve("dl/x");
        store.downloadRange("shards/a/lucene/_0.cfs@abc", 0, 6, dl);
        assertThat(Files.readString(dl)).isEqualTo("lucene");
        assertThatThrownBy(() -> store.download("shards/a/lucene/gone@1", dl)).isInstanceOf(ObjectMissingException.class);
    }

    @Test
    void listingSkipsReservedEntriesAndDeleteIsIdempotent() throws Exception {
        FsObjectStore store = new FsObjectStore(tmp.resolve("store"));
        store.put("shards/a/LATEST", b("1"));
        store.put("shards/a/manifests/gen-1.json", b("{}"));
        store.put("shards/ab/LATEST", b("2"));
        store.putIfAbsent("catalog/shards.json", b("{}")); // takes the lock: .lock exists now
        assertThat(store.list("shards/a/").stream().map(ObjectInfo::key))
                .containsExactly("shards/a/LATEST", "shards/a/manifests/gen-1.json");
        assertThat(store.list("shards/a").stream().map(ObjectInfo::key)).contains("shards/ab/LATEST");
        assertThat(store.list("shards/a/LATEST").stream().map(ObjectInfo::key)).containsExactly("shards/a/LATEST");
        assertThat(store.list("").stream().map(ObjectInfo::key)).noneMatch(k -> k.startsWith("."));
        assertThat(store.list("nothing/here/")).isEmpty();
        store.delete("shards/a/LATEST");
        store.delete("shards/a/LATEST");
        assertThat(store.stat("shards/a/LATEST")).isEmpty();
    }

    @Test
    void keysCannotEscapeTheRoot() throws Exception {
        FsObjectStore store = new FsObjectStore(tmp.resolve("store"));
        for (String bad : new String[]{"../x", "a/../../x", "/etc/passwd", "a//b", ".tmp/x", ".lock", "a/", "", "a/.hidden"}) {
            assertThat(FsObjectStore.validKey(bad)).as(bad).isFalse();
            assertThatThrownBy(() -> store.get(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(FsObjectStore.validKey("shards/a/lucene/_0.cfs@1f")).isTrue();
        assertThat(FsObjectStore.validKey("shards/a/graph/gen-3@ab12.cgf.gz")).isTrue();
    }
}
