package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behaviour every {@link ObjectStore} implementation must satisfy. */
public abstract class ObjectStoreContract {

    @TempDir Path tmp;

    abstract ObjectStore store();

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void putThenGet() throws IOException {
        String etag = store().put("a/one.txt", b("hello"));
        Optional<StoredObject> got = store().get("a/one.txt");
        assertThat(got).isPresent();
        assertThat(got.get().data()).isEqualTo(b("hello"));
        assertThat(got.get().etag()).isEqualTo(etag);
    }

    @Test
    void getMissingIsEmpty() throws IOException {
        assertThat(store().get("nope/missing")).isEmpty();
    }

    @Test
    void downloadMissingThrowsNoSuchFile() {
        Path target = tmp.resolve("out.bin");
        assertThatThrownBy(() -> store().download("nope/missing", target))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessageContaining("nope/missing");
        assertThat(target).doesNotExist();
    }

    @Test
    void putIfAbsentOnlyOnce() throws IOException {
        assertThat(store().putIfAbsent("lock/x", b("first"))).isPresent();
        assertThat(store().putIfAbsent("lock/x", b("second"))).isEmpty();
        assertThat(store().get("lock/x").orElseThrow().data()).isEqualTo(b("first"));
    }

    @Test
    void putIfMatchSucceedsWithCurrentEtag() throws IOException {
        String etag = store().put("cas/k", b("v1"));
        Optional<String> next = store().putIfMatch("cas/k", b("v2"), etag);
        assertThat(next).isPresent();
        StoredObject got = store().get("cas/k").orElseThrow();
        assertThat(got.data()).isEqualTo(b("v2"));
        assertThat(got.etag()).isEqualTo(next.get());
        // the etag returned by get is accepted too
        assertThat(store().putIfMatch("cas/k", b("v3"), got.etag())).isPresent();
    }

    @Test
    void putIfMatchFailsWithStaleEtag() throws IOException {
        String stale = store().put("cas/s", b("v1"));
        store().put("cas/s", b("v2"));
        assertThat(store().putIfMatch("cas/s", b("v3"), stale)).isEmpty();
        assertThat(store().get("cas/s").orElseThrow().data()).isEqualTo(b("v2"));
    }

    @Test
    void putIfMatchOnMissingKeyIsEmpty() throws IOException {
        assertThat(store().putIfMatch("cas/missing", b("v"), "\"deadbeef\"")).isEmpty();
        assertThat(store().get("cas/missing")).isEmpty();
    }

    @Test
    void listByPrefixIncludesSizes() throws IOException {
        store().put("list/a", b("12345"));
        store().put("list/sub/b", b("123"));
        store().put("other/c", b("1"));
        List<ObjectInfo> infos = store().list("list/");
        assertThat(infos).extracting(ObjectInfo::key).containsExactlyInAnyOrder("list/a", "list/sub/b");
        assertThat(infos).filteredOn(i -> i.key().equals("list/a")).singleElement()
                .satisfies(i -> {
                    assertThat(i.size()).isEqualTo(5);
                    assertThat(i.lastModified()).isNotNull();
                });
    }

    @Test
    void deleteMissingIsNoop() throws IOException {
        store().delete("never/existed");
        store().put("del/x", b("x"));
        store().delete("del/x");
        assertThat(store().get("del/x")).isEmpty();
    }

    @Test
    void largeFileRoundTrip() throws IOException {
        byte[] data = new byte[8 * 1024 * 1024];
        new Random(42).nextBytes(data);
        Path src = tmp.resolve("big-src.bin");
        Files.write(src, data);
        store().put("big/blob", src);

        Path dst = tmp.resolve("sub/big-dst.bin");
        Files.createDirectories(dst.getParent());
        store().download("big/blob", dst);
        assertThat(Files.readAllBytes(dst)).isEqualTo(data);
        assertThat(store().list("big/")).singleElement().satisfies(i -> assertThat(i.size()).isEqualTo(data.length));
    }
}
