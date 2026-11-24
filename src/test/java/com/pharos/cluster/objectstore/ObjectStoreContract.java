package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
                .isInstanceOf(ObjectMissingException.class)
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

    private byte[] putRangeObject(String key) throws IOException {
        byte[] data = new byte[10_000];
        new Random(7).nextBytes(data);
        store().put(key, data);
        return data;
    }

    @Test
    void rangeReturnsExactBytes() throws IOException {
        byte[] data = putRangeObject("range/blob");
        long[][] ranges = {{0, 1}, {4095, 2}, {9990, 10}};
        for (long[] r : ranges) {
            Path t = tmp.resolve("r-" + r[0] + ".bin");
            store().downloadRange("range/blob", r[0], r[1], t);
            assertThat(Files.readAllBytes(t))
                    .isEqualTo(java.util.Arrays.copyOfRange(data, (int) r[0], (int) (r[0] + r[1])));
        }
    }

    @Test
    void rangePastEndIsEof() throws IOException {
        putRangeObject("range/short");
        Path t = tmp.resolve("eof.bin");
        assertThatThrownBy(() -> store().downloadRange("range/short", 9990, 11, t))
                .isInstanceOf(EOFException.class);
        assertThat(t).doesNotExist();
    }

    /** S3 answers 416 when the range starts at or past the end; that is an EOF, not a generic failure. */
    @Test
    void rangeStartingAtOrPastEndIsEof() throws IOException {
        putRangeObject("range/416");
        for (long offset : new long[] {10_000, 10_001, 20_000}) {
            Path t = tmp.resolve("416-" + offset + ".bin");
            assertThatThrownBy(() -> store().downloadRange("range/416", offset, 1, t))
                    .as("offset %d", offset).isInstanceOf(EOFException.class);
            assertThat(t).doesNotExist();
        }
    }

    /** A range ending exactly at the last byte is complete (206 with the full requested length). */
    @Test
    void rangeEndingAtLastByteIsComplete() throws IOException {
        byte[] data = putRangeObject("range/tail");
        Path t = tmp.resolve("tail.bin");
        store().downloadRange("range/tail", 9_000, 1_000, t);
        assertThat(Files.readAllBytes(t)).isEqualTo(java.util.Arrays.copyOfRange(data, 9_000, 10_000));
    }

    @Test
    void rangeOfMissingKeyIsNoSuchFile() {
        Path t = tmp.resolve("missing.bin");
        assertThatThrownBy(() -> store().downloadRange("range/nope", 0, 10, t))
                .isInstanceOf(ObjectMissingException.class);
        assertThat(t).doesNotExist();
    }

    @Test
    void zeroLengthRangeWritesEmptyFile() throws IOException {
        putRangeObject("range/zero");
        Path t = tmp.resolve("zero.bin");
        store().downloadRange("range/zero", 5, 0, t);
        assertThat(t).exists();
        assertThat(Files.size(t)).isZero();
    }

    @Test
    void zeroLengthRangeOfMissingKeyIsNoSuchFile() {
        Path t = tmp.resolve("zero-missing.bin");
        assertThatThrownBy(() -> store().downloadRange("range/nope-zero", 0, 0, t))
                .isInstanceOf(ObjectMissingException.class);
        assertThat(t).doesNotExist();
    }

    @Test
    void zeroLengthRangeAtEndIsEmptyButPastEndIsEof() throws IOException {
        putRangeObject("range/zero-end");
        Path atEnd = tmp.resolve("zero-at-end.bin");
        store().downloadRange("range/zero-end", 10_000, 0, atEnd);
        assertThat(Files.size(atEnd)).isZero();
        Path past = tmp.resolve("zero-past-end.bin");
        assertThatThrownBy(() -> store().downloadRange("range/zero-end", 10_001, 0, past))
                .isInstanceOf(EOFException.class);
        assertThat(past).doesNotExist();
    }

    @Test
    void rangeOverflowIsIllegalArgument() throws IOException {
        putRangeObject("range/overflow");
        Path t = tmp.resolve("overflow.bin");
        assertThatThrownBy(() -> store().downloadRange("range/overflow", Long.MAX_VALUE - 5, 10, t))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
        assertThat(t).doesNotExist();
    }

    private byte[] readRange(String key, long offset, long length) throws IOException {
        try (java.io.InputStream in = store().openRange(key, offset, length)) {
            return in.readAllBytes();
        }
    }

    @Test
    void openRangeStreamsExactBytes() throws IOException {
        byte[] data = putRangeObject("orange/blob");
        long[][] ranges = {{0, 1}, {4095, 2}, {9990, 10}, {0, 10_000}};
        for (long[] r : ranges) {
            assertThat(readRange("orange/blob", r[0], r[1]))
                    .isEqualTo(java.util.Arrays.copyOfRange(data, (int) r[0], (int) (r[0] + r[1])));
        }
    }

    @Test
    void openRangePastEndIsEof() throws IOException {
        putRangeObject("orange/short");
        assertThatThrownBy(() -> readRange("orange/short", 9990, 11)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> readRange("orange/short", 10_001, 1)).isInstanceOf(EOFException.class);
    }

    @Test
    void openRangeStartingAtEndIsEof() throws IOException {
        putRangeObject("orange/416");
        assertThatThrownBy(() -> readRange("orange/416", 10_000, 1)).isInstanceOf(EOFException.class);
    }

    @Test
    void openRangeOfMissingKeyIsNoSuchFile() {
        assertThatThrownBy(() -> readRange("orange/nope", 0, 10))
                .isInstanceOf(ObjectMissingException.class)
                .hasMessageContaining("orange/nope");
    }

    @Test
    void zeroLengthOpenRangeIsEmptyButChecksTheObject() throws IOException {
        putRangeObject("orange/zero");
        assertThat(readRange("orange/zero", 5, 0)).isEmpty();
        assertThat(readRange("orange/zero", 10_000, 0)).isEmpty();
        assertThatThrownBy(() -> readRange("orange/zero", 10_001, 0)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> readRange("orange/nope-zero", 0, 0)).isInstanceOf(ObjectMissingException.class);
    }

    @Test
    void openRangeOverflowIsIllegalArgument() throws IOException {
        putRangeObject("orange/overflow");
        assertThatThrownBy(() -> store().openRange("orange/overflow", Long.MAX_VALUE - 5, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
        assertThatThrownBy(() -> store().openRange("orange/overflow", -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
