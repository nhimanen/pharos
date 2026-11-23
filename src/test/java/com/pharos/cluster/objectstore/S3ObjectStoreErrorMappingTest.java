package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ObjectStoreErrorMappingTest {

    private static S3Exception err(int status, String code) {
        return (S3Exception) S3Exception.builder().statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build()).build();
    }

    @Test
    void noSuchKeyIsMissingKey() {
        assertThat(S3ObjectStore.isMissingKey(err(404, "NoSuchKey"))).isTrue();
        assertThat(S3ObjectStore.isMissingKey(NoSuchKeyException.builder().statusCode(404).build())).isTrue();
    }

    @Test
    void noSuchBucketIsNotMissingKey() {
        assertThat(S3ObjectStore.isMissingKey(err(404, "NoSuchBucket"))).isFalse();
    }

    @Test
    void preconditionFailedIsNotMissingKey() {
        assertThat(S3ObjectStore.isMissingKey(err(412, "PreconditionFailed"))).isFalse();
        assertThat(S3ObjectStore.isMissingKey((S3Exception) S3Exception.builder().statusCode(404).build())).isFalse();
    }

    @Test
    void anyNotFoundOnHeadIsMissing() {
        // A HEAD response has no body, so NoSuchKey can't be told from other 404s.
        assertThat(S3ObjectStore.isMissingOnHead((S3Exception) S3Exception.builder().statusCode(404).build())).isTrue();
        assertThat(S3ObjectStore.isMissingOnHead(err(404, "NoSuchKey"))).isTrue();
        assertThat(S3ObjectStore.isMissingOnHead(err(403, "AccessDenied"))).isFalse();
    }

    @Test
    void contentRangeStartingAtOffsetIsAccepted() throws Exception {
        S3ObjectStore.checkContentRange("bytes 100-199/1000", 100, "k");
        S3ObjectStore.checkContentRange("bytes 0-9/10", 0, "k");
    }

    @Test
    void contentRangeStartingElsewhereIsRejected() {
        assertThatThrownBy(() -> S3ObjectStore.checkContentRange("bytes 0-99/1000", 100, "k"))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("Content-Range");
    }

    @Test
    void missingContentRangeIsRejectedUnlessOffsetIsZero() throws Exception {
        // A 200 that ignored Range carries no Content-Range: its body starts at byte 0.
        assertThatThrownBy(() -> S3ObjectStore.checkContentRange(null, 100, "k"))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("ignored the Range");
        S3ObjectStore.checkContentRange(null, 0, "k");
    }

    @Test
    void unparseableContentRangeIsRejected() {
        assertThatThrownBy(() -> S3ObjectStore.checkContentRange("garbage", 100, "k"))
                .isInstanceOf(java.io.IOException.class);
    }

    // ── RangeStream: abort the connection instead of draining it ─────────────────────────────

    /** A body of {@code n} bytes that counts close() calls and can fail on read. */
    private static final class Body extends java.io.ByteArrayInputStream {
        int closes;
        RuntimeException failWith;

        Body(int n) { super(new byte[n]); }

        @Override
        public synchronized int read(byte[] b, int off, int len) {
            if (failWith != null) throw failWith;
            return super.read(b, off, len);
        }

        @Override
        public void close() { closes++; }
    }

    private static S3ObjectStore.RangeStream range(Body body, java.util.concurrent.atomic.AtomicInteger aborts,
                                                   long length) {
        return new S3ObjectStore.RangeStream(body, aborts::incrementAndGet, length,
                (op, e) -> new java.io.IOException(op + " failed for key 'k': " + e.getMessage(), e));
    }

    @Test
    void fullyReadRangeClosesWithoutAbort() throws Exception {
        Body body = new Body(10);
        var aborts = new java.util.concurrent.atomic.AtomicInteger();
        try (var in = range(body, aborts, 10)) {
            assertThat(in.readAllBytes()).hasSize(10);
        }
        assertThat(aborts).hasValue(0);
        assertThat(body.closes).isEqualTo(1);
    }

    @Test
    void rangeClosedWithBytesLeftIsAbortedNotDrained() throws Exception {
        Body body = new Body(10);
        var aborts = new java.util.concurrent.atomic.AtomicInteger();
        try (var in = range(body, aborts, 10)) {
            assertThat(in.read(new byte[4], 0, 4)).isEqualTo(4);
        }
        assertThat(aborts).hasValue(1);
        assertThat(body.available()).as("not drained").isEqualTo(6);
        assertThat(body.closes).isEqualTo(1);
    }

    @Test
    void rangeFailingMidReadIsAborted() throws Exception {
        Body body = new Body(10);
        body.failWith = software.amazon.awssdk.core.exception.SdkClientException.create("connection reset");
        var aborts = new java.util.concurrent.atomic.AtomicInteger();
        var in = range(body, aborts, 10);
        assertThatThrownBy(() -> in.read(new byte[4], 0, 4))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("key 'k'");
        assertThat(aborts).hasValue(1);
        in.close();
        assertThat(aborts).as("aborted once").hasValue(1);
        assertThat(body.closes).isEqualTo(1);
    }
}
