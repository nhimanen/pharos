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
}
