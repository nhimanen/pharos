package com.pharos.cluster.objectstore;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.assertj.core.api.Assertions.assertThat;

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
}
