package com.pharos.cluster.objectstore;

import com.pharos.config.ClusterConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** {@link ObjectStore} backed by an S3-compatible service (AWS S3, MinIO, ...). Keys are used as given. */
public class S3ObjectStore implements ObjectStore {

    private final S3Client s3;
    private final String bucket;
    /** For messages: the configured endpoint, or the AWS default. */
    private final String endpoint;

    public S3ObjectStore(ClusterConfig.ObjectStoreConfig cfg) {
        this(cfg, credentialsFor(cfg, System::getenv));
    }

    /** Visible for tests that supply credentials directly. */
    S3ObjectStore(ClusterConfig.ObjectStoreConfig cfg, AwsCredentialsProvider credentials) {
        this.bucket = cfg.getBucket();
        this.endpoint = describeEndpoint(cfg);
        try {
            this.s3 = buildClient(cfg, credentials);
        } catch (RuntimeException e) {
            throw new IllegalStateException("cannot create the S3 client for bucket '" + bucket + "' at "
                    + endpoint + ": " + e.getMessage(), e);
        }
    }

    static String describeEndpoint(ClusterConfig.ObjectStoreConfig cfg) {
        return cfg.getEndpoint() == null || cfg.getEndpoint().isBlank()
                ? "the default AWS endpoint (region " + cfg.getRegion() + ")"
                : "endpoint '" + cfg.getEndpoint() + "'";
    }

    private static S3Client buildClient(ClusterConfig.ObjectStoreConfig cfg, AwsCredentialsProvider credentials) {
        var builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .region(Region.of(cfg.getRegion()))
                .credentialsProvider(credentials)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(cfg.isPathStyle()).build());
        if (cfg.getEndpoint() != null && !cfg.getEndpoint().isBlank()) {
            builder.endpointOverride(URI.create(cfg.getEndpoint()));
        }
        return builder.build();
    }

    /**
     * Static credentials from the variables {@code accessKeyEnv}/{@code secretKeyEnv} name; the
     * default AWS chain only when neither is configured. A named variable that is unset or empty
     * fails fast instead of silently falling back to other credentials.
     */
    static AwsCredentialsProvider credentialsFor(ClusterConfig.ObjectStoreConfig cfg,
                                                 java.util.function.Function<String, String> env) {
        String akVar = blankToNull(cfg.getAccessKeyEnv());
        String skVar = blankToNull(cfg.getSecretKeyEnv());
        if (akVar == null && skVar == null) return DefaultCredentialsProvider.builder().build();
        if (akVar == null || skVar == null) {
            throw new IllegalStateException("cluster.objectStore: set both accessKeyEnv and secretKeyEnv, or neither"
                    + " (got only " + (akVar != null ? "accessKeyEnv" : "secretKeyEnv") + ")");
        }
        String ak = env.apply(akVar);
        String sk = env.apply(skVar);
        for (String[] v : new String[][]{{akVar, ak}, {skVar, sk}}) {
            if (v[1] == null || v[1].isEmpty()) {
                throw new IllegalStateException("environment variable " + v[0] + " (cluster.objectStore."
                        + (v[0].equals(akVar) ? "accessKeyEnv" : "secretKeyEnv") + ") is not set or empty");
            }
        }
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk));
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    @Override
    public Optional<StoredObject> get(String key) throws IOException {
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(getReq(key))) {
            byte[] data = in.readAllBytes();
            return Optional.of(new StoredObject(data, in.response().eTag()));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (isMissingKey(e)) return Optional.empty();
            throw wrap("get", key, e);
        } catch (SdkException e) {
            throw wrap("get", key, e);
        }
    }

    @Override
    public void download(String key, Path target) throws IOException {
        Path abs = target.toAbsolutePath();
        Path dir = abs.getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, abs.getFileName().toString(), ".part");
        try {
            try (InputStream in = s3.getObject(getReq(key))) {
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING);
        } catch (NoSuchKeyException e) {
            throw missing(key);
        } catch (S3Exception e) {
            if (isMissingKey(e)) throw missing(key);
            throw wrap("download", key, e);
        } catch (SdkException e) {
            throw wrap("download", key, e);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public void downloadRange(String key, long offset, long length, Path target) throws IOException {
        if (offset < 0 || length < 0) throw new IllegalArgumentException("negative offset or length");
        Path abs = target.toAbsolutePath();
        Path dir = abs.getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, abs.getFileName().toString(), ".part");
        try {
            if (length > 0) {
                GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key)
                        .range("bytes=" + offset + "-" + (offset + length - 1)).build();
                try (ResponseInputStream<GetObjectResponse> in = s3.getObject(req)) {
                    long copied = Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                    if (copied != length) {
                        Long total = totalSize(in.response().contentRange());
                        if (total != null && offset + length > total) {
                            throw new EOFException("range " + offset + "+" + length + " exceeds size " + total
                                    + " of " + key);
                        }
                        throw new IOException("short range body for '" + key + "': expected " + length
                                + " bytes, got " + copied);
                    }
                }
            }
            Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING);
        } catch (NoSuchKeyException e) {
            throw missing(key);
        } catch (S3Exception e) {
            if (isMissingKey(e)) throw missing(key);
            if (e.statusCode() == 416) {
                throw new EOFException("range " + offset + "+" + length + " is past the end of " + key);
            }
            throw wrap("download range", key, e);
        } catch (SdkException e) {
            throw wrap("download range", key, e);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** Total object size from a {@code Content-Range: bytes a-b/total} value; null when unknown. */
    private static Long totalSize(String contentRange) {
        if (contentRange == null) return null;
        int slash = contentRange.lastIndexOf('/');
        if (slash < 0) return null;
        try {
            return Long.parseLong(contentRange.substring(slash + 1).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String put(String key, Path source) throws IOException {
        try {
            return s3.putObject(putReq(key).build(), RequestBody.fromFile(source)).eTag();
        } catch (SdkException e) {
            throw wrap("put", key, e);
        }
    }

    @Override
    public String put(String key, byte[] data) throws IOException {
        try {
            return s3.putObject(putReq(key).build(), RequestBody.fromBytes(data)).eTag();
        } catch (SdkException e) {
            throw wrap("put", key, e);
        }
    }

    @Override
    public Optional<String> putIfAbsent(String key, byte[] data) throws IOException {
        return conditionalPut(key, data, putReq(key).ifNoneMatch("*").build(), false);
    }

    @Override
    public Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException {
        Objects.requireNonNull(etag, "etag");
        // AWS answers If-Match on a nonexistent key with 404 NoSuchKey: treat as "etag changed".
        return conditionalPut(key, data, putReq(key).ifMatch(etag).build(), true);
    }

    private Optional<String> conditionalPut(String key, byte[] data, PutObjectRequest req,
                                          boolean missingIsEmpty) throws IOException {
        try {
            return Optional.ofNullable(s3.putObject(req, RequestBody.fromBytes(data)).eTag());
        } catch (S3Exception e) {
            // 412 PreconditionFailed; 409 ConditionalRequestConflict (concurrent conditional writers)
            if (e.statusCode() == 412 || e.statusCode() == 409) return Optional.empty();
            if (missingIsEmpty && isMissingKey(e)) return Optional.empty();
            throw wrap("conditional put", key, e);
        } catch (SdkException e) {
            throw wrap("conditional put", key, e);
        }
    }

    @Override
    public List<ObjectInfo> list(String prefix) throws IOException {
        List<ObjectInfo> out = new ArrayList<>();
        try {
            String token = null;
            do {
                ListObjectsV2Response resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket).prefix(prefix).continuationToken(token).build());
                for (S3Object o : resp.contents()) {
                    out.add(new ObjectInfo(o.key(), o.size() == null ? 0 : o.size(), o.lastModified()));
                }
                token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
            } while (token != null);
        } catch (SdkException e) {
            throw wrap("list", prefix, e);
        }
        return out;
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            s3.deleteObject(b -> b.bucket(bucket).key(key));
        } catch (S3Exception e) {
            if (isMissingKey(e)) return;
            throw wrap("delete", key, e);
        } catch (SdkException e) {
            throw wrap("delete", key, e);
        }
    }

    @Override
    public void close() {
        s3.close();
    }

    private GetObjectRequest getReq(String key) {
        return GetObjectRequest.builder().bucket(bucket).key(key).build();
    }

    private PutObjectRequest.Builder putReq(String key) {
        return PutObjectRequest.builder().bucket(bucket).key(key);
    }

    private static NoSuchFileException missing(String key) {
        return new NoSuchFileException(key, null, "object not found: " + key);
    }

    /** True only for a missing object (NoSuchKey); a 404 for NoSuchBucket etc. is a real error. */
    static boolean isMissingKey(S3Exception e) {
        if (e instanceof NoSuchKeyException) return true;
        if (e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null) {
            return "NoSuchKey".equals(e.awsErrorDetails().errorCode());
        }
        return false;
    }

    private IOException wrap(String op, String key, SdkException e) {
        return new IOException("S3 " + op + " failed for key '" + key + "' in bucket '" + bucket + "' at "
                + endpoint + ": " + e.getMessage(), e);
    }
}
