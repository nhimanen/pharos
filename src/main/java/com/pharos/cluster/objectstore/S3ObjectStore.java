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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** {@link ObjectStore} backed by an S3-compatible service (AWS S3, MinIO, ...). Keys are used as given. */
public class S3ObjectStore implements ObjectStore {

    private final S3Client s3;
    private final String bucket;

    public S3ObjectStore(ClusterConfig.ObjectStoreConfig cfg) {
        this(cfg, credentialsFor(cfg));
    }

    /** Visible for tests that supply credentials directly. */
    S3ObjectStore(ClusterConfig.ObjectStoreConfig cfg, AwsCredentialsProvider credentials) {
        this.bucket = cfg.getBucket();
        var builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .region(Region.of(cfg.getRegion()))
                .credentialsProvider(credentials)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(cfg.isPathStyle()).build());
        if (cfg.getEndpoint() != null && !cfg.getEndpoint().isBlank()) {
            builder.endpointOverride(URI.create(cfg.getEndpoint()));
        }
        this.s3 = builder.build();
    }

    private static AwsCredentialsProvider credentialsFor(ClusterConfig.ObjectStoreConfig cfg) {
        String ak = cfg.getAccessKeyEnv() == null ? null : System.getenv(cfg.getAccessKeyEnv());
        String sk = cfg.getSecretKeyEnv() == null ? null : System.getenv(cfg.getSecretKeyEnv());
        if (ak != null && !ak.isEmpty() && sk != null && !sk.isEmpty()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk));
        }
        return DefaultCredentialsProvider.builder().build();
    }

    @Override
    public Optional<StoredObject> get(String key) throws IOException {
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(getReq(key))) {
            byte[] data = in.readAllBytes();
            return Optional.of(new StoredObject(data, in.response().eTag()));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return Optional.empty();
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
            if (e.statusCode() == 404) throw missing(key);
            throw wrap("download", key, e);
        } catch (SdkException e) {
            throw wrap("download", key, e);
        } finally {
            Files.deleteIfExists(tmp);
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
        return conditionalPut(key, data, putReq(key).ifNoneMatch("*").build());
    }

    @Override
    public Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException {
        return conditionalPut(key, data, putReq(key).ifMatch(etag).build());
    }

    private Optional<String> conditionalPut(String key, byte[] data, PutObjectRequest req) throws IOException {
        try {
            return Optional.ofNullable(s3.putObject(req, RequestBody.fromBytes(data)).eTag());
        } catch (S3Exception e) {
            // 412 PreconditionFailed; 409 ConditionalRequestConflict (concurrent conditional writers)
            if (e.statusCode() == 412 || e.statusCode() == 409) return Optional.empty();
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
            if (e.statusCode() == 404) return;
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

    private static IOException wrap(String op, String key, SdkException e) {
        return new IOException("S3 " + op + " failed for key '" + key + "': " + e.getMessage(), e);
    }
}
