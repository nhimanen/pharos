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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link ObjectStore} backed by an S3-compatible service (AWS S3, MinIO, ...). Keys are used as given.
 *
 * <p>Timeouts: the HTTP client connects within {@code cluster.objectStore.readTimeoutSeconds} and
 * waits for each read of a response up to that or 30 s, whichever is longer (a whole-object download
 * or an upload on a slow bucket is not cut short); a ranged read ({@link #openRange}, which a reader's
 * block fetches use) also bounds each attempt by that and the whole call, retries included, by
 * {@link ClusterConfig.ObjectStoreConfig#readTotalTimeout}. An unreachable bucket then fails a query's
 * miss in seconds instead of stalling it for the SDK's minutes of socket timeouts and retries.
 */
public class S3ObjectStore implements ObjectStore {

    private final S3Client s3;
    /** Per-request bounds of a ranged read: one attempt, and the whole call with retries. */
    private final java.time.Duration readAttemptTimeout;
    private final java.time.Duration readTotalTimeout;
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
        this.readAttemptTimeout = cfg.readAttemptTimeout();
        this.readTotalTimeout = cfg.readTotalTimeout();
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

    /** The client-wide floor of {@link #clientSocketTimeout}. */
    static final java.time.Duration MIN_CLIENT_SOCKET_TIMEOUT = java.time.Duration.ofSeconds(30);

    /**
     * The HTTP client's socket timeout (the idle time between two reads of any call): at least
     * 30 s ({@link #MIN_CLIENT_SOCKET_TIMEOUT}), so a whole-object download or a writer's upload
     * waiting on a slow bucket is not cut by the tight {@code readTimeoutSeconds}; ranged reads keep
     * that bound through their per-request timeouts ({@link #readTimeouts}).
     */
    static java.time.Duration clientSocketTimeout(ClusterConfig.ObjectStoreConfig cfg) {
        java.time.Duration read = cfg.readAttemptTimeout();
        return read.compareTo(MIN_CLIENT_SOCKET_TIMEOUT) > 0 ? read : MIN_CLIENT_SOCKET_TIMEOUT;
    }

    private static S3Client buildClient(ClusterConfig.ObjectStoreConfig cfg, AwsCredentialsProvider credentials) {
        var builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .connectionTimeout(cfg.readAttemptTimeout())
                        .socketTimeout(clientSocketTimeout(cfg)) // idle time between reads, not a whole body
                        .build())
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
        ObjectStore.checkRange(offset, length);
        Path abs = target.toAbsolutePath();
        Path dir = abs.getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, abs.getFileName().toString(), ".part");
        try {
            try (InputStream in = openRange(key, offset, length)) {
                long copied = Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                if (copied != length) {
                    throw new IOException("short range body for '" + key + "': expected " + length
                            + " bytes, got " + copied);
                }
            }
            Files.move(tmp, abs, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public InputStream openRange(String key, long offset, long length) throws IOException {
        long end = ObjectStore.checkRange(offset, length);
        try {
            if (length == 0) {
                // No ranged GET can express zero bytes; a HEAD gives the same missing-key / past-end answers.
                long size;
                try {
                    size = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key)
                                    .overrideConfiguration(this::readTimeouts).build())
                            .contentLength();
                } catch (S3Exception e) {
                    if (isMissingOnHead(e)) throw missing(key);
                    throw e;
                }
                if (offset > size) {
                    throw new EOFException("range " + offset + "+0 is past the end (size " + size + ") of " + key);
                }
                return InputStream.nullInputStream();
            }
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key)
                    .range("bytes=" + offset + "-" + (end - 1))
                    .overrideConfiguration(this::readTimeouts).build();
            ResponseInputStream<GetObjectResponse> in = s3.getObject(req);
            try {
                String contentRange = in.response().contentRange();
                checkContentRange(contentRange, offset, key);
                Long total = totalSize(contentRange);
                if (total != null && end > total) {
                    throw new EOFException("range " + offset + "+" + length + " exceeds size " + total + " of " + key);
                }
            } catch (IOException | RuntimeException e) {
                in.abort();
                closeQuietly(in);
                throw e;
            }
            return new RangeStream(in, in::abort, length, (op, e) -> wrap(op, key, e));
        } catch (NoSuchKeyException e) {
            throw missing(key);
        } catch (S3Exception e) {
            if (isMissingKey(e)) throw missing(key);
            if (e.statusCode() == 416) {
                throw new EOFException("range " + offset + "+" + length + " is past the end of " + key);
            }
            throw wrap("open range", key, e);
        } catch (SdkException e) {
            throw wrap("open range", key, e);
        }
    }

    /** A ranged read's bounds: {@code readTimeoutSeconds} per attempt, three times that per call. */
    private void readTimeouts(software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration.Builder o) {
        o.apiCallAttemptTimeout(readAttemptTimeout).apiCallTimeout(readTotalTimeout);
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException | RuntimeException ignored) {
            // already failing
        }
    }

    /**
     * A ranged GET body that reports SDK failures while streaming as {@link IOException}s naming the
     * key. It aborts the HTTP connection (never drains it) when a read fails, and when it is closed
     * before all {@code length} bytes were read: draining a large unread rest would stall the closing
     * thread, and a broken connection must not go back to the pool.
     */
    static final class RangeStream extends java.io.FilterInputStream {
        @FunctionalInterface
        interface Wrapper {
            IOException wrap(String op, SdkException e);
        }

        private final Runnable abort;
        private final Wrapper wrapper;
        private long remaining;
        private boolean aborted;
        private boolean closed;

        RangeStream(InputStream in, Runnable abort, long length, Wrapper wrapper) {
            super(in);
            this.abort = abort;
            this.remaining = length;
            this.wrapper = wrapper;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                int n = in.read(b, off, len);
                if (n > 0) remaining -= n;
                return n;
            } catch (SdkException e) {
                abortOnce();
                throw wrapper.wrap("read range", e);
            } catch (IOException | RuntimeException e) {
                abortOnce();
                throw e;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            try {
                long skipped = in.skip(n);
                if (skipped > 0) remaining -= skipped;
                return skipped;
            } catch (SdkException e) {
                abortOnce();
                throw wrapper.wrap("read range", e);
            } catch (IOException | RuntimeException e) {
                abortOnce();
                throw e;
            }
        }

        /** Aborts the connection the first time only (a failed read, or a close with bytes left). */
        private void abortOnce() {
            if (aborted) return;
            aborted = true;
            try {
                abort.run();
            } catch (RuntimeException ignored) {
                // closing the stream releases what is left
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            try {
                if (remaining > 0) abortOnce();
                in.close();
            } catch (SdkException e) {
                throw wrapper.wrap("close range", e);
            }
        }
    }

    /**
     * Rejects a response whose body does not start at {@code offset}: a {@code Content-Range} with
     * another start, or none at all for a non-zero offset (a 200 that ignored the Range header and
     * sent the object from byte 0).
     */
    static void checkContentRange(String contentRange, long offset, String key) throws IOException {
        if (contentRange == null) {
            if (offset == 0) return;
            throw new IOException("server ignored the Range header for '" + key + "' (no Content-Range,"
                    + " expected a body starting at " + offset + ")");
        }
        String v = contentRange.trim();
        int dash = v.indexOf('-');
        long start;
        try {
            if (!v.startsWith("bytes ") || dash < 0) throw new NumberFormatException(v);
            start = Long.parseLong(v.substring("bytes ".length(), dash).trim());
        } catch (NumberFormatException e) {
            throw new IOException("unparseable Content-Range '" + contentRange + "' for '" + key + "'");
        }
        if (start != offset) {
            throw new IOException("Content-Range '" + contentRange + "' for '" + key + "' starts at " + start
                    + ", expected " + offset);
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

    private static ObjectMissingException missing(String key) {
        return new ObjectMissingException(key);
    }

    /**
     * A HEAD response has no body, so S3 can't say NoSuchKey: any 404 counts as a missing object
     * (a missing bucket included — the next non-HEAD call reports that precisely).
     */
    static boolean isMissingOnHead(S3Exception e) {
        return e.statusCode() == 404 || isMissingKey(e);
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
