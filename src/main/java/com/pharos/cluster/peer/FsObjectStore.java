package com.pharos.cluster.peer;

import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectMissingException;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.StoredObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * An {@link ObjectStore} on a local directory: the writer side of the peer-pull backend. A writer
 * "publishes" into it exactly as into a bucket ({@link com.pharos.cluster.objectstore.ObjectStorePublisher},
 * the same layout, the same {@link com.pharos.cluster.objectstore.GenerationGc} retention), and the
 * replication endpoint serves readers from it.
 *
 * <p>Key {@code a/b/c} is the file {@code <root>/a/b/c}. Every segment must be a plain name (letters,
 * digits, {@code . _ - @ = +}, not starting with {@code .}), so no key escapes the root and the
 * reserved {@code .tmp/} and {@code .lock} entries are never keys.
 *
 * <p>Writes go to {@code .tmp/} and are moved into place atomically, so a reader never sees a
 * partial object. Conditional writes ({@link #putIfAbsent}, {@link #putIfMatch}) and deletes run
 * under one lock: a {@link ReentrantLock} in this process plus an exclusive {@link FileLock} on
 * {@code <root>/.lock}, so two processes (a {@code pharos index} run and the {@code pharos web}
 * writer) serialise on the same store. The ETag of an object is the hex SHA-256 of its bytes.
 *
 * <p>{@link #put(String, Path)} hard-links the source when it can (same file system; the source must
 * never be modified in place, which holds for Lucene files and the publisher's temp files) and
 * copies otherwise. The object's modification time is set to now either way, because
 * {@link com.pharos.cluster.objectstore.GenerationGc} keeps young unreferenced objects (an
 * in-flight publish) by that time. On a link this also touches the source's time (one inode), which
 * nothing in pharos reads for index files.
 */
public final class FsObjectStore implements ObjectStore {

    private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9_@=+-][A-Za-z0-9._@=+-]{0,254}");
    static final String TMP_DIR = ".tmp";
    static final String LOCK_FILE = ".lock";

    private final Path root;
    private final Path tmp;
    private final ReentrantLock lock = new ReentrantLock();

    public FsObjectStore(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        this.tmp = this.root.resolve(TMP_DIR);
        Files.createDirectories(tmp);
    }

    public Path root() { return root; }

    /**
     * The file holding {@code key}, which need not exist.
     *
     * @throws IllegalArgumentException when {@code key} is not a valid key of this store
     */
    public Path pathOf(String key) {
        if (key == null || key.isEmpty() || key.startsWith("/") || key.endsWith("/")) {
            throw new IllegalArgumentException("invalid key: " + key);
        }
        for (String seg : key.split("/", -1)) {
            if (!SEGMENT.matcher(seg).matches()) throw new IllegalArgumentException("invalid key: " + key);
        }
        Path p = root.resolve(key).normalize();
        if (!p.startsWith(root) || p.equals(root)) throw new IllegalArgumentException("invalid key: " + key);
        return p;
    }

    /** True when {@code key} is well-formed for this store. */
    public static boolean validKey(String key) {
        if (key == null || key.isEmpty() || key.startsWith("/") || key.endsWith("/")) return false;
        for (String seg : key.split("/", -1)) {
            if (!SEGMENT.matcher(seg).matches()) return false;
        }
        return true;
    }

    @Override
    public Optional<StoredObject> get(String key) throws IOException {
        Path p = pathOf(key);
        byte[] data;
        try {
            data = Files.readAllBytes(p);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
        return Optional.of(new StoredObject(data, etagOf(data)));
    }

    @Override
    public void download(String key, Path target) throws IOException {
        Path p = pathOf(key);
        Path abs = target.toAbsolutePath();
        Files.createDirectories(abs.getParent());
        Path part = Files.createTempFile(abs.getParent(), abs.getFileName().toString(), ".part");
        try {
            try {
                Files.copy(p, part, StandardCopyOption.REPLACE_EXISTING);
            } catch (NoSuchFileException e) {
                throw new ObjectMissingException(key);
            }
            Files.move(part, abs, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(part);
        }
    }

    @Override
    public void downloadRange(String key, long offset, long length, Path target) throws IOException {
        Path abs = target.toAbsolutePath();
        Files.createDirectories(abs.getParent());
        Path part = Files.createTempFile(abs.getParent(), abs.getFileName().toString(), ".part");
        try {
            try (InputStream in = openRange(key, offset, length);
                 OutputStream out = Files.newOutputStream(part)) {
                if (in.transferTo(out) != length) throw new EOFException("short read of " + key);
            }
            Files.move(part, abs, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(part);
        }
    }

    @Override
    public InputStream openRange(String key, long offset, long length) throws IOException {
        long end = ObjectStore.checkRange(offset, length);
        FileChannel ch;
        try {
            ch = FileChannel.open(pathOf(key), StandardOpenOption.READ);
        } catch (NoSuchFileException e) {
            throw new ObjectMissingException(key);
        }
        try {
            long size = ch.size();
            if (end > size) throw new EOFException("range " + offset + "+" + length + " exceeds size " + size);
            ch.position(offset);
            return new BoundedInputStream(Channels.newInputStream(ch), length);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    @Override
    public String put(String key, Path source) throws IOException {
        Path target = pathOf(key);
        String etag;
        try (InputStream in = Files.newInputStream(source)) {
            etag = etagOf(in);
        }
        Path staged = tmp.resolve("put-" + java.util.UUID.randomUUID());
        try {
            try {
                Files.createLink(staged, source);
            } catch (IOException | UnsupportedOperationException e) { // another file system, or no links
                Files.copy(source, staged);
            }
            Files.setLastModifiedTime(staged, FileTime.from(Instant.now()));
            withLock(() -> {
                moveIntoPlace(staged, target);
                return null;
            });
        } finally {
            Files.deleteIfExists(staged);
        }
        return etag;
    }

    @Override
    public String put(String key, byte[] data) throws IOException {
        Path target = pathOf(key);
        Path staged = stage(data);
        try {
            withLock(() -> {
                moveIntoPlace(staged, target);
                return null;
            });
        } finally {
            Files.deleteIfExists(staged);
        }
        return etagOf(data);
    }

    @Override
    public Optional<String> putIfAbsent(String key, byte[] data) throws IOException {
        Path target = pathOf(key);
        Path staged = stage(data);
        try {
            return withLock(() -> {
                if (Files.exists(target)) return Optional.<String>empty();
                moveIntoPlace(staged, target);
                return Optional.of(etagOf(data));
            });
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    @Override
    public Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException {
        java.util.Objects.requireNonNull(etag, "etag");
        Path target = pathOf(key);
        Path staged = stage(data);
        try {
            return withLock(() -> {
                byte[] current;
                try {
                    current = Files.readAllBytes(target);
                } catch (NoSuchFileException e) {
                    return Optional.<String>empty();
                }
                if (!etagOf(current).equals(etag)) return Optional.<String>empty();
                moveIntoPlace(staged, target);
                return Optional.of(etagOf(data));
            });
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    @Override
    public List<ObjectInfo> list(String prefix) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                return listOnce(prefix);
            } catch (NoSuchFileException e) { // a directory vanished mid-walk (a concurrent delete)
                if (attempt == 3) throw e;
            }
        }
    }

    private List<ObjectInfo> listOnce(String prefix) throws IOException {
        String p = prefix == null ? "" : prefix;
        int slash = p.lastIndexOf('/');
        Path dir = slash < 0 ? root : root.resolve(p.substring(0, slash)).normalize();
        if (!dir.startsWith(root) || !Files.isDirectory(dir)) return List.of();
        List<ObjectInfo> out = new ArrayList<>();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path f : (Iterable<Path>) s::iterator) {
                String key = root.relativize(f).toString().replace(java.io.File.separatorChar, '/');
                if (!key.startsWith(p) || !validKey(key)) continue; // skips .tmp/ and .lock
                BasicFileAttributes a;
                try {
                    a = Files.readAttributes(f, BasicFileAttributes.class);
                } catch (NoSuchFileException e) {
                    continue; // deleted while listing
                }
                if (!a.isRegularFile()) continue;
                out.add(new ObjectInfo(key, a.size(), a.lastModifiedTime().toInstant()));
            }
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
        out.sort(Comparator.comparing(ObjectInfo::key));
        return out;
    }

    @Override
    public Optional<ObjectInfo> stat(String key) throws IOException {
        try {
            BasicFileAttributes a = Files.readAttributes(pathOf(key), BasicFileAttributes.class);
            if (!a.isRegularFile()) return Optional.empty();
            return Optional.of(new ObjectInfo(key, a.size(), a.lastModifiedTime().toInstant()));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        Path target = pathOf(key);
        withLock(() -> Files.deleteIfExists(target));
    }

    @Override
    public void close() {}

    private Path stage(byte[] data) throws IOException {
        Path staged = tmp.resolve("put-" + java.util.UUID.randomUUID());
        Files.write(staged, data, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        return staged;
    }

    private static void moveIntoPlace(Path staged, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try {
            Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) { // root and .tmp are one directory tree: not expected
            Files.move(staged, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (FileAlreadyExistsException e) {
            Files.move(staged, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @FunctionalInterface
    private interface Locked<T> { T run() throws IOException; }

    private <T> T withLock(Locked<T> body) throws IOException {
        lock.lock();
        try (FileChannel ch = FileChannel.open(root.resolve(LOCK_FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = ch.lock()) {
            return body.run();
        } finally {
            lock.unlock();
        }
    }

    static String etagOf(byte[] data) {
        return HexFormat.of().formatHex(sha256().digest(data));
    }

    private static String etagOf(InputStream in) throws IOException {
        MessageDigest md = sha256();
        try (DigestInputStream d = new DigestInputStream(in, md)) {
            d.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(md.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Reads at most {@code remaining} bytes, then reports end of stream; closes the channel. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        BoundedInputStream(InputStream in, long remaining) {
            this.in = in;
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = in.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int n = in.read(buf, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
