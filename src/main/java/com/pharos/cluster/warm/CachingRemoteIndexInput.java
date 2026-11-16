package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A Lucene {@link IndexInput} over one manifest file whose bytes live in the object store, read
 * through the shared {@link BlockCache} in blocks of {@code cache.blockSize()}: block {@code i}
 * covers {@code [i × blockSize, min((i + 1) × blockSize, length))}.
 *
 * <p>No lease is held between calls: each read acquires the block, copies what it needs into a small
 * per-input buffer (or straight into the caller's array for long reads) and closes the lease, so
 * any block not being copied right now may be evicted. Clones and slices share the file but own
 * their position and buffer; closing one releases nothing the others use.
 *
 * <p>It is deliberately not a {@code MemorySegmentAccessInput}: Lucene asks those for one segment
 * over a whole file (postings decoding, vector scoring), which block reads can't give without
 * copying the file. Lucene then scores vectors through its generic path, which sums in another
 * order than over an {@code MMapDirectory}, so a KNN score can differ from a full local copy's in
 * the last bit (it matches an {@code NIOFSDirectory} exactly).
 *
 * <p>All multi-byte reads are little-endian, as everywhere in Lucene. A read past the end of the
 * input (or slice) throws {@link EOFException}. A failed fetch is rethrown as an {@link IOException}
 * naming the shard, generation, file and object key.
 */
public final class CachingRemoteIndexInput extends IndexInput implements RandomAccessInput {

    /** Bytes copied out of a block per lease; also the threshold above which reads bypass the buffer. */
    static final int BUFFER_SIZE = 8192;

    private final RemoteFile file;
    /** Start of this input within the file. */
    private final long offset;
    private final long length;
    /** Position relative to {@link #offset}. */
    private long pos;
    /** Copy of file bytes {@code [bufStart, bufStart + bufLen)}; never shared between inputs. */
    private byte[] buf;
    private long bufStart = -1;
    private int bufLen;

    CachingRemoteIndexInput(String resourceDescription, RemoteFile file) {
        this(resourceDescription, file, 0, file.length);
    }

    private CachingRemoteIndexInput(String resourceDescription, RemoteFile file, long offset, long length) {
        super(resourceDescription);
        this.file = file;
        this.offset = offset;
        this.length = length;
    }

    // ---------------------------------------------------------------- IndexInput

    @Override
    public long length() {
        return length;
    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public void seek(long p) throws IOException {
        if (p < 0) throw new IllegalArgumentException("negative seek position " + p + ": " + this);
        if (p > length) throw new EOFException("seek past EOF: position " + p + ", length " + length + ": " + this);
        pos = p;
    }

    @Override
    public byte readByte() throws IOException {
        byte b = readByte(pos);
        pos++;
        return b;
    }

    @Override
    public void readBytes(byte[] b, int off, int len) throws IOException {
        readBytes(pos, b, off, len);
        pos += len;
    }

    @Override
    public short readShort() throws IOException {
        short v = readShort(pos);
        pos += Short.BYTES;
        return v;
    }

    @Override
    public int readInt() throws IOException {
        int v = readInt(pos);
        pos += Integer.BYTES;
        return v;
    }

    @Override
    public long readLong() throws IOException {
        long v = readLong(pos);
        pos += Long.BYTES;
        return v;
    }

    @Override
    public CachingRemoteIndexInput clone() {
        CachingRemoteIndexInput c = (CachingRemoteIndexInput) super.clone();
        c.buf = null; // the clone fills its own buffer: two inputs never write into one array
        c.bufStart = -1;
        c.bufLen = 0;
        return c;
    }

    @Override
    public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset > length - sliceLength) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset="
                    + sliceOffset + ",length=" + sliceLength + ",fileLength=" + length + ": " + this);
        }
        return new CachingRemoteIndexInput(getFullSliceDescription(sliceDescription), file, offset + sliceOffset,
                sliceLength);
    }

    /** Starts background fetches of the blocks under {@code [offset, offset + length)}; never waits. */
    @Override
    public void prefetch(long prefetchOffset, long prefetchLength) throws IOException {
        if (prefetchOffset < 0 || prefetchLength < 0 || prefetchOffset > length - prefetchLength) {
            throw new IllegalArgumentException("prefetch out of bounds: offset=" + prefetchOffset + ",length="
                    + prefetchLength + ",fileLength=" + length + ": " + this);
        }
        file.prefetch(offset + prefetchOffset, prefetchLength);
    }

    /** Holds no resources: leases never outlive a read, and the cache belongs to the directory's owner. */
    @Override
    public void close() {
        buf = null;
        bufStart = -1;
        bufLen = 0;
    }

    // ---------------------------------------------------------------- RandomAccessInput

    @Override
    public byte readByte(long p) throws IOException {
        checkRange(p, 1);
        long abs = offset + p;
        if (!buffered(abs, 1)) fill(abs);
        return buf[(int) (abs - bufStart)];
    }

    @Override
    public void readBytes(long p, byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        checkRange(p, len);
        long abs = offset + p;
        while (len > 0) {
            if (buffered(abs, 1)) {
                int idx = (int) (abs - bufStart);
                int n = Math.min(len, bufLen - idx);
                System.arraycopy(buf, idx, b, off, n);
                abs += n;
                off += n;
                len -= n;
            } else if (len >= BUFFER_SIZE) {
                file.copy(abs, b, off, len); // long read: straight from the blocks, no double copy
                return;
            } else {
                fill(abs);
            }
        }
    }

    @Override
    public short readShort(long p) throws IOException {
        checkRange(p, Short.BYTES);
        long abs = offset + p;
        if (buffered(abs, Short.BYTES) || fillHolds(abs, Short.BYTES)) {
            return (short) BitUtil.VH_LE_SHORT.get(buf, (int) (abs - bufStart));
        }
        return (short) BitUtil.VH_LE_SHORT.get(spanning(abs, Short.BYTES), 0);
    }

    @Override
    public int readInt(long p) throws IOException {
        checkRange(p, Integer.BYTES);
        long abs = offset + p;
        if (buffered(abs, Integer.BYTES) || fillHolds(abs, Integer.BYTES)) {
            return (int) BitUtil.VH_LE_INT.get(buf, (int) (abs - bufStart));
        }
        return (int) BitUtil.VH_LE_INT.get(spanning(abs, Integer.BYTES), 0);
    }

    @Override
    public long readLong(long p) throws IOException {
        checkRange(p, Long.BYTES);
        long abs = offset + p;
        if (buffered(abs, Long.BYTES) || fillHolds(abs, Long.BYTES)) {
            return (long) BitUtil.VH_LE_LONG.get(buf, (int) (abs - bufStart));
        }
        return (long) BitUtil.VH_LE_LONG.get(spanning(abs, Long.BYTES), 0);
    }

    // ---------------------------------------------------------------- buffer

    private void checkRange(long p, long n) throws EOFException {
        if (p < 0 || p > length - n) {
            throw new EOFException("read past EOF: position " + p + " + " + n + " bytes, length " + length + ": "
                    + this);
        }
    }

    private boolean buffered(long abs, int n) {
        return bufStart >= 0 && abs >= bufStart && abs + n <= bufStart + bufLen;
    }

    /** Refills the buffer at {@code abs} and reports whether {@code n} bytes from there now fit in it. */
    private boolean fillHolds(long abs, int n) throws IOException {
        fill(abs);
        return buffered(abs, n);
    }

    /** Copies up to {@link #BUFFER_SIZE} bytes at {@code abs}, never past the end of abs's block. */
    private void fill(long abs) throws IOException {
        if (buf == null) buf = new byte[BUFFER_SIZE];
        bufStart = -1; // stays invalid if the copy fails
        bufLen = file.copyWithinBlock(abs, buf);
        bufStart = abs;
    }

    /** The {@code n} bytes at {@code abs}, which straddle a block boundary. */
    private byte[] spanning(long abs, int n) throws IOException {
        byte[] tmp = new byte[n];
        file.copy(abs, tmp, 0, n);
        return tmp;
    }

    // ---------------------------------------------------------------- shared file

    /** One manifest file shared by an input and all its clones and slices; also the block fetcher. */
    static final class RemoteFile implements BlockCache.BlockFetcher {
        private static final int STREAM_CHUNK = 64 * 1024;

        final ShardId shard;
        final long generation;
        final ManifestFile file;
        final BlockCache cache;
        final ReplicaSource source;
        final int blockSize;
        final long length;
        /** {@link BlockCache#objectHash} of the file's key: hashed once, not on every buffer refill. */
        final String objectHash;

        RemoteFile(ShardId shard, long generation, ManifestFile file, BlockCache cache, ReplicaSource source) {
            this.shard = Objects.requireNonNull(shard, "shard");
            this.generation = generation;
            this.file = Objects.requireNonNull(file, "file");
            this.cache = Objects.requireNonNull(cache, "cache");
            this.source = Objects.requireNonNull(source, "source");
            this.blockSize = cache.blockSize();
            this.length = file.length();
            this.objectHash = BlockCache.objectHash(file.key());
        }

        /** "shard S generation N file F (object key K)", for every error this file reports. */
        String where() {
            return "shard " + shard + " generation " + generation + " file " + file.name() + " (object key "
                    + file.key() + ")";
        }

        @Override
        public void fetch(BlockKey key, long offset, long len, Path target) throws IOException {
            source.downloadRange(file, offset, len, target);
        }

        /** Streams the ranged body straight into the cache's memory: no temp file on a full disk. */
        @Override
        public void fetchToMemory(BlockKey key, long offset, long len, ByteBuffer target) throws IOException {
            try (InputStream in = source.openRange(file, offset, len)) {
                byte[] chunk = new byte[(int) Math.min(len, STREAM_CHUNK)];
                long remaining = len;
                while (remaining > 0) {
                    int n = in.read(chunk, 0, (int) Math.min(chunk.length, remaining));
                    if (n < 0) {
                        throw new EOFException("short range body for '" + file.key() + "': expected " + len
                                + " bytes at " + offset + ", got " + (len - remaining));
                    }
                    target.put(chunk, 0, n);
                    remaining -= n;
                }
                if (in.read() >= 0) {
                    throw new IOException("range body for '" + file.key() + "' is longer than the " + len
                            + " bytes requested at " + offset);
                }
            }
        }

        /** Copies {@code len} bytes at {@code abs} into {@code dst}, one short lease per block. */
        void copy(long abs, byte[] dst, int off, int len) throws IOException {
            while (len > 0) {
                long index = abs / blockSize;
                long inBlock = abs - index * blockSize;
                try (BlockCache.Lease lease = acquire(index)) {
                    int n = (int) Math.min(len, lease.length() - inBlock);
                    MemorySegment.copy(lease.segment(), ValueLayout.JAVA_BYTE, inBlock, dst, off, n);
                    abs += n;
                    off += n;
                    len -= n;
                }
            }
        }

        /** Copies from {@code abs} up to the end of its block or of {@code dst}; returns the count. */
        int copyWithinBlock(long abs, byte[] dst) throws IOException {
            long index = abs / blockSize;
            long inBlock = abs - index * blockSize;
            try (BlockCache.Lease lease = acquire(index)) {
                int n = (int) Math.min(dst.length, lease.length() - inBlock);
                MemorySegment.copy(lease.segment(), ValueLayout.JAVA_BYTE, inBlock, dst, 0, n);
                return n;
            }
        }

        void prefetch(long abs, long len) {
            if (len <= 0) return;
            long first = abs / blockSize;
            long last = (abs + len - 1) / blockSize;
            for (long i = first; i <= last; i++) {
                try {
                    cache.prefetch(new BlockKey(file.key(), i), objectHash, blockLength(i), this);
                } catch (RuntimeException e) {
                    return; // advisory: the read that needs the block reports the problem
                }
            }
        }

        private long blockLength(long index) {
            return Math.min(blockSize, length - index * blockSize);
        }

        private BlockCache.Lease acquire(long index) throws IOException {
            try {
                return cache.acquire(new BlockKey(file.key(), index), objectHash, blockLength(index), this);
            } catch (InterruptedIOException e) {
                InterruptedIOException x = new InterruptedIOException("interrupted reading block " + index + " of "
                        + where());
                x.initCause(e);
                throw x;
            } catch (IOException | RuntimeException e) {
                throw new IOException("cannot read block " + index + " of " + where() + ": " + e.getMessage(), e);
            }
        }
    }
}
