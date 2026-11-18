package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import org.apache.lucene.codecs.CodecUtil;
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
 * <p><b>Edges.</b> Opening a Lucene reader reads every file's header and its 16-byte footer. When the
 * directory has a head sidecar for the file (its first bytes, downloaded with the generation), reads
 * inside it are served from memory; with footer synthesis on, reads inside the last 16 bytes are
 * served from a footer built from the manifest's checksum ({@link CodecUtil#writeFooter}'s layout).
 * Neither touches the block cache. A block holding the real footer is compared with the synthesised
 * one whenever it is acquired: on a mismatch the block is dropped from the cache and the read fails
 * naming the object key (a corrupt object, or a corrupt cached block, which the next read refetches).
 *
 * <p>All multi-byte reads are little-endian, as everywhere in Lucene. A read past the end of the
 * input (or slice) throws {@link EOFException}. A failed fetch is rethrown as an {@link IOException}
 * naming the shard, generation, file and object key.
 */
public final class CachingRemoteIndexInput extends IndexInput implements RandomAccessInput {

    /** Reads of at least this many bytes bypass the buffer and copy straight out of the blocks. */
    static final int BUFFER_SIZE = 8192;
    /**
     * A new input's (or clone's) first buffer. Lucene clones inputs for every term and postings
     * lookup and most clones read a few bytes: allocating a full buffer each time made the lazy
     * reader allocate ~3x a full copy's bytes per query and run old-generation GCs.
     */
    static final int MIN_BUFFER = 1024;
    /**
     * The buffer doubles while an input reads sequentially (each refill continues where the last one
     * ended), up to this size: fewer leases per byte for long scans, never more than a block.
     */
    static final int MAX_BUFFER = 64 * 1024;

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
    /**
     * How many bytes the next refill copies: {@link #MIN_BUFFER}, doubled per sequential refill up
     * to {@link #MAX_BUFFER}, back to {@link #MIN_BUFFER} on a random one. The array only grows.
     */
    private int window;

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

    /** Bulk little-endian floats, copied from the buffer a run at a time (vector reads). */
    @Override
    public void readFloats(float[] dst, int off, int len) throws IOException {
        readElements(dst, off, len, LE_FLOAT);
    }

    @Override
    public void readInts(int[] dst, int off, int len) throws IOException {
        readElements(dst, off, len, LE_INT);
    }

    @Override
    public void readLongs(long[] dst, int off, int len) throws IOException {
        readElements(dst, off, len, LE_LONG);
    }

    private static final ValueLayout.OfFloat LE_FLOAT =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt LE_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    /**
     * Reads {@code len} elements of {@code layout} at the file pointer into {@code dst} (a primitive
     * array): whole runs straight out of the buffer, an element straddling a block boundary through
     * {@link #spanning}. The range is checked first, so a read past the end moves nothing.
     */
    private void readElements(Object dst, int off, int len, ValueLayout layout) throws IOException {
        Objects.checkFromIndexSize(off, len, java.lang.reflect.Array.getLength(dst));
        int size = (int) layout.byteSize();
        checkRange(pos, (long) len * size);
        while (len > 0) {
            long abs = offset + pos;
            if (!buffered(abs, size) && !fillHolds(abs, size)) {
                MemorySegment one = MemorySegment.ofArray(spanning(abs, size));
                MemorySegment.copy(one, layout, 0, dst, off, 1);
                pos += size;
                off++;
                len--;
                continue;
            }
            int idx = (int) (abs - bufStart);
            int n = Math.min(len, (bufLen - idx) / size);
            MemorySegment.copy(MemorySegment.ofArray(buf), layout, idx, dst, off, n);
            pos += (long) n * size;
            off += n;
            len -= n;
        }
    }

    @Override
    public CachingRemoteIndexInput clone() {
        CachingRemoteIndexInput c = (CachingRemoteIndexInput) super.clone();
        c.buf = null; // the clone fills its own buffer: two inputs never write into one array
        c.bufStart = -1;
        c.bufLen = 0;
        c.window = 0;
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
            } else if (len >= Math.min(BUFFER_SIZE, Math.max(window, MIN_BUFFER))) {
                // at least what a refill would copy: straight from the blocks (one lease per block
                // touched), not refill after refill through the buffer
                file.copy(abs, b, off, len);
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

    /**
     * Copies up to {@link #window} bytes at {@code abs}, never past the end of abs's block. The window
     * starts at {@link #MIN_BUFFER}, doubles (to {@link #MAX_BUFFER}) while refills are sequential (each
     * continues where the last one ended) and drops back to {@link #MIN_BUFFER} on a random refill, so
     * a seek after a long scan copies a small window, not 64 KiB.
     */
    private void fill(long abs) throws IOException {
        boolean sequential = bufStart >= 0 && abs == bufStart + bufLen;
        window = window == 0 || !sequential ? MIN_BUFFER : Math.min(MAX_BUFFER, window * 2);
        if (buf == null || buf.length < window) buf = new byte[window];
        bufStart = -1; // stays invalid if the copy fails
        bufLen = file.copyWithinBlock(abs, buf, window);
        bufStart = abs;
    }

    /** Bytes the last buffer refill copied (tests). */
    int lastFillLength() {
        return bufLen;
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
        /** The file's first {@code head.length} bytes (a sidecar); empty without one. */
        final byte[] head;
        /** The file's last 16 bytes, built from the manifest; null without footer synthesis. */
        final byte[] footer;
        final long footerStart;
        /** {@link BlockCache#blockId} per block, built on first use: no string building per read. */
        private final String[] blockIds;
        /** Told when a block's object is missing from the bucket; null for nobody. */
        private final RemoteDirectory.MissingObjectListener missing;

        RemoteFile(ShardId shard, long generation, ManifestFile file, BlockCache cache, ReplicaSource source) {
            this(shard, generation, file, cache, source, null, false);
        }

        RemoteFile(ShardId shard, long generation, ManifestFile file, BlockCache cache, ReplicaSource source,
                   byte[] head, boolean synthFooter) {
            this(shard, generation, file, cache, source, head, synthFooter, null);
        }

        /**
         * @param head        the file's first bytes, or null
         * @param synthFooter serve the last 16 bytes from {@link #footerOf} (a Lucene file whose manifest
         *                    checksum is its footer checksum)
         */
        RemoteFile(ShardId shard, long generation, ManifestFile file, BlockCache cache, ReplicaSource source,
                   byte[] head, boolean synthFooter, RemoteDirectory.MissingObjectListener missing) {
            this.missing = missing;
            this.shard = Objects.requireNonNull(shard, "shard");
            this.generation = generation;
            this.file = Objects.requireNonNull(file, "file");
            this.cache = Objects.requireNonNull(cache, "cache");
            this.source = Objects.requireNonNull(source, "source");
            this.blockSize = cache.blockSize();
            this.length = file.length();
            this.objectHash = BlockCache.objectHash(file.key());
            this.head = head == null || head.length > length ? new byte[0] : head;
            this.footer = synthFooter ? footerOf(file) : null;
            this.footerStart = length - FOOTER;
            long blocks = (length + blockSize - 1) / blockSize;
            this.blockIds = blocks <= MAX_CACHED_IDS ? new String[(int) blocks] : null;
        }

        /** Files with more blocks than this build their block ids per read instead of caching them. */
        private static final int MAX_CACHED_IDS = 1 << 16;

        private String blockId(long index) {
            if (blockIds == null) return BlockCache.blockId(objectHash, index);
            String id = blockIds[(int) index];
            if (id == null) {
                id = BlockCache.blockId(objectHash, index); // racy init is fine: equal strings
                blockIds[(int) index] = id;
            }
            return id;
        }

        static final int FOOTER = 16;

        /**
         * The footer {@link CodecUtil#writeFooter} wrote: big-endian {@code FOOTER_MAGIC}, algorithm id
         * 0, and the checksum; null when the file is too short or the checksum is not a CRC32 value.
         */
        static byte[] footerOf(ManifestFile f) {
            if (f.length() < FOOTER || f.checksum() == null) return null;
            long crc;
            try {
                crc = Long.parseUnsignedLong(f.checksum(), 16);
            } catch (NumberFormatException e) {
                return null;
            }
            if ((crc & 0xFFFFFFFF00000000L) != 0) return null;
            return ByteBuffer.allocate(FOOTER).putInt(CodecUtil.FOOTER_MAGIC).putInt(0).putLong(crc).array();
        }

        /** Copies from {@code abs} out of the head or the footer when it lies there; -1 otherwise. */
        private int copyEdge(long abs, byte[] dst, int off, int len) {
            if (abs < head.length) {
                int n = (int) Math.min(len, head.length - abs);
                System.arraycopy(head, (int) abs, dst, off, n);
                return n;
            }
            if (footer != null && abs >= footerStart) {
                int n = (int) Math.min(len, length - abs);
                System.arraycopy(footer, (int) (abs - footerStart), dst, off, n);
                return n;
            }
            return -1;
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
                int edge = copyEdge(abs, dst, off, len);
                if (edge > 0) {
                    abs += edge;
                    off += edge;
                    len -= edge;
                    continue;
                }
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

        /** Copies from {@code abs} up to the end of its block or {@code max} bytes; returns the count. */
        int copyWithinBlock(long abs, byte[] dst, int max) throws IOException {
            int edge = copyEdge(abs, dst, 0, max);
            if (edge > 0) return edge;
            long index = abs / blockSize;
            long inBlock = abs - index * blockSize;
            try (BlockCache.Lease lease = acquire(index)) {
                int n = (int) Math.min(max, lease.length() - inBlock);
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

        /** The keys of all the file's blocks, in order. */
        java.util.List<BlockKey> blockKeys() {
            long blocks = (length + blockSize - 1) / blockSize;
            java.util.List<BlockKey> out = new java.util.ArrayList<>();
            for (long i = 0; i < blocks; i++) out.add(new BlockKey(file.key(), i));
            return out;
        }

        /** Index of the block holding byte {@code abs}. */
        long blockOf(long abs) {
            return abs / blockSize;
        }

        /** Length of block {@code index} (the last one may be short). */
        long lengthOfBlock(long index) {
            return blockLength(index);
        }

        /** Starts prefetching one block; see {@link BlockCache#prefetch}. */
        java.util.concurrent.CompletableFuture<Void> prefetchBlock(long index) {
            return cache.prefetch(new BlockKey(file.key(), index), objectHash, blockLength(index), this);
        }

        /** The key of block {@code index}. */
        BlockKey blockKey(long index) {
            return new BlockKey(file.key(), index);
        }

        /** Starts prefetching every block of the file; one future per block (see {@link BlockCache#prefetch}). */
        java.util.List<java.util.concurrent.CompletableFuture<Void>> prefetchAll() {
            long blocks = (length + blockSize - 1) / blockSize;
            java.util.List<java.util.concurrent.CompletableFuture<Void>> out = new java.util.ArrayList<>();
            for (long i = 0; i < blocks; i++) {
                out.add(cache.prefetch(new BlockKey(file.key(), i), objectHash, blockLength(i), this));
            }
            return out;
        }

        private long blockLength(long index) {
            return Math.min(blockSize, length - index * blockSize);
        }

        private BlockCache.Lease acquire(long index) throws IOException {
            BlockKey key = new BlockKey(file.key(), index);
            BlockCache.Lease lease;
            try {
                lease = cache.acquire(key, objectHash, blockId(index), blockLength(index), this);
            } catch (InterruptedIOException e) {
                InterruptedIOException x = new InterruptedIOException("interrupted reading block " + index + " of "
                        + where());
                x.initCause(e);
                throw x;
            } catch (IOException | RuntimeException e) {
                if (missing != null && objectMissing(e)) {
                    try {
                        missing.objectMissing(shard, generation, file.key());
                    } catch (RuntimeException ignored) {
                        // a listener must not hide the read's own failure
                    }
                }
                throw new IOException("cannot read block " + index + " of " + where() + ": " + e.getMessage(), e);
            }
            if (footer != null) checkFooter(key, index, lease);
            return lease;
        }

        /** True when {@code e} (or a cause) says the object is not in the store. */
        static boolean objectMissing(Throwable e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof java.nio.file.NoSuchFileException) return true;
                if (t.getCause() == t) break;
            }
            return false;
        }

        /** A block holding the whole footer must end with the synthesised one. */
        private void checkFooter(BlockKey key, long index, BlockCache.Lease lease) throws IOException {
            long inBlock = footerStart - index * blockSize;
            if (inBlock < 0 || inBlock + FOOTER > lease.length()) return;
            if (MemorySegment.mismatch(lease.segment(), inBlock, inBlock + FOOTER,
                    MemorySegment.ofArray(footer), 0, FOOTER) < 0) {
                return;
            }
            cache.invalidate(key, objectHash);
            lease.close();
            throw new IOException("block " + index + " of " + where() + " does not end with the footer that the"
                    + " manifest's checksum " + file.checksum() + " implies (corrupt object or cached block);"
                    + " dropped it from the cache");
        }
    }
}
