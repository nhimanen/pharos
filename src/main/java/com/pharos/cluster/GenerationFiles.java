package com.pharos.cluster;

import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Integrity checks for the files of a published generation once they are on local disk, shared by
 * the reader ({@link ReplicaSyncer}) and a writer restoring a shard
 * ({@link com.pharos.cluster.objectstore.ObjectStoreWriterStateStore}).
 */
public final class GenerationFiles {

    private GenerationFiles() {}

    /** A plain file name: no separators, no {@code .}/{@code ..}, nothing absolute. */
    public static final Pattern PLAIN_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,254}");

    /** The bytes on disk (or the manifest describing them) cannot be trusted; retrying will not help. */
    public static final class IntegrityException extends IOException {
        public IntegrityException(String message) { super(message); }
        public IntegrityException(String message, Throwable cause) { super(message, cause); }
    }

    /** Every entry needs a role and a plain file name; at least one Lucene file, at most one graph. */
    public static void checkFiles(ShardManifest manifest) throws IntegrityException {
        Set<String> luceneNames = new HashSet<>();
        int graphs = 0;
        for (ManifestFile f : manifest.files()) {
            if (f.role() == null) throw new IntegrityException("manifest gen-" + manifest.generation() + " has no role in " + f);
            if (f.name() == null || !PLAIN_NAME.matcher(f.name()).matches()) {
                throw new IntegrityException("manifest gen-" + manifest.generation() + " has an unsafe file name: " + f);
            }
            if (f.length() < 0) throw new IntegrityException("negative length in " + f);
            if (ManifestFile.LUCENE.equals(f.role()) && !luceneNames.add(f.name())) {
                throw new IntegrityException("duplicate Lucene file " + f.name());
            }
            if (ManifestFile.GRAPH.equals(f.role())) graphs++;
        }
        if (luceneNames.isEmpty()) throw new IntegrityException("manifest gen-" + manifest.generation() + " has no Lucene files");
        if (graphs > 1) throw new IntegrityException("manifest gen-" + manifest.generation() + " lists " + graphs + " graphs");
    }

    public static void checkLength(Path file, ManifestFile f) throws IOException {
        long actual = Files.size(file);
        if (actual != f.length()) {
            throw new IntegrityException("integrity check failed for " + f.key() + ": " + actual
                    + " bytes, manifest says " + f.length());
        }
    }

    /** The CRC32 of the whole file must be the manifest's checksum (graph and refs files). */
    public static void checkCrc32(Path file, ManifestFile f) throws IOException {
        CRC32 crc = new CRC32();
        try (InputStream in = new CheckedInputStream(Files.newInputStream(file), crc)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        if (crc.getValue() != parseChecksum(f)) {
            throw new IntegrityException("integrity check failed for " + f.key() + ": CRC32 "
                    + Long.toHexString(crc.getValue()) + ", manifest says " + f.checksum());
        }
    }

    /**
     * Downloaded Lucene files are checksummed end to end ({@link CodecUtil#checksumEntireFile}
     * throws when the body does not match the footer); others (hard-linked from a verified
     * generation) have their footer read. Either checksum must match the manifest, and the commit
     * must be readable. Only the files in {@code local} are checked.
     */
    public static void checkLuceneChecksums(Path lucene, ShardManifest manifest, Set<String> local,
                                            Set<String> downloaded) throws IOException {
        try (Directory dir = FSDirectory.open(lucene)) {
            for (ManifestFile f : manifest.files()) {
                if (!ManifestFile.LUCENE.equals(f.role()) || !local.contains(f.name())) continue;
                long actual;
                try (IndexInput in = dir.openInput(f.name(), IOContext.READONCE)) {
                    actual = downloaded.contains(f.name())
                            ? CodecUtil.checksumEntireFile(in)
                            : CodecUtil.retrieveChecksum(in);
                } catch (org.apache.lucene.index.CorruptIndexException e) {
                    throw new IntegrityException("integrity check failed for " + f.key() + ": " + e.getMessage(), e);
                }
                if (actual != parseChecksum(f)) {
                    throw new IntegrityException("integrity check failed for " + f.key() + ": footer checksum "
                            + Long.toHexString(actual) + ", manifest says " + f.checksum());
                }
            }
            try {
                SegmentInfos.readLatestCommit(dir);
            } catch (org.apache.lucene.index.CorruptIndexException | org.apache.lucene.index.IndexFormatTooOldException
                     | org.apache.lucene.index.IndexFormatTooNewException
                     | org.apache.lucene.index.IndexNotFoundException e) {
                throw new IntegrityException("unreadable Lucene commit in " + lucene + ": " + e.getMessage(), e);
            }
        }
    }

    public static long parseChecksum(ManifestFile f) throws IntegrityException {
        try {
            return Long.parseUnsignedLong(Objects.requireNonNull(f.checksum(), "checksum"), 16);
        } catch (NumberFormatException | NullPointerException e) {
            throw new IntegrityException("invalid checksum in " + f);
        }
    }
}
