package com.pharos.cluster.warm;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteDirectoryTest {

    private static final String PROJECT = "sample-app";
    private static final ShardId SHARD = new ShardId(PROJECT);
    private static final int KIB = 1024;

    /** sample-app indexed with fake embeddings and published to an in-memory store, once per class. */
    @TempDir static Path shared;
    private static InMemoryObjectStore store;
    private static ObjectStoreReplicaSource source;
    private static ShardManifest manifest;
    /** A full local copy of the published Lucene files: the FSDirectory reference. */
    private static Path fullCopy;
    /** Only the small files the reader downloads eagerly (segments_N, .si, .fnm). */
    private static Path eagerCopy;

    @TempDir Path tmp;
    private final List<AutoCloseable> open = new ArrayList<>();

    @BeforeAll
    static void indexAndPublish() throws IOException {
        Path src = shared.resolve("src");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = src.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
        store = new InMemoryObjectStore();
        ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ClusterConfig cfg = new ClusterConfig();
        GenerationGc gc = new GenerationGc(store, layout, cfg.getRetentionGenerations(),
                Duration.ofMinutes(cfg.getRetentionMinutes()));
        Path synonyms = shared.resolve("synonyms.txt");
        Files.writeString(synonyms, "auth, authentication\n");
        ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, "writer", synonyms);

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(shared.resolve("writer-indexes"));
        ProjectRegistry registry = ProjectRegistry.at(shared.resolve("writer-registry.json"));
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                return List.of();
            }
        };
        LuceneIndexer lucene = new LuceneIndexer(config);
        try {
            new ProjectIndexManager(config, lucene, registry, List.of(new HashEmbeddings()), noopModules,
                    List.of(new JavaCodeParser()), publisher)
                    .index(src, PROJECT, false, false, true, false, ProgressListener.SILENT); // with embeddings
        } finally {
            lucene.close();
        }

        source = new ObjectStoreReplicaSource(store, layout);
        manifest = source.manifest(SHARD, Generation.FIRST);
        fullCopy = shared.resolve("full");
        eagerCopy = shared.resolve("eager");
        Files.createDirectories(eagerCopy);
        for (ManifestFile f : luceneFiles(manifest)) {
            source.download(f, fullCopy.resolve(f.name()));
            if (f.name().startsWith("segments_") || f.name().endsWith(".si") || f.name().endsWith(".fnm")) {
                Files.copy(fullCopy.resolve(f.name()), eagerCopy.resolve(f.name()));
            }
        }
    }

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    private <T extends AutoCloseable> T track(T c) {
        open.add(c);
        return c;
    }

    private static List<ManifestFile> luceneFiles(ShardManifest m) {
        return m.files().stream().filter(f -> ManifestFile.LUCENE.equals(f.role())).toList();
    }

    private BlockCache cache(int blockSize) throws IOException {
        return track(new BlockCache(tmp.resolve("blocks-" + blockSize), 1L << 30, blockSize, 4));
    }

    private RemoteDirectory remote(BlockCache cache, Path eager, ReplicaSource src, ShardManifest m) {
        return track(new RemoteDirectory(SHARD, m, eager, cache, src));
    }

    // ---------------------------------------------------------------- the published fixture

    @Test
    void listAllAndFileLengthComeFromTheManifest() throws IOException {
        RemoteDirectory dir = remote(cache(64 * KIB), null, source, manifest);
        List<String> names = luceneFiles(manifest).stream().map(ManifestFile::name).sorted().toList();
        assertThat(names).isNotEmpty();
        assertThat(dir.listAll()).containsExactlyElementsOf(names);
        for (ManifestFile f : luceneFiles(manifest)) {
            assertThat(dir.fileLength(f.name())).isEqualTo(f.length());
        }
        assertThatThrownBy(() -> dir.fileLength("_99.cfs")).isInstanceOf(NoSuchFileException.class);
        assertThatThrownBy(() -> dir.openInput("_99.cfs", IOContext.DEFAULT)).isInstanceOf(NoSuchFileException.class);
    }

    @Test
    void checkIndexPassesOverRemoteDirectory() throws Exception {
        long total = luceneFiles(manifest).stream().mapToLong(ManifestFile::length).sum();
        for (int blockSize : new int[]{64 * KIB, KIB}) { // 1 KiB: every file of the small fixture spans blocks
            BlockCache cache = cache(blockSize);
            RemoteDirectory dir = remote(cache, null, source, manifest); // no eager files: every byte via blocks
            long start = System.nanoTime();
            CheckIndex.Status status;
            try (CheckIndex check = new CheckIndex(dir)) {
                check.setInfoStream(null);
                check.setLevel(CheckIndex.Level.MIN_LEVEL_FOR_SLOW_CHECKS);
                status = check.checkIndex();
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.printf("CheckIndex over RemoteDirectory, %d B blocks, %d B of files: %d ms, %s%n",
                    blockSize, total, millis, cache.stats());
            assertThat(status.clean).as("CheckIndex clean with %d B blocks", blockSize).isTrue();
            assertThat(status.numSegments).isPositive();
            // CheckIndex verifies every file's checksum, so every block was fetched exactly once.
            assertThat(cache.stats().bytesFetched()).isEqualTo(total);
            assertThat(cache.stats().leasesOpen()).isZero();
        }
    }

    @Test
    void queriesMatchFsDirectory() throws Exception {
        // NIOFSDirectory reads through the same generic paths as the remote input: everything is
        // identical. MMapDirectory scores vectors with MemorySegment code that sums in another order,
        // so KNN scores may differ from it in the last bit (see CachingRemoteIndexInput).
        try (FSDirectory nio = new NIOFSDirectory(fullCopy); DirectoryReader viaNio = DirectoryReader.open(nio);
             FSDirectory mmap = new MMapDirectory(fullCopy); DirectoryReader viaMmap = DirectoryReader.open(mmap)) {
            for (int blockSize : new int[]{64 * KIB, KIB}) {
                RemoteDirectory dir = remote(cache(blockSize), eagerCopy, source, manifest);
                try (DirectoryReader actual = DirectoryReader.open(dir)) {
                    assertSameResults(viaNio, actual, true);
                    assertSameResults(viaMmap, actual, false);
                }
            }
        }
    }

    private static void assertSameResults(DirectoryReader expected, DirectoryReader actual, boolean exactKnnScores)
            throws IOException {
        assertThat(actual.maxDoc()).isEqualTo(expected.maxDoc()).isPositive();
        IndexSearcher e = new IndexSearcher(expected);
        IndexSearcher a = new IndexSearcher(actual);

        List<Query> queries = new ArrayList<>();
        List<String> methodTerms = terms(expected, DocumentMapper.F_METHOD_NAME, 1, 20);
        List<String> bodyTerms = terms(expected, DocumentMapper.F_BODY, 2, 40);
        assertThat(methodTerms).isNotEmpty();
        assertThat(bodyTerms).hasSizeGreaterThan(4);
        for (String t : methodTerms) queries.add(new TermQuery(new Term(DocumentMapper.F_METHOD_NAME, t)));
        for (int i = 0; i + 1 < bodyTerms.size(); i += 2) {
            queries.add(new TermQuery(new Term(DocumentMapper.F_BODY, bodyTerms.get(i))));
            queries.add(new PhraseQuery(20, DocumentMapper.F_BODY, bodyTerms.get(i), bodyTerms.get(i + 1)));
            queries.add(new PhraseQuery(DocumentMapper.F_BODY, bodyTerms.get(i), bodyTerms.get(i + 1)));
            String prefix = bodyTerms.get(i).substring(0, Math.min(2, bodyTerms.get(i).length()));
            queries.add(new PrefixQuery(new Term(DocumentMapper.F_BODY, prefix)));
        }
        String vectorField = null;
        for (FieldInfo fi : expected.leaves().get(0).reader().getFieldInfos()) {
            if (fi.getVectorDimension() > 0) {
                vectorField = fi.name;
                break;
            }
        }
        assertThat(vectorField).as("the fixture has a vector field").isNotNull();
        for (String text : List.of("add two numbers", "greeting service", "user model")) {
            queries.add(new KnnFloatVectorQuery(vectorField, HashEmbeddings.vector(text), 5));
        }

        int withHits = 0;
        for (Query q : queries) {
            TopDocs te = e.search(q, 50);
            TopDocs ta = a.search(q, 50);
            assertThat(ta.totalHits).as("%s", q).isEqualTo(te.totalHits);
            assertThat(ta.scoreDocs).as("%s", q).hasSameSizeAs(te.scoreDocs);
            for (int i = 0; i < te.scoreDocs.length; i++) {
                assertThat(ta.scoreDocs[i].doc).as("%s #%d", q, i).isEqualTo(te.scoreDocs[i].doc);
                float want = te.scoreDocs[i].score;
                if (exactKnnScores || !(q instanceof KnnFloatVectorQuery)) {
                    assertThat(ta.scoreDocs[i].score).as("%s #%d", q, i).isEqualTo(want);
                } else {
                    assertThat(ta.scoreDocs[i].score).as("%s #%d", q, i)
                            .isCloseTo(want, org.assertj.core.data.Offset.offset(2 * Math.ulp(want)));
                }
            }
            if (te.scoreDocs.length > 0) withHits++;
        }
        assertThat(withHits).isGreaterThan(queries.size() / 2);

        // Stored fields of every document.
        StoredFields se = expected.storedFields();
        StoredFields sa = actual.storedFields();
        for (int d = 0; d < expected.maxDoc(); d++) {
            assertThat(sa.document(d).toString()).isEqualTo(se.document(d).toString());
        }
        // Numeric doc values.
        int valued = 0;
        for (int l = 0; l < expected.leaves().size(); l++) {
            LeafReaderContext le = expected.leaves().get(l);
            LeafReaderContext la = actual.leaves().get(l);
            NumericDocValues ve = le.reader().getNumericDocValues(DocumentMapper.F_IN_DEGREE);
            NumericDocValues va = la.reader().getNumericDocValues(DocumentMapper.F_IN_DEGREE);
            if (ve == null) {
                assertThat(va).isNull();
                continue;
            }
            for (int d = ve.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = ve.nextDoc()) {
                assertThat(va.advance(d)).isEqualTo(d);
                assertThat(va.longValue()).isEqualTo(ve.longValue());
                valued++;
            }
            assertThat(va.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
        }
        assertThat(valued).isPositive();
    }

    /** Up to {@code max} terms of {@code field} with at least {@code minDocFreq} docs, in term order. */
    private static List<String> terms(DirectoryReader r, String field, int minDocFreq, int max) throws IOException {
        List<String> out = new ArrayList<>();
        for (LeafReaderContext ctx : r.leaves()) {
            Terms terms = ctx.reader().terms(field);
            if (terms == null) continue;
            TermsEnum te = terms.iterator();
            for (BytesRef t = te.next(); t != null && out.size() < max; t = te.next()) {
                String s = t.utf8ToString();
                if (te.docFreq() >= minDocFreq && s.length() >= 3 && !out.contains(s)) out.add(s);
            }
        }
        return out;
    }

    @Test
    void diskFullStreamsBlocksIntoMemoryWithoutDownloadRange() throws Exception {
        // No usable space: every block takes the uncached path, which must stream via openRange
        // rather than spilling a downloadRange temp file into java.io.tmpdir.
        BlockCache cache = track(new BlockCache(tmp.resolve("full-disk"), 1L << 30, KIB, 4,
                System::currentTimeMillis, () -> 0L));
        ReplicaSource noDownloads = new DelegatingSource(source) {
            @Override
            public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
                throw new IOException("downloadRange must not be used on a full disk");
            }
        };
        RemoteDirectory dir = remote(cache, null, noDownloads, manifest);
        try (CheckIndex check = new CheckIndex(dir)) {
            check.setInfoStream(null);
            assertThat(check.checkIndex().clean).isTrue();
        }
        assertThat(cache.stats().writeFailures()).isPositive();
        assertThat(cache.stats().bytesOnDisk()).isZero();
    }

    // ---------------------------------------------------------------- a synthetic object

    /** One "Lucene" file of random bytes, published by hand. */
    private record Blob(byte[] data, ShardManifest manifest) {}

    private static Blob blob(String name, int size) throws IOException {
        byte[] data = new byte[size];
        new Random(size).nextBytes(data);
        String key = "pharos/shards/" + PROJECT + "/objects/" + name + "-test";
        store.put(key, data);
        ShardManifest m = manifestOf(7, List.of(new ManifestFile(ManifestFile.LUCENE, name, key, size, "0")));
        return new Blob(data, m);
    }

    private static ShardManifest manifestOf(long gen, List<ManifestFile> files) {
        return new ShardManifest(ShardManifest.FORMAT_VERSION, PROJECT, gen, null, Instant.now(), "writer", 0,
                files.stream().mapToLong(ManifestFile::length).sum(), null, files, null, null);
    }

    private static ByteBuffer le(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void readsAcrossBlockBoundary() throws Exception {
        Blob b = blob("_x.bin", 10_000);
        RemoteDirectory dir = remote(cache(KIB), null, source, b.manifest());
        try (IndexInput in = dir.openInput("_x.bin", IOContext.DEFAULT)) {
            assertThat(in).isInstanceOf(CachingRemoteIndexInput.class);
            assertThat(in.length()).isEqualTo(10_000);
            ByteBuffer d = le(b.data());

            in.seek(KIB - 3);
            assertThat(in.readLong()).isEqualTo(d.getLong(KIB - 3));
            assertThat(in.getFilePointer()).isEqualTo(KIB + 5);
            in.seek(2 * KIB - 2);
            assertThat(in.readInt()).isEqualTo(d.getInt(2 * KIB - 2));
            in.seek(3 * KIB - 1);
            assertThat(in.readShort()).isEqualTo(d.getShort(3 * KIB - 1));

            RandomAccessInput ra = (RandomAccessInput) in;
            for (long p : new long[]{0, KIB - 7, KIB - 1, KIB, 5 * KIB - 4, 10_000 - 8}) {
                assertThat(ra.readLong(p)).as("long at %d", p).isEqualTo(d.getLong((int) p));
                assertThat(ra.readInt(p)).as("int at %d", p).isEqualTo(d.getInt((int) p));
                assertThat(ra.readShort(p)).as("short at %d", p).isEqualTo(d.getShort((int) p));
                assertThat(ra.readByte(p)).as("byte at %d", p).isEqualTo(b.data()[(int) p]);
            }

            // Long and short bulk reads spanning several blocks, from the sequential and positional APIs.
            byte[] big = new byte[9_000];
            in.seek(500);
            in.readBytes(big, 0, big.length);
            assertThat(big).isEqualTo(java.util.Arrays.copyOfRange(b.data(), 500, 9_500));
            byte[] small = new byte[100];
            ra.readBytes(KIB - 50, small, 0, small.length);
            assertThat(small).isEqualTo(java.util.Arrays.copyOfRange(b.data(), KIB - 50, KIB + 50));

            // Byte-by-byte over the whole object.
            in.seek(0);
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            for (int i = 0; i < 10_000; i++) all.write(in.readByte());
            assertThat(all.toByteArray()).isEqualTo(b.data());

            // Past the end.
            assertThatThrownBy(in::readByte).isInstanceOf(EOFException.class);
            in.seek(10_000 - 3);
            assertThatThrownBy(in::readInt).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> in.readBytes(new byte[10], 0, 10)).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> ra.readLong(10_000 - 7)).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> ra.readByte(10_000)).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> in.seek(10_001)).isInstanceOf(EOFException.class);
        }
    }

    /** Bulk float/int/long reads equal NIOFSDirectory's (little-endian), at any alignment and across blocks. */
    @Test
    void bulkReadsMatchNiofs() throws Exception {
        Blob b = blob("_v.bin", 10_000);
        Path local = tmp.resolve("bulk");
        Files.createDirectories(local);
        Files.write(local.resolve("_v.bin"), b.data());
        RemoteDirectory dir = remote(cache(KIB), null, source, b.manifest());
        try (FSDirectory nio = new NIOFSDirectory(local);
             IndexInput want = nio.openInput("_v.bin", IOContext.DEFAULT);
             IndexInput got = dir.openInput("_v.bin", IOContext.DEFAULT)) {
            for (long start : new long[]{0, 1, 3, KIB - 6, KIB - 4, 2 * KIB - 1, 5_000}) {
                for (int count : new int[]{1, 2, 7, 300, 1_000}) {
                    if (start + 8L * count > 10_000) continue;
                    want.seek(start);
                    got.seek(start);
                    float[] wf = new float[count + 2];
                    float[] gf = new float[count + 2];
                    want.readFloats(wf, 1, count);
                    got.readFloats(gf, 1, count);
                    assertThat(gf).as("floats at %d x %d", start, count).isEqualTo(wf);
                    assertThat(got.getFilePointer()).isEqualTo(want.getFilePointer());

                    want.seek(start);
                    got.seek(start);
                    int[] wi = new int[count + 2];
                    int[] gi = new int[count + 2];
                    want.readInts(wi, 1, count);
                    got.readInts(gi, 1, count);
                    assertThat(gi).as("ints at %d x %d", start, count).isEqualTo(wi);
                    assertThat(got.getFilePointer()).isEqualTo(want.getFilePointer());

                    want.seek(start);
                    got.seek(start);
                    long[] wl = new long[count + 2];
                    long[] gl = new long[count + 2];
                    want.readLongs(wl, 1, count);
                    got.readLongs(gl, 1, count);
                    assertThat(gl).as("longs at %d x %d", start, count).isEqualTo(wl);
                    assertThat(got.getFilePointer()).isEqualTo(want.getFilePointer());
                }
            }
            got.seek(10_000 - 7);
            assertThatThrownBy(() -> got.readLongs(new long[1], 0, 1)).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> got.readFloats(new float[2], 0, 2)).isInstanceOf(EOFException.class);
            assertThat(got.getFilePointer()).as("a failed bulk read does not move").isEqualTo(10_000 - 7);
        }
    }

    @Test
    void sliceAndCloneAreIndependent() throws Exception {
        Blob b = blob("_y.bin", 5_000);
        BlockCache cache = cache(KIB);
        RemoteDirectory dir = remote(cache, null, source, b.manifest());
        ByteBuffer d = le(b.data());
        IndexInput in = dir.openInput("_y.bin", IOContext.DEFAULT);
        in.seek(100);

        IndexInput clone = in.clone();
        assertThat(clone.getFilePointer()).isEqualTo(100);
        clone.seek(4_000);
        assertThat(clone.readByte()).isEqualTo(b.data()[4_000]);
        assertThat(in.getFilePointer()).isEqualTo(100); // the clone's seek and read did not move the parent
        assertThat(in.readByte()).isEqualTo(b.data()[100]);

        IndexInput slice = in.slice("s", 1_000, 3_000);
        assertThat(slice.length()).isEqualTo(3_000);
        assertThat(slice.getFilePointer()).isZero();
        assertThat(slice.readInt()).isEqualTo(d.getInt(1_000));
        slice.seek(2_999);
        assertThat(slice.readByte()).isEqualTo(b.data()[3_999]);
        assertThatThrownBy(slice::readByte).isInstanceOf(EOFException.class); // its own end, not the file's
        assertThatThrownBy(() -> ((RandomAccessInput) slice).readByte(3_000)).isInstanceOf(EOFException.class);
        assertThatThrownBy(() -> slice.seek(3_001)).isInstanceOf(EOFException.class);
        assertThat(((RandomAccessInput) slice).readLong(KIB)).isEqualTo(d.getLong(1_000 + KIB));

        IndexInput nested = slice.slice("n", 500, 1_000); // file range [1500, 2500)
        assertThat(nested.readLong()).isEqualTo(d.getLong(1_500));
        IndexInput nestedClone = nested.clone();
        nestedClone.seek(992);
        assertThat(nestedClone.readLong()).isEqualTo(d.getLong(2_492));
        assertThatThrownBy(nestedClone::readByte).isInstanceOf(EOFException.class);
        assertThat(nested.getFilePointer()).isEqualTo(8);

        RandomAccessInput ras = in.randomAccessSlice(2_000, 100);
        assertThat(ras.readInt(0)).isEqualTo(d.getInt(2_000));
        assertThatThrownBy(() -> ras.readInt(97)).isInstanceOf(EOFException.class);

        assertThatThrownBy(() -> in.slice("bad", 4_000, 1_001)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> slice.slice("bad", -1, 10)).isInstanceOf(IllegalArgumentException.class);

        // Closing clones and slices (which Lucene never does) leaves the parent usable, and vice versa.
        clone.close();
        slice.close();
        assertThat(in.readByte()).isEqualTo(b.data()[101]);
        in.close();
        assertThat(nested.readByte()).isEqualTo(b.data()[1_508]);
        assertThat(cache.stats().leasesOpen()).isZero();
    }

    @Test
    void prefetchFetchesBlocksAheadWithoutWaiting() throws Exception {
        Blob b = blob("_p.bin", 10 * KIB + 10);
        BlockCache cache = cache(KIB);
        RemoteDirectory dir = remote(cache, null, source, b.manifest());
        try (IndexInput in = dir.openInput("_p.bin", IOContext.DEFAULT)) {
            IndexInput slice = in.slice("tail", 2 * KIB, 8 * KIB + 10);
            slice.prefetch(0, slice.length()); // blocks 2..10
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (cache.stats().bytesOnDisk() < 8 * KIB + 10 && System.nanoTime() < deadline) Thread.sleep(5);
            assertThat(cache.stats().bytesOnDisk()).isEqualTo(8 * KIB + 10);
            assertThat(cache.stats().misses()).isZero();
            byte[] all = new byte[(int) slice.length()];
            slice.readBytes(all, 0, all.length);
            assertThat(all).isEqualTo(java.util.Arrays.copyOfRange(b.data(), 2 * KIB, b.data().length));
            assertThat(cache.stats().misses()).isZero(); // every block was already there
            assertThat(cache.stats().bytesFetched()).isEqualTo(8 * KIB + 10);
            assertThatThrownBy(() -> slice.prefetch(1, slice.length())).isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ---------------------------------------------------------------- review focus 1

    @Test
    void missingObjectFailsWithShardGenKeyInMessage() throws Exception {
        // Two segments, not compound: segment 0's stored fields span many blocks; segment 1 is separate.
        Path local = tmp.resolve("synthetic");
        Random rnd = new Random(3);
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer())
                .setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
        try (FSDirectory fs = FSDirectory.open(local); IndexWriter w = new IndexWriter(fs, iwc)) {
            for (int i = 0; i < 200; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "alpha", Field.Store.NO));
                byte[] payload = new byte[2_000];
                rnd.nextBytes(payload);
                doc.add(new StoredField("payload", HexFormat.of().formatHex(payload)));
                w.addDocument(doc);
            }
            w.commit();
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "beta", Field.Store.YES));
                doc.add(new StoredField("payload", "small-" + i));
                w.addDocument(doc);
            }
            w.commit();
        }
        List<ManifestFile> files = new ArrayList<>();
        String fdtKey = null;
        String fdtName = null;
        try (Stream<Path> s = Files.list(local)) {
            for (Path p : s.sorted().toList()) {
                String name = p.getFileName().toString();
                if (name.equals("write.lock")) continue;
                String key = "pharos/shards/" + PROJECT + "/objects/synthetic-" + name;
                store.put(key, p);
                files.add(new ManifestFile(ManifestFile.LUCENE, name, key, Files.size(p), "0"));
                if (name.startsWith("_0") && name.endsWith(".fdt")) {
                    fdtKey = key;
                    fdtName = name;
                }
            }
        }
        assertThat(fdtKey).as("segment 0 has its own .fdt").isNotNull();
        byte[] fdtBytes = Files.readAllBytes(local.resolve(fdtName));
        assertThat(fdtBytes.length).isGreaterThan(20 * 4 * KIB);

        RemoteDirectory dir = remote(cache(4 * KIB), null, source, manifestOf(42, files));
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            store.delete(fdtKey);

            // A middle document of segment 0 needs blocks of the deleted .fdt that were never read.
            String key = fdtKey;
            String name = fdtName;
            assertThatThrownBy(() -> reader.storedFields().document(100))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("shard " + PROJECT)
                    .hasMessageContaining("generation 42")
                    .hasMessageContaining(name)
                    .hasMessageContaining(key)
                    .hasRootCauseInstanceOf(NoSuchFileException.class);

            // Queries that touch other files still work on the same reader.
            TopDocs beta = searcher.search(new TermQuery(new Term("tag", "beta")), 10);
            assertThat(beta.totalHits.value()).isEqualTo(5);
            for (var sd : beta.scoreDocs) {
                assertThat(reader.storedFields().document(sd.doc).get("tag")).isEqualTo("beta");
            }
            assertThat(searcher.count(new TermQuery(new Term("tag", "alpha")))).isEqualTo(200);

            // Once the object is back, the same read retries and succeeds.
            store.put(key, fdtBytes);
            assertThat(reader.storedFields().document(100).get("payload")).hasSize(4_000);
        }
    }

    /**
     * Final review I2: an object store that never answers. The waiter gives up after the cache's fetch
     * timeout with an IOException naming the shard and object key, instead of stalling the query.
     */
    @Test
    void unansweredFetchTimesOutNamingShardAndKey() throws Exception {
        Blob b = blob("_hang.bin", 3 * KIB);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ReplicaSource hanging = new ReplicaSource() {
            private void hang() throws IOException {
                try {
                    release.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new java.io.InterruptedIOException();
                }
            }
            @Override public Optional<Generation> latest(ShardId s) { return Optional.empty(); }
            @Override public ShardManifest manifest(ShardId s, Generation g) { throw new UnsupportedOperationException(); }
            @Override public void download(ManifestFile f, Path t) throws IOException { hang(); source.download(f, t); }
            @Override public void downloadRange(ManifestFile f, long o, long l, Path t) throws IOException {
                hang();
                source.downloadRange(f, o, l, t);
            }
            @Override public InputStream openRange(ManifestFile f, long o, long l) throws IOException {
                hang();
                return source.openRange(f, o, l);
            }
            @Override public Optional<byte[]> synonyms() { return Optional.empty(); }
        };
        BlockCache cache = track(new BlockCache(tmp.resolve("hang"), 1L << 30, KIB, 4, Duration.ofMillis(300)));
        RemoteDirectory dir = remote(cache, null, hanging, b.manifest());
        try (IndexInput in = dir.openInput("_hang.bin", IOContext.DEFAULT)) {
            long start = System.nanoTime();
            assertThatThrownBy(in::readByte)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("shard " + PROJECT)
                    .hasMessageContaining(b.manifest().files().get(0).key())
                    .hasMessageContaining("timed out");
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isBetween(250L, 10_000L);
            assertThat(cache.stats().leasesOpen()).isZero();
            release.countDown();
            // the store answers again: the next read succeeds
            in.seek(0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            byte first;
            while (true) {
                try {
                    first = in.readByte();
                    break;
                } catch (IOException e) {
                    if (System.nanoTime() > deadline) throw e;
                }
            }
            assertThat(first).isEqualTo(b.data()[0]);
        } finally {
            release.countDown();
        }
    }

    /** The disk-full branch (fetchToMemory → openRange) reports a missing object like the cached one. */
    @Test
    void missingObjectOnTheUncachedPathFailsWithShardGenKeyInMessage() throws Exception {
        Blob b = blob("_m.bin", 3 * KIB);
        BlockCache cache = track(new BlockCache(tmp.resolve("no-space"), 1L << 30, KIB, 4,
                System::currentTimeMillis, () -> 0L));
        RemoteDirectory dir = remote(cache, null, source, b.manifest());
        String key = b.manifest().files().get(0).key();
        store.delete(key);
        try (IndexInput in = dir.openInput("_m.bin", IOContext.DEFAULT)) {
            assertThatThrownBy(in::readByte)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("shard " + PROJECT)
                    .hasMessageContaining("generation 7")
                    .hasMessageContaining("_m.bin")
                    .hasMessageContaining(key)
                    .hasRootCauseInstanceOf(NoSuchFileException.class);
        } finally {
            store.put(key, b.data());
        }
        assertThat(cache.stats().bytesOnDisk()).isZero();
    }

    /**
     * A ranged body that ends early (connection closed mid-body, no SDK error) is an EOF on the
     * uncached path: the block is never served short, and nothing is cached.
     */
    @Test
    void midBodyTruncationOnTheUncachedPathIsEof() throws Exception {
        Blob b = blob("_t.bin", 3 * KIB);
        BlockCache cache = track(new BlockCache(tmp.resolve("no-space-trunc"), 1L << 30, KIB, 4,
                System::currentTimeMillis, () -> 0L));
        ReplicaSource truncating = new DelegatingSource(source) {
            @Override
            public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
                try (InputStream in = super.openRange(file, offset, length)) {
                    return new java.io.ByteArrayInputStream(in.readNBytes((int) (length / 2)));
                }
            }
        };
        RemoteDirectory dir = remote(cache, null, truncating, b.manifest());
        try (IndexInput in = dir.openInput("_t.bin", IOContext.DEFAULT)) {
            assertThatThrownBy(in::readByte)
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("short range body");
        }
        assertThat(cache.stats().bytesOnDisk()).isZero();
    }

    /** An eager copy of the wrong size (a torn download, tampering) is replaced, not fatal. */
    @Test
    void eagerFileOfTheWrongSizeIsDownloadedAgain() throws Exception {
        ManifestFile f = luceneFiles(manifest).stream()
                .max(java.util.Comparator.comparingLong(ManifestFile::length)).orElseThrow();
        byte[] real = Files.readAllBytes(fullCopy.resolve(f.name()));
        Path eager = tmp.resolve("eager-wrong");
        Files.createDirectories(eager);
        Path local = eager.resolve(f.name());
        Files.write(local, java.util.Arrays.copyOf(real, 100)); // truncated
        RemoteDirectory dir = remote(cache(KIB), eager, source, manifest);
        try (IndexInput in = dir.openInput(f.name(), IOContext.DEFAULT)) {
            assertThat(in).isNotInstanceOf(CachingRemoteIndexInput.class);
            byte[] all = new byte[real.length];
            in.readBytes(all, 0, all.length);
            assertThat(all).isEqualTo(real);
        }
        assertThat(local).hasBinaryContent(real);

        // A download whose checksum is not the manifest's is never moved in.
        long crc = Long.parseUnsignedLong(f.checksum(), 16);
        List<ManifestFile> lying = new ArrayList<>(luceneFiles(manifest));
        lying.replaceAll(x -> x == f ? new ManifestFile(x.role(), x.name(), x.key(), x.length(),
                Long.toHexString(crc ^ 1)) : x);
        Files.write(local, java.util.Arrays.copyOf(real, 100));
        RemoteDirectory checked = remote(cache(KIB), eager, source, manifestOf(7, lying));
        assertThatThrownBy(() -> checked.openInput(f.name(), IOContext.DEFAULT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum")
                .hasMessageContaining("shard " + PROJECT)
                .hasMessageContaining("generation 7")
                .hasMessageContaining(f.key());
        assertThat(local).hasSize(100);

        // When the object can't be fetched either, the error names shard, generation and key.
        store.delete(f.key());
        try {
            RemoteDirectory again = remote(cache(KIB), eager, source, manifestOf(7, luceneFiles(manifest)));
            assertThatThrownBy(() -> again.openInput(f.name(), IOContext.DEFAULT))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("shard " + PROJECT)
                    .hasMessageContaining("generation 7")
                    .hasMessageContaining(f.key());
        } finally {
            store.put(f.key(), real);
        }
    }

    // ---------------------------------------------------------------- edges: heads and synthesised footers

    /** The fixture rewritten as one segment without compound files (the layout of a large shard), published. */
    private record Flat(Path files, ShardManifest manifest) {}

    private Flat flatFixture() throws IOException {
        Path src = tmp.resolve("flat-src");
        Path dir = tmp.resolve("flat");
        Files.createDirectories(src);
        Files.createDirectories(dir);
        for (ManifestFile f : luceneFiles(manifest)) Files.copy(fullCopy.resolve(f.name()), src.resolve(f.name()));
        org.apache.lucene.index.TieredMergePolicy mp = new org.apache.lucene.index.TieredMergePolicy();
        mp.setNoCFSRatio(0.0);
        // rewritten into a fresh dir, so the result is never the source's (possibly compound) single segment
        try (FSDirectory from = FSDirectory.open(src);
             org.apache.lucene.index.DirectoryReader r = org.apache.lucene.index.DirectoryReader.open(from);
             FSDirectory fs = FSDirectory.open(dir); IndexWriter w = new IndexWriter(fs,
                new IndexWriterConfig(new StandardAnalyzer()).setUseCompoundFile(false).setMergePolicy(mp))) {
            List<org.apache.lucene.index.CodecReader> leaves = new ArrayList<>();
            for (var leaf : r.leaves()) leaves.add(org.apache.lucene.index.SlowCodecReaderWrapper.wrap(leaf.reader()));
            w.addIndexes(leaves.toArray(new org.apache.lucene.index.CodecReader[0]));
            w.forceMerge(1);
            w.commit();
        }
        List<ManifestFile> files = new ArrayList<>();
        try (FSDirectory fs = FSDirectory.open(dir); Stream<Path> s = Files.list(dir)) {
            for (Path p : s.sorted().toList()) {
                String name = p.getFileName().toString();
                if (name.equals("write.lock")) continue;
                String key = "pharos/shards/" + PROJECT + "/objects/flat-" + name;
                store.put(key, p);
                long crc;
                try (IndexInput in = fs.openInput(name, IOContext.READONCE)) {
                    crc = org.apache.lucene.codecs.CodecUtil.retrieveChecksum(in);
                }
                files.add(new ManifestFile(ManifestFile.LUCENE, name, key, Files.size(p), Long.toHexString(crc)));
            }
        }
        assertThat(files).anySatisfy(f -> assertThat(f.name()).endsWith(".tim"));
        return new Flat(dir, manifestOf(11, files));
    }

    private static boolean alwaysEager(String name) {
        return name.startsWith("segments_") || name.endsWith(".si") || name.endsWith(".fnm") || name.endsWith(".cfe");
    }

    /** The always-eager files plus a head sidecar ({@code .edges/<name>.head}) for every other file. */
    private Path eagerWithHeads(Flat flat) throws IOException {
        Path dir = tmp.resolve("eager-heads");
        Files.createDirectories(dir.resolve(RemoteDirectory.EDGES_DIR));
        for (ManifestFile f : luceneFiles(flat.manifest())) {
            byte[] all = Files.readAllBytes(flat.files().resolve(f.name()));
            if (alwaysEager(f.name())) {
                Files.write(dir.resolve(f.name()), all);
            } else {
                Files.write(RemoteDirectory.headFile(dir, f.name()),
                        java.util.Arrays.copyOf(all, (int) Math.min(all.length, RemoteDirectory.HEAD_BYTES)));
            }
        }
        return dir;
    }

    /** Opening a reader reads each file's header and footer: the sidecars and the manifest serve both. */
    @Test
    void openingAReaderWithEdgesFetchesNothing() throws Exception {
        Flat flat = flatFixture();
        Path eager = eagerWithHeads(flat);
        assertThat(luceneFiles(flat.manifest())).as("some files are longer than their head")
                .anySatisfy(f -> assertThat(f.length()).isGreaterThan(RemoteDirectory.HEAD_BYTES));
        for (int blockSize : new int[]{64 * KIB, KIB}) {
            BlockCache cache = cache(blockSize);
            RemoteDirectory dir = track(new RemoteDirectory(SHARD, flat.manifest(), eager, cache, source, true));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.maxDoc()).isPositive();
                assertThat(cache.stats().bytesFetched()).as("%d B blocks", blockSize).isZero();
            }
            // The synthesised footer is byte-identical to every real one.
            for (ManifestFile f : luceneFiles(flat.manifest())) {
                byte[] all = Files.readAllBytes(flat.files().resolve(f.name()));
                try (IndexInput in = dir.openInput(f.name(), IOContext.DEFAULT)) {
                    byte[] tail = new byte[16];
                    in.seek(f.length() - 16);
                    in.readBytes(tail, 0, 16);
                    assertThat(tail).as(f.name()).isEqualTo(java.util.Arrays.copyOfRange(all, all.length - 16, all.length));
                }
            }
            // Reading everything (CheckIndex) checks each fetched tail block against its footer.
            try (CheckIndex check = new CheckIndex(dir)) {
                check.setInfoStream(null);
                assertThat(check.checkIndex().clean).isTrue();
            }
            assertThat(cache.stats().bytesFetched()).isPositive();
        }
    }

    @Test
    void corruptTailBlockIsDetectedAgainstTheFooter() throws Exception {
        ManifestFile victim = luceneFiles(manifest).stream()
                .filter(f -> !Files.exists(eagerCopy.resolve(f.name())))
                .max(java.util.Comparator.comparingLong(ManifestFile::length)).orElseThrow();
        long wrong = Long.parseUnsignedLong(victim.checksum(), 16) ^ 0x5a5aL;
        List<ManifestFile> files = new ArrayList<>(luceneFiles(manifest));
        files.replaceAll(f -> f == victim ? new ManifestFile(f.role(), f.name(), f.key(), f.length(),
                Long.toHexString(wrong)) : f);
        BlockCache cache = cache(KIB);
        RemoteDirectory dir = track(new RemoteDirectory(SHARD, manifestOf(9, files), null, cache, source, true));
        try (IndexInput in = dir.openInput(victim.name(), IOContext.DEFAULT)) {
            in.seek(victim.length() - 8);
            assertThat(org.apache.lucene.codecs.CodecUtil.readBELong(in)).as("served from the manifest")
                    .isEqualTo(wrong);
            assertThat(cache.stats().bytesFetched()).isZero();
            in.seek(0);
            byte[] all = new byte[(int) victim.length()];
            assertThatThrownBy(() -> in.readBytes(all, 0, all.length))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("footer")
                    .hasMessageContaining(victim.key());
        }
        long last = (victim.length() - 1) / KIB;
        assertThat(cache.isCached(new BlockKey(victim.key(), last))).as("dropped from the cache").isFalse();
    }

    // ---------------------------------------------------------------- read-only

    @Test
    void writeMethodsAreUnsupported() throws IOException {
        Blob b = blob("_w.bin", 100);
        RemoteDirectory dir = remote(cache(KIB), null, source, b.manifest());
        List<org.junit.jupiter.api.function.Executable> writes = List.of(
                () -> dir.deleteFile("_w.bin"),
                () -> dir.createOutput("_n.bin", IOContext.DEFAULT),
                () -> dir.createTempOutput("p", "s", IOContext.DEFAULT),
                () -> dir.sync(List.of("_w.bin")),
                dir::syncMetaData,
                () -> dir.rename("_w.bin", "_v.bin"));
        for (var w : writes) {
            assertThatThrownBy(w::execute)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("read-only remote directory");
        }
        assertThat(dir.listAll()).containsExactly("_w.bin");
        assertThat(dir.obtainLock("write.lock")).isNotNull(); // NoLockFactory
    }

    @Test
    void eagerFileIsReadLocallyAndOthersRemotely() throws IOException {
        RemoteDirectory dir = remote(cache(64 * KIB), eagerCopy, source, manifest);
        for (ManifestFile f : luceneFiles(manifest)) {
            try (IndexInput in = dir.openInput(f.name(), IOContext.DEFAULT)) {
                boolean eager = Files.exists(eagerCopy.resolve(f.name()));
                assertThat(in instanceof CachingRemoteIndexInput).as(f.name()).isEqualTo(!eager);
                assertThat(in.length()).isEqualTo(f.length());
            }
        }
    }

    // ---------------------------------------------------------------- doubles

    /** Deterministic fake embeddings: a unit vector seeded by the text. */
    static final class HashEmbeddings implements EmbeddingProvider {
        static final int DIMS = 16;

        static float[] vector(String text) {
            Random r = new Random(text.hashCode());
            float[] v = new float[DIMS];
            double norm = 0;
            for (int i = 0; i < DIMS; i++) {
                v[i] = (float) r.nextGaussian();
                norm += v[i] * v[i];
            }
            for (int i = 0; i < DIMS; i++) v[i] /= (float) Math.sqrt(norm);
            return v;
        }

        @Override public String modelId() { return "test-hash"; }
        @Override public float[] embed(String text) { return text == null || text.isBlank() ? null : vector(text); }
        @Override public int dimensions() { return DIMS; }
        @Override public boolean isAvailable() { return true; }
    }

    private static class DelegatingSource implements ReplicaSource {
        private final ReplicaSource d;

        DelegatingSource(ReplicaSource d) { this.d = d; }

        @Override public Optional<Generation> latest(ShardId shard) throws IOException { return d.latest(shard); }
        @Override public ShardManifest manifest(ShardId shard, Generation gen) throws IOException {
            return d.manifest(shard, gen);
        }
        @Override public void download(ManifestFile file, Path target) throws IOException { d.download(file, target); }
        @Override public void downloadRange(ManifestFile file, long offset, long length, Path target)
                throws IOException {
            d.downloadRange(file, offset, length, target);
        }
        @Override public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
            return d.openRange(file, offset, length);
        }
        @Override public Optional<byte[]> synonyms() throws IOException { return d.synonyms(); }
    }
}
