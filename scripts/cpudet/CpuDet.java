// Measures Lucene's float-vector arithmetic under the JVM flags of one run: which FMA and SIMD width
// Lucene picked, the exact bits of cosine scores, KNN query latency and HNSW indexing throughput.
// Run by scripts/cpu-determinism.sh, once per flag set, so their outputs can be compared bit for bit.
// Single-file source program: java [flags] -cp lucene-core.jar scripts/cpudet/CpuDet.java <mode> ...
//
//   info                                   effective Constants and vector shape
//   pairs <dim> <out>                      cosine/dot bits of 200 x 2000 seeded pairs, cold and warm; ns/op
//   build <dir> <dim> <n>                  HNSW index of n seeded vectors (+ duplicates), docs/s
//   search <dir> <dim> <nq> <k> <out>      KNN + exact-scan scores of nq seeded queries: bits and latency
//   fma                                    Math.fma vs a*b+c ns/op (run with -XX:-UseFMA for software fma)
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

public class CpuDet {
    static final String FIELD = "v";

    public static void main(String[] a) throws Exception {
        switch (a[0]) {
            case "info" -> info();
            case "pairs" -> pairs(Integer.parseInt(a[1]), a[2]);
            case "build" -> build(Path.of(a[1]), Integer.parseInt(a[2]), Integer.parseInt(a[3]));
            case "search" -> search(Path.of(a[1]), Integer.parseInt(a[2]), Integer.parseInt(a[3]),
                    Integer.parseInt(a[4]), a[5]);
            case "fma" -> fma();
            default -> throw new IllegalArgumentException(a[0]);
        }
    }

    static void info() throws Exception {
        var hs = ManagementFactory.getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
        String bits;
        try {
            Class<?> shape = Class.forName("jdk.incubator.vector.VectorShape");
            Object pref = shape.getMethod("preferredShape").invoke(null);
            bits = String.valueOf(shape.getMethod("vectorBitSize").invoke(pref));
        } catch (Throwable t) {
            bits = "n/a";
        }
        VectorUtil.cosine(new float[64], new float[64]); // loads the provider (its INFO line goes to stderr)
        System.out.printf("INFO HAS_FAST_VECTOR_FMA=%s HAS_FAST_SCALAR_FMA=%s UseFMA=%s MaxVectorSize=%s UseAVX=%s preferredShape=%s%n",
                Constants.HAS_FAST_VECTOR_FMA, Constants.HAS_FAST_SCALAR_FMA, hs.getVMOption("UseFMA").getValue(),
                hs.getVMOption("MaxVectorSize").getValue(), hs.getVMOption("UseAVX").getValue(), bits);
    }

    static float[] vec(Random r, int dim) {
        float[] v = new float[dim];
        double n = 0;
        for (int i = 0; i < dim; i++) { v[i] = (float) r.nextGaussian(); n += v[i] * v[i]; }
        float s = (float) (1 / Math.sqrt(n));
        for (int i = 0; i < dim; i++) v[i] *= s;
        return v;
    }

    static float[][] vecs(long seed, int count, int dim) {
        Random r = new Random(seed);
        float[][] out = new float[count][];
        for (int i = 0; i < count; i++) out[i] = vec(r, dim);
        return out;
    }

    static void pairs(int dim, String out) throws Exception {
        float[][] q = vecs(1, 200, dim), d = vecs(2, 2000, dim);
        // cold: the first calls of the process, before C2 compiles anything
        float[] cold = new float[64];
        for (int i = 0; i < cold.length; i++) cold[i] = VectorUtil.cosine(q[0], d[i]);
        float[] cos = new float[q.length * d.length], dot = new float[cos.length];
        long best = Long.MAX_VALUE;
        for (int round = 0; round < 15; round++) {
            long t0 = System.nanoTime();
            for (int i = 0; i < q.length; i++)
                for (int j = 0; j < d.length; j++) cos[i * d.length + j] = VectorUtil.cosine(q[i], d[j]);
            best = Math.min(best, System.nanoTime() - t0);
        }
        long bestDot = Long.MAX_VALUE;
        for (int round = 0; round < 15; round++) {
            long t0 = System.nanoTime();
            for (int i = 0; i < q.length; i++)
                for (int j = 0; j < d.length; j++) dot[i * d.length + j] = VectorUtil.dotProduct(q[i], d[j]);
            bestDot = Math.min(bestDot, System.nanoTime() - t0);
        }
        int coldDiff = 0;
        for (int i = 0; i < cold.length; i++) if (Float.floatToRawIntBits(cold[i]) != Float.floatToRawIntBits(cos[i])) coldDiff++;
        try (var o = new DataOutputStream(new FileOutputStream(out))) {
            for (float f : cos) o.writeInt(Float.floatToRawIntBits(f));
            for (float f : dot) o.writeInt(Float.floatToRawIntBits(f));
        }
        System.out.printf("PAIRS dim=%d cosine_ns=%.1f dot_ns=%.1f cos_sha=%s dot_sha=%s cold_vs_warm_diff=%d/64%n", dim,
                (double) best / cos.length, (double) bestDot / dot.length, sha(cos), sha(dot), coldDiff);
    }

    static void build(Path dir, int dim, int n) throws Exception {
        float[][] v = vecs(3, n, dim);
        long t0 = System.nanoTime();
        try (var d = FSDirectory.open(dir);
             var w = new IndexWriter(d, new IndexWriterConfig().setRAMBufferSizeMB(64)
                     .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
            for (int i = 0; i < n; i++) {
                Document doc = new Document();
                // every 50th vector repeats an earlier one: exact ties, as identical methods produce
                float[] x = (i % 50 == 49) ? v[i - 25] : v[i];
                doc.add(new KnnFloatVectorField(FIELD, x, VectorSimilarityFunction.COSINE));
                w.addDocument(doc);
            }
            long t1 = System.nanoTime();
            w.forceMerge(1);
            long t2 = System.nanoTime();
            System.out.printf("BUILD dim=%d n=%d add_docs_per_s=%.0f add_plus_merge_s=%.2f merge_s=%.2f%n", dim, n,
                    n / ((t1 - t0) / 1e9), (t2 - t0) / 1e9, (t2 - t1) / 1e9);
        }
    }

    static void search(Path dir, int dim, int nq, int k, String out) throws Exception {
        float[][] qs = vecs(4, nq, dim);
        try (var d = FSDirectory.open(dir); var r = DirectoryReader.open(d)) {
            var s = new IndexSearcher(r);
            s.setQueryCache(null);
            MessageDigest knnSha = MessageDigest.getInstance("SHA-256"), exactSha = MessageDigest.getInstance("SHA-256");
            TopDocs[] cold = new TopDocs[5];
            for (int i = 0; i < cold.length; i++) cold[i] = s.search(new KnnFloatVectorQuery(FIELD, qs[i], k), k);
            for (int i = 0; i < 300; i++) s.search(new KnnFloatVectorQuery(FIELD, qs[i % nq], k), k); // warm up
            long[] lat = new long[nq];
            TopDocs[] res = new TopDocs[nq];
            for (int i = 0; i < nq; i++) {
                long t0 = System.nanoTime();
                res[i] = s.search(new KnnFloatVectorQuery(FIELD, qs[i], k), k);
                lat[i] = System.nanoTime() - t0;
            }
            int coldDiff = 0;
            for (int i = 0; i < cold.length; i++) if (!Arrays.equals(enc(cold[i]), enc(res[i]))) coldDiff++;
            try (var o = new DataOutputStream(new FileOutputStream(out))) {
                for (TopDocs td : res) { byte[] b = enc(td); knnSha.update(b); o.write(b); }
                // exact scan: every document's score for the first 20 queries (the flat scorer's path)
                long e0 = System.nanoTime();
                int scored = 0;
                for (int i = 0; i < Math.min(20, nq); i++) {
                    for (LeafReaderContext leaf : r.leaves()) {
                        FloatVectorValues fv = leaf.reader().getFloatVectorValues(FIELD);
                        VectorScorer sc = fv.scorer(qs[i]);
                        DocIdSetIterator it = sc.iterator();
                        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            int bits = Float.floatToRawIntBits(sc.score());
                            exactSha.update(new byte[]{(byte) (bits >>> 24), (byte) (bits >>> 16), (byte) (bits >>> 8), (byte) bits});
                            o.writeInt(bits);
                            scored++;
                        }
                    }
                }
                long e1 = System.nanoTime();
                Arrays.sort(lat);
                double mean = Arrays.stream(lat).average().orElse(0) / 1e3;
                System.out.printf("SEARCH dim=%d nq=%d k=%d knn_mean_us=%.1f knn_p50_us=%.1f knn_p99_us=%.1f exact_ns_per_doc=%.1f knn_sha=%s exact_sha=%s cold_vs_warm_diff=%d/5%n",
                        dim, nq, k, mean, lat[nq / 2] / 1e3, lat[(int) (nq * 0.99)] / 1e3, (double) (e1 - e0) / scored,
                        hex(knnSha.digest()), hex(exactSha.digest()), coldDiff);
            }
        }
    }

    static byte[] enc(TopDocs td) {
        var b = java.nio.ByteBuffer.allocate(td.scoreDocs.length * 8);
        for (ScoreDoc sd : td.scoreDocs) b.putInt(sd.doc).putInt(Float.floatToRawIntBits(sd.score));
        return b.array();
    }

    static void fma() {
        float[] x = vecs(5, 1, 4096)[0], y = vecs(6, 1, 4096)[0];
        float acc = 0;
        long best = Long.MAX_VALUE, bestMul = Long.MAX_VALUE;
        for (int round = 0; round < 30; round++) {
            long t0 = System.nanoTime();
            for (int rep = 0; rep < 100; rep++) for (int i = 0; i < x.length; i++) acc = Math.fma(x[i], y[i], acc);
            best = Math.min(best, System.nanoTime() - t0);
            t0 = System.nanoTime();
            for (int rep = 0; rep < 100; rep++) for (int i = 0; i < x.length; i++) acc = x[i] * y[i] + acc;
            bestMul = Math.min(bestMul, System.nanoTime() - t0);
        }
        System.out.printf("FMA math_fma_ns=%.2f mul_add_ns=%.2f (acc %s)%n", best / 409600.0, bestMul / 409600.0, acc);
    }

    static String sha(float[] f) throws Exception {
        var b = java.nio.ByteBuffer.allocate(f.length * 4);
        for (float x : f) b.putInt(Float.floatToRawIntBits(x));
        return hex(MessageDigest.getInstance("SHA-256").digest(b.array()));
    }

    static String hex(byte[] b) {
        return HexFormat.of().formatHex(b).substring(0, 16);
    }
}
