package com.pharos.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;

import java.util.List;
import java.util.Random;

/**
 * The float-vector arithmetic Lucene uses in this JVM, which decides the exact bits of every vector score.
 *
 * <p>Lucene picks per CPU, once, at class load: fused multiply-add or not ({@code Constants.HAS_FAST_SCALAR_FMA},
 * {@code HAS_FAST_VECTOR_FMA}) and, with the Java Vector API enabled ({@code --add-modules jdk.incubator.vector}),
 * the SIMD width of its Panama code (512 bits with AVX-512, 256 with AVX2, 128 on NEON). Each choice changes the
 * rounding or the summation order of a cosine, so two readers on different CPUs score the same document about one
 * ulp apart, and exact ties between shards flip order in a fan-out. The Panama code is not even stable within one
 * JVM: its lane reductions round differently before and after C2 compiles them.
 *
 * <p>Ruling CPU-R1 (1.12.0): readers run Lucene's scalar code with FMA off ({@link #DETERMINISTIC_FLAGS}, and no
 * {@code --add-modules jdk.incubator.vector}). Plain Java float arithmetic is strictly specified, so every JVM on
 * every CPU (x86-64 with or without FMA or AVX, aarch64, C1 or C2) produces the same bits. Writers, routers and
 * single nodes keep Lucene's per-CPU choice: no score they compute reaches a fan-out. Neither can be changed once
 * the JVM runs; every pharos launcher passes the flags ({@code docker/pharos/entrypoint.sh}, which the operator's
 * pods run, the {@code ./pharos} client and {@code setup.sh}'s daemon start). Measurements are in the README,
 * "Multi-node CPU determinism".
 *
 * <p>{@code canary} is a hash of the exact bits of a few fixed 384- and 768-dim cosines computed through
 * {@link VectorUtil} after a warm-up: equal profiles score identically. Readers advertise their profile in the
 * heartbeat, and the membership warns when the live readers' profiles differ.
 *
 * @param scalarFma  Lucene's {@code HAS_FAST_SCALAR_FMA}
 * @param vectorFma  Lucene's {@code HAS_FAST_VECTOR_FMA} (only used by the Panama code)
 * @param simdBits   the vector width of Lucene's Panama code; 0 when Lucene runs its scalar code
 * @param canary     8 hex digits over the bits of the fixed cosines
 * @param requested  the {@value #PROPERTY} system property the launcher set ({@value #DETERMINISTIC},
 *                   {@value #NATIVE} when PHAROS_ARITHMETIC=native was set explicitly, {@value #NATIVE_DEFAULT}
 *                   when native is only the launcher's default for a non-reader role), or null when it set
 *                   none (a custom launcher)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ArithmeticProfile(boolean scalarFma, boolean vectorFma, int simdBits, String canary, String requested) {

    /** The system property a launcher sets to say which arithmetic it asked for. */
    public static final String PROPERTY = "pharos.arithmetic";
    public static final String DETERMINISTIC = "deterministic";
    public static final String NATIVE = "native";
    /**
     * What a launcher passes when native is only its default for the role it was given (not {@code reader}), not
     * an explicit {@code PHAROS_ARITHMETIC=native}: a node that then serves as a reader still warns.
     */
    public static final String NATIVE_DEFAULT = "native-default";
    /** The JVM flags of {@link #DETERMINISTIC}; the launchers' copies are checked against this list by tests. */
    public static final List<String> DETERMINISTIC_FLAGS = List.of(
            "-Dlucene.useScalarFMA=false", "-Dlucene.useVectorFMA=false", "-D" + PROPERTY + "=" + DETERMINISTIC);
    /** The JVM flag a {@link #DETERMINISTIC} launch leaves out: it turns on Lucene's Panama code. */
    public static final String VECTOR_MODULE = "jdk.incubator.vector";

    private static volatile ArithmeticProfile effective;

    /** This JVM's profile, computed once (a few milliseconds of vector math: the canary warms up first). */
    public static ArithmeticProfile effective() {
        ArithmeticProfile p = effective;
        if (p == null) {
            p = new ArithmeticProfile(Constants.HAS_FAST_SCALAR_FMA, Constants.HAS_FAST_VECTOR_FMA, detectSimdBits(),
                    computeCanary(), System.getProperty(PROPERTY));
            effective = p;
        }
        return p;
    }

    /** True for the setting of ruling CPU-R1: Lucene's scalar code, no FMA. */
    @JsonIgnore
    public boolean isDeterministic() {
        return !scalarFma && simdBits == 0;
    }

    /** e.g. {@code fma=off simd=scalar canary=1a2b3c4d} or {@code fma=on simd=512 canary=…}. */
    public String summary() {
        String fma;
        if (simdBits == 0 || scalarFma == vectorFma) fma = scalarFma ? "on" : "off"; // scalar code: no vector FMA
        else fma = "scalar-" + (scalarFma ? "on" : "off") + "/vector-" + (vectorFma ? "on" : "off");
        return "fma=" + fma + " simd=" + (simdBits == 0 ? "scalar" : String.valueOf(simdBits)) + " canary=" + canary;
    }

    /**
     * The startup check of a reader: null when the arithmetic is deterministic or the launcher asked for
     * {@value #NATIVE} explicitly; otherwise the WARN to log, naming the flags. {@value #NATIVE_DEFAULT} warns: the
     * launcher defaulted to native for another role (e.g. {@code PHAROS_ROLE} unset, so {@code all}) while the
     * config made this node a reader.
     */
    public String startupWarning() {
        if (isDeterministic() || NATIVE.equals(requested)) return null;
        String why = DETERMINISTIC.equals(requested)
                ? "the launcher asked for deterministic arithmetic but the JVM options override it"
                : NATIVE_DEFAULT.equals(requested)
                ? "the launcher chose native as the default for another role (is PHAROS_ROLE, or cluster.role in"
                        + " the config it read, not reader?)"
                : "this JVM was not started with the deterministic arithmetic flags";
        return "Vector arithmetic is " + summary() + ": " + why + ". Readers on other CPU types (and this one,"
                + " before its JIT warms up) may score the same document an ulp apart and order exact ties"
                + " differently. Start readers with PHAROS_ARITHMETIC=deterministic (JVM flags "
                + String.join(" ", DETERMINISTIC_FLAGS.subList(0, 2)) + ", without --add-modules " + VECTOR_MODULE
                + "), or set PHAROS_ARITHMETIC=native to accept it.";
    }

    /** Lucene's Panama vector width, or 0 when it runs the scalar code. Mirrors Lucene's own checks. */
    static int detectSimdBits() {
        if (ModuleLayer.boot().findModule(VECTOR_MODULE).isEmpty() || Constants.IS_CLIENT_VM
                || !Constants.IS_HOTSPOT_VM || Constants.IS_JVMCI_VM) {
            return 0;
        }
        String forced = System.getProperty("tests.vectorsize"); // outside Lucene's tests: scalar code
        if (forced != null && !forced.isBlank() && !"default".equals(forced)) return 0;
        if (Boolean.getBoolean("tests.forceintegervectors")) return 0;
        int upper = 25; // VectorizationProvider's DEFAULT_UPPER_JAVA_FEATURE_VERSION in Lucene 10.4
        try {
            upper = Math.max(upper, Integer.parseInt(
                    System.getProperty("org.apache.lucene.vectorization.upperJavaFeatureVersion", "25")));
        } catch (NumberFormatException ignored) {
            // Lucene ignores it too
        }
        if (Runtime.version().feature() > upper) return 0;
        try {
            Class<?> shape = Class.forName("jdk.incubator.vector.VectorShape");
            Object preferred = shape.getMethod("preferredShape").invoke(null);
            int bits = (Integer) shape.getMethod("vectorBitSize").invoke(preferred);
            return bits < 128 ? 0 : bits; // Lucene refuses Panama below 128 bits
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return 0;
        }
    }

    /** Hash of the exact bits of fixed cosines at 384 and 768 dims (the embedding sizes pharos uses). */
    static String computeCanary() {
        // Warm up first: Lucene's Panama code rounds differently before C2 compiles it (the scalar code doesn't),
        // and the canary should show the bits queries get. A few milliseconds.
        float[] x = new float[768], y = new float[768];
        Random w = new Random(1);
        for (int i = 0; i < x.length; i++) { x[i] = (float) w.nextGaussian(); y[i] = (float) w.nextGaussian(); }
        float sink = 0;
        for (int i = 0; i < 20_000; i++) sink += VectorUtil.cosine(x, y);
        if (sink == 42f) System.out.print(""); // keeps the loop
        Random r = new Random(20261126L);
        int h = 0x811C9DC5;
        for (int dim : new int[]{384, 768}) {
            for (int pair = 0; pair < 4; pair++) {
                float[] a = new float[dim], b = new float[dim];
                for (int i = 0; i < dim; i++) {
                    a[i] = (float) r.nextGaussian();
                    b[i] = (float) r.nextGaussian();
                }
                int bits = Float.floatToRawIntBits(VectorUtil.cosine(a, b));
                h = (h ^ bits) * 0x01000193;
                bits = Float.floatToRawIntBits(VectorUtil.dotProduct(a, b));
                h = (h ^ bits) * 0x01000193;
            }
        }
        return String.format("%08x", h);
    }
}
