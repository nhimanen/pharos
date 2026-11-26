package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.membership.ReaderHeartbeat;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ArithmeticProfileTest {

    /**
     * The canary of ruling CPU-R1's arithmetic (Lucene 10.4 scalar code, FMA off). Plain Java floats are strictly
     * specified, so this is the same on every JVM and CPU: measured on an AMD Ryzen 9700X (AVX-512) and an Intel
     * i5-1135G7. A change means every deterministic reader's scores moved (a Lucene upgrade changing its scalar
     * code): readers of the old and new release then disagree during a rolling upgrade — say so in the CHANGELOG.
     */
    static final String DETERMINISTIC_CANARY = "b10a7e42";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void deterministicMeansScalarWithoutFma() {
        assertThat(new ArithmeticProfile(false, false, 0, "x", null).isDeterministic()).isTrue();
        assertThat(new ArithmeticProfile(false, true, 0, "x", null).isDeterministic()).isTrue(); // vector FMA unused
        assertThat(new ArithmeticProfile(true, false, 0, "x", null).isDeterministic()).isFalse();
        assertThat(new ArithmeticProfile(false, false, 256, "x", null).isDeterministic()).isFalse();
    }

    @Test
    void summaryNamesFmaWidthAndCanary() {
        assertThat(new ArithmeticProfile(false, false, 0, "0badf00d", null).summary())
                .isEqualTo("fma=off simd=scalar canary=0badf00d");
        assertThat(new ArithmeticProfile(true, true, 512, "c", null).summary()).isEqualTo("fma=on simd=512 canary=c");
        assertThat(new ArithmeticProfile(false, true, 256, "c", null).summary())
                .isEqualTo("fma=scalar-off/vector-on simd=256 canary=c");
    }

    @Test
    void startupWarningOnlyWhenNotDeterministicAndNotAskedForNative() {
        assertThat(new ArithmeticProfile(false, false, 0, "c", ArithmeticProfile.DETERMINISTIC).startupWarning()).isNull();
        assertThat(new ArithmeticProfile(true, true, 512, "c", ArithmeticProfile.NATIVE).startupWarning()).isNull();
        String custom = new ArithmeticProfile(true, true, 512, "c", null).startupWarning();
        assertThat(custom).contains("not started with the deterministic arithmetic flags")
                .contains("-Dlucene.useScalarFMA=false -Dlucene.useVectorFMA=false")
                .contains("without --add-modules jdk.incubator.vector").contains("PHAROS_ARITHMETIC=native");
        assertThat(new ArithmeticProfile(false, false, 256, "c", ArithmeticProfile.DETERMINISTIC).startupWarning())
                .contains("the JVM options override it");
        // native only as the launcher's default for another role (PHAROS_ROLE unset, config says reader): warns
        assertThat(new ArithmeticProfile(true, true, 512, "c", ArithmeticProfile.NATIVE_DEFAULT).startupWarning())
                .contains("default for another role").contains("PHAROS_ARITHMETIC=deterministic");
    }

    @Test
    void effectiveProfileIsComputedOnceWithAHexCanary() {
        ArithmeticProfile p = ArithmeticProfile.effective();
        assertThat(p).isSameAs(ArithmeticProfile.effective());
        assertThat(p.canary()).matches("[0-9a-f]{8}");
        assertThat(ArithmeticProfile.computeCanary()).isEqualTo(p.canary()); // stable once warm
    }

    @Test
    void heartbeatCarriesTheProfileAndOlderHeartbeatsHaveNone() throws IOException {
        ArithmeticProfile p = new ArithmeticProfile(false, false, 0, "0badf00d", ArithmeticProfile.DETERMINISTIC);
        ReaderHeartbeat hb = new ReaderHeartbeat("r1", "http://r1", List.of("a"), false, null, 0, 0, 1L,
                ReaderHeartbeat.READY, List.of(), null, null, Boolean.TRUE, null, null, null, p);
        String json = MAPPER.writeValueAsString(hb);
        assertThat(json).contains("\"arithmetic\":{").doesNotContain("deterministic\":"); // isDeterministic ignored
        ReaderHeartbeat back = MAPPER.readValue(json, ReaderHeartbeat.class);
        assertThat(back.arithmetic()).isEqualTo(p);
        assertThat(back.with(true, 2L).arithmetic()).isEqualTo(p);

        ReaderHeartbeat old = new ReaderHeartbeat("r0", "http://r0", List.of("a"), false, null, 0, 0, 1L);
        String oldJson = MAPPER.writeValueAsString(old);
        assertThat(oldJson).doesNotContain("arithmetic");
        assertThat(MAPPER.readValue(oldJson, ReaderHeartbeat.class).arithmetic()).isNull();
    }

    /**
     * The proof behind ruling CPU-R1, in fresh JVMs: the deterministic flags give the same bits whatever this CPU's
     * FMA and SIMD support, and the same canary as recorded on other CPUs. On x86-64 the JVM can be told to act as
     * older hardware (AVX2 only, no FMA, SSE only); elsewhere only the plain deterministic launch runs.
     */
    @Test
    void deterministicFlagsGiveTheSameBitsOnEveryCpuFlavour() throws Exception {
        List<List<String>> flavours = new ArrayList<>();
        flavours.add(List.of());
        flavours.add(List.of("-XX:TieredStopAtLevel=1")); // C1 only
        if ("amd64".equals(System.getProperty("os.arch"))) {
            flavours.add(List.of("-XX:UseAVX=2"));
            flavours.add(List.of("-XX:-UseFMA"));
            flavours.add(List.of("-XX:UseAVX=0"));
        }
        for (List<String> extra : flavours) {
            List<String> cmd = new ArrayList<>(List.of(javaBin(), "-Xmx64m", "-XX:+IgnoreUnrecognizedVMOptions"));
            cmd.addAll(extra);
            cmd.addAll(ArithmeticProfile.DETERMINISTIC_FLAGS);
            cmd.addAll(List.of("-cp", System.getProperty("java.class.path"), PrintProfile.class.getName()));
            String out = run(cmd);
            assertThat(out).as("profile under %s", extra)
                    .isEqualTo("fma=off simd=scalar canary=" + DETERMINISTIC_CANARY + " deterministic=true");
        }
    }

    /** Prints this JVM's profile (run in a fork). */
    public static final class PrintProfile {
        public static void main(String[] args) {
            ArithmeticProfile p = ArithmeticProfile.effective();
            System.out.print(p.summary() + " deterministic=" + p.isDeterministic());
        }
    }

    static String javaBin() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    static String run(List<String> cmd) throws Exception {
        Process proc = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        assertThat(proc.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(proc.exitValue()).as("exit of %s: %s", cmd, out).isZero();
        return out;
    }
}
