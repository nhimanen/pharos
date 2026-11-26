package com.pharos.cluster.membership;

import com.pharos.cluster.ArithmeticProfile;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ArithmeticDivergenceTest {

    private static final ArithmeticProfile DET = new ArithmeticProfile(false, false, 0, "b10a7e42", "deterministic");
    private static final ArithmeticProfile AVX512 = new ArithmeticProfile(true, true, 512, "772d9962", null);

    private static ReaderHeartbeat hb(String node, ArithmeticProfile p) {
        return new ReaderHeartbeat(node, "http://" + node, List.of("a"), false, null, 0, 0, 1L,
                ReaderHeartbeat.READY, List.of(), null, null, Boolean.TRUE, null, null, null, p);
    }

    @Test
    void groupsReadersByProfileLeavingOutOlderOnes() {
        var groups = ArithmeticDivergence.groups(List.of(hb("r2", DET), hb("r1", DET), hb("r3", AVX512), hb("old", null)));
        assertThat(groups).containsExactly(
                Map.entry("fma=off simd=scalar canary=b10a7e42", List.of("r1", "r2")),
                Map.entry("fma=on simd=512 canary=772d9962", List.of("r3")));
    }

    @Test
    void describeNamesNodesProfilesAndTheFix() {
        String msg = ArithmeticDivergence.describe(ArithmeticDivergence.groups(List.of(hb("r1", DET), hb("r3", AVX512))));
        assertThat(msg).contains("[r1] fma=off simd=scalar canary=b10a7e42; [r3] fma=on simd=512 canary=772d9962.")
                .contains("PHAROS_ARITHMETIC=deterministic")
                .contains("-Dlucene.useScalarFMA=false -Dlucene.useVectorFMA=false, without --add-modules jdk.incubator.vector");
    }

    @Test
    void warnsOncePerDistinctSetOfProfiles() {
        ArithmeticDivergence d = new ArithmeticDivergence();
        assertThat(d.observe(List.of(hb("r1", DET), hb("r2", DET), hb("old", null)))).isFalse(); // all the same
        assertThat(d.observe(List.of())).isFalse();
        List<ReaderHeartbeat> mixed = List.of(hb("r1", DET), hb("r3", AVX512));
        assertThat(d.observe(mixed)).isTrue();
        assertThat(d.observe(mixed)).isFalse(); // every heartbeat after: still the same mix, no new WARN
        assertThat(d.observe(List.of(hb("r1", DET)))).isFalse(); // r3 fixed
        assertThat(d.observe(mixed)).isTrue(); // and broken again
    }
}
