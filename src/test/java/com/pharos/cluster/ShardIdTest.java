package com.pharos.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ShardIdTest {

    @Test
    void acceptsValidNames() {
        assertThat(ShardId.ofProject("pharos").value()).isEqualTo("pharos");
        assertThat(new ShardId("my-own-gpt").value()).isEqualTo("my-own-gpt");
        assertThat(new ShardId("a.b_c").value()).isEqualTo("a.b_c");
    }

    @Test
    void rejectsInvalidNames() {
        for (String bad : new String[]{"", "my project", "a/b", "..", "x..y", "a".repeat(129)}) {
            assertThatThrownBy(() -> new ShardId(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("project name '" + bad
                            + "' cannot be a shard id: use letters, digits, '.', '_' or '-'");
        }
    }

    @Test
    void generationOrdering() {
        assertThat(Generation.FIRST.next()).isEqualTo(new Generation(2));
        assertThat(Generation.FIRST).isLessThan(Generation.FIRST.next());
    }
}
