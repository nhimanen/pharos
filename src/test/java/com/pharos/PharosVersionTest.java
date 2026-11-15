package com.pharos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PharosVersionTest {
    @Test
    void versionIsFilteredFromPom() {
        assertThat(PharosVersion.get()).matches("\\d+\\.\\d+\\.\\d+.*");
    }
}
