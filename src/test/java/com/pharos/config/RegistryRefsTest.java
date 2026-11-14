package com.pharos.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unresolved refs (hundreds of thousands per large project) must stay on disk: find() — called
 * by every search — used to hydrate and cache them, which kept ~180 MB of them in a daemon's heap.
 */
class RegistryRefsTest {

    @TempDir Path tmp;

    private ProjectMeta meta(String name) throws Exception {
        Path idx = Files.createDirectories(tmp.resolve("indexes").resolve(name));
        return new ProjectMeta(name, tmp.resolve("src").toString(), idx.toString());
    }

    private static List<ProjectMeta.UnresolvedRef> refs() {
        return List.of(new ProjectMeta.UnresolvedRef("a.A#x()", "y", 3),
                       new ProjectMeta.UnresolvedRef("a.A#x()", "z", 4));
    }

    @Test
    void findCarriesNoRefs_refsOfReadsThemFromDisk() throws Exception {
        ProjectRegistry registry = new ProjectRegistry(tmp.resolve("registry.json"));
        ProjectMeta m = meta("p");
        m.setUnresolvedRefs(refs());
        registry.register(m);

        ProjectMeta found = registry.find("p").orElseThrow();
        assertThat(found.getUnresolvedRefs()).as("not cached in the registry").isNullOrEmpty();
        assertThat(registry.refsOf(found)).extracting(r -> r.calleeMethodName).containsExactly("y", "z");
        assertThat(registry.find("p").orElseThrow().getUnresolvedRefs()).isNullOrEmpty();
    }

    @Test
    void reRegisteringWithoutRefs_keepsTheRefsFile() throws Exception {
        ProjectRegistry registry = new ProjectRegistry(tmp.resolve("registry.json"));
        ProjectMeta full = meta("p");
        full.setUnresolvedRefs(refs());
        registry.register(full);

        // an incremental run registers a meta without refs
        registry.register(meta("p"));

        assertThat(registry.refsOf(registry.find("p").orElseThrow())).hasSize(2);
    }

    @Test
    void refsOf_prefersRefsAlreadyInMemory() throws Exception {
        ProjectRegistry registry = new ProjectRegistry(tmp.resolve("registry.json"));
        ProjectMeta m = meta("p");
        m.setUnresolvedRefs(refs());
        assertThat(registry.refsOf(m)).isSameAs(m.getUnresolvedRefs());
    }
}
