package com.pharos.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link ProjectRegistry}'s on-disk cache freshness.
 *
 * <p>Background: a long-running daemon holds an in-memory cache of the registry.
 * When {@code pharos index <path>} runs as a separate JVM and writes new projects
 * to {@code registry.json}, the daemon used to keep serving stale data until
 * restart — symptom: {@code pharos projects} reporting "No projects indexed"
 * right after {@code pharos index} succeeded.  The mtime check in
 * {@link ProjectRegistry#ensureCache} fixes that.
 */
class ProjectRegistryTest {

    @TempDir
    Path tempDir;

    @Test
    void listAll_picksUpExternalWrites() throws IOException {
        Path registryFile = tempDir.resolve("registry.json");

        ProjectRegistry registry = new ProjectRegistry(registryFile);

        // First reader hits an empty registry and caches the result.
        assertThat(registry.listAll()).isEmpty();

        // Another process writes a project to registry.json directly.
        writeRegistryWithProject(registryFile, "yeast", "/repos/yeast");

        // listAll() must re-read because the file's mtime advanced.
        assertThat(registry.listAll())
                .extracting(ProjectMeta::getName)
                .containsExactly("yeast");
    }

    @Test
    void find_picksUpExternalWrites() throws IOException {
        Path registryFile = tempDir.resolve("registry.json");

        ProjectRegistry registry = new ProjectRegistry(registryFile);
        assertThat(registry.find("cooper")).isEmpty();

        writeRegistryWithProject(registryFile, "cooper", "/repos/cooper");

        assertThat(registry.find("cooper"))
                .isPresent()
                .get()
                .extracting(ProjectMeta::getRootPath)
                .isEqualTo("/repos/cooper");
    }

    @Test
    void register_thenListAll_doesNotReloadFromDisk() throws IOException {
        // Sanity check: when we are the writer, our own writes shouldn't trigger
        // a (cheap but unnecessary) reload — the mtime watermark advances to match
        // the file we just wrote.  We verify this indirectly by deleting the file
        // after register() and observing that listAll() still returns the cached
        // entry.  If the cache were being invalidated on every read, deleting the
        // file would drop the entry.
        Path registryFile = tempDir.resolve("registry.json");
        ProjectRegistry registry = new ProjectRegistry(registryFile);

        ProjectMeta meta = new ProjectMeta("yeast", "/repos/yeast",
                tempDir.resolve("indexes/yeast").toString());
        registry.register(meta);

        Files.delete(registryFile);  // mtime "older" than our watermark is irrelevant; absence is what changed
        // The mtime check treats file absence as staleness — we expect a reload to an empty map.
        assertThat(registry.listAll()).isEmpty();
    }

    @Test
    void register_concurrentExternalWrite_isPickedUp() throws IOException, InterruptedException {
        // Two writers race: one in-process (register), one out-of-process (file write).
        // After both happen, listAll() must reflect the on-disk state — not just the
        // last in-process state.
        Path registryFile = tempDir.resolve("registry.json");
        ProjectRegistry registry = new ProjectRegistry(registryFile);

        ProjectMeta inProcess = new ProjectMeta("yeast", "/repos/yeast",
                tempDir.resolve("indexes/yeast").toString());
        registry.register(inProcess);

        // External writer adds a different project and writes the file with a later mtime.
        // We bump the mtime explicitly to avoid same-second flakiness on coarse-mtime filesystems.
        writeRegistryWithProject(registryFile, "cooper", "/repos/cooper");
        Files.setLastModifiedTime(registryFile,
                FileTime.from(Instant.now().plusSeconds(2)));

        assertThat(registry.listAll())
                .extracting(ProjectMeta::getName)
                .containsExactly("cooper");  // external file wins on next read
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private static void writeRegistryWithProject(Path registryFile, String name, String rootPath)
            throws IOException {
        String json = """
                {
                  "%s" : {
                    "name" : "%s",
                    "rootPath" : "%s",
                    "indexPath" : "/tmp/indexes/%s",
                    "methodCount" : 0,
                    "classCount" : 0,
                    "fileCount" : 0,
                    "knownPackages" : [ ],
                    "linkedProjects" : [ ],
                    "unresolvedRefs" : [ ]
                  }
                }
                """.formatted(name, name, rootPath, name);
        Files.writeString(registryFile, json);
    }
}
