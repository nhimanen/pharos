package com.pharos.indexer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class FileStateTrackerTest {

    @TempDir
    Path indexDir;

    @TempDir
    Path sourceDir;

    // --- isDirty ---

    @Test
    void isDirty_newFile_isTrue() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");

        assertThat(tracker.isDirty(file)).isTrue();
    }

    @Test
    void isDirty_afterTrack_isFalse() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");

        tracker.track(file);

        assertThat(tracker.isDirty(file)).isFalse();
    }

    @Test
    void isDirty_afterContentChange_isTrue() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker tracker = new FileStateTracker(indexDir);
        tracker.track(file);

        // Change file content — SHA-256 will differ
        Thread.sleep(10); // ensure mtime differs
        Files.writeString(file, "class A { void changed() {} }");

        assertThat(tracker.isDirty(file)).isTrue();
    }

    // --- track ---

    @Test
    void track_recordsFileState() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path file = Files.writeString(sourceDir.resolve("B.java"), "class B {}");

        tracker.track(file);

        assertThat(tracker.isDirty(file)).isFalse();
    }

    // --- remove ---

    @Test
    void remove_untracksFile() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path file = Files.writeString(sourceDir.resolve("C.java"), "class C {}");
        tracker.track(file);
        assertThat(tracker.isDirty(file)).isFalse();

        tracker.remove(file);

        // After remove, the file appears "new" again → isDirty = true
        assertThat(tracker.isDirty(file)).isTrue();
    }

    @Test
    void remove_isNoOpForUntrackedFile() {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path ghost = sourceDir.resolve("Ghost.java");

        assertThatCode(() -> tracker.remove(ghost)).doesNotThrowAnyException();
    }

    // --- trackedFiles ---

    @Test
    void trackedFiles_returnsAllTrackedPaths() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path a = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        Path b = Files.writeString(sourceDir.resolve("B.java"), "class B {}");
        tracker.track(a);
        tracker.track(b);

        Set<Path> tracked = tracker.trackedFiles();

        assertThat(tracked).containsExactlyInAnyOrder(
                a.toAbsolutePath(), b.toAbsolutePath());
    }

    @Test
    void trackedFiles_emptyWhenNothingTracked() {
        FileStateTracker tracker = new FileStateTracker(indexDir);

        assertThat(tracker.trackedFiles()).isEmpty();
    }

    @Test
    void trackedFiles_doesNotContainRemovedFile() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path a = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        tracker.track(a);
        tracker.remove(a);

        assertThat(tracker.trackedFiles()).doesNotContain(a.toAbsolutePath());
    }

    // --- clear ---

    @Test
    void clear_removesAllTrackedState() throws Exception {
        FileStateTracker tracker = new FileStateTracker(indexDir);
        Path a = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        tracker.track(a);
        tracker.clear();

        assertThat(tracker.trackedFiles()).isEmpty();
        assertThat(tracker.isDirty(a)).isTrue();
    }

    // --- save + reload ---

    @Test
    void saveAndReload_persistsTrackedState() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("D.java"), "class D {}");

        FileStateTracker tracker1 = new FileStateTracker(indexDir);
        tracker1.track(file);
        tracker1.save();

        // New tracker instance — loads from disk
        FileStateTracker tracker2 = new FileStateTracker(indexDir);

        assertThat(tracker2.isDirty(file)).isFalse();
        assertThat(tracker2.trackedFiles()).contains(file.toAbsolutePath());
    }

    @Test
    void saveAndReload_removedFileNotPresent() throws Exception {
        Path a = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        Path b = Files.writeString(sourceDir.resolve("B.java"), "class B {}");

        FileStateTracker tracker1 = new FileStateTracker(indexDir);
        tracker1.track(a);
        tracker1.track(b);
        tracker1.remove(a);
        tracker1.save();

        FileStateTracker tracker2 = new FileStateTracker(indexDir);

        assertThat(tracker2.trackedFiles()).containsExactly(b.toAbsolutePath());
    }

    // --- hasOutdatedEmbeddings ---

    @Test
    void hasOutdatedEmbeddings_emptyTracker_returnsFalse() {
        FileStateTracker tracker = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(tracker.hasOutdatedEmbeddings()).isFalse();
    }

    @Test
    void hasOutdatedEmbeddings_afterTrackWithCurrentVersions_returnsFalse() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker tracker = new FileStateTracker(indexDir, 1, "model:4");
        tracker.track(file);

        assertThat(tracker.hasOutdatedEmbeddings()).isFalse();
    }

    @Test
    void hasOutdatedEmbeddings_savedWithOldChunkingVersion_returnsTrue() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        // Track file with old version (simulates state from before a CHUNKING_VERSION bump)
        FileStateTracker old = new FileStateTracker(indexDir, 0, "model:4");
        old.track(file);
        old.save();

        // New tracker with version 1 — should see the saved state as outdated
        FileStateTracker current = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(current.hasOutdatedEmbeddings()).isTrue();
    }

    @Test
    void hasOutdatedEmbeddings_savedWithDifferentModelFingerprint_returnsTrue() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker old = new FileStateTracker(indexDir, 1, "model-v1:4");
        old.track(file);
        old.save();

        FileStateTracker current = new FileStateTracker(indexDir, 1, "model-v2:4");
        assertThat(current.hasOutdatedEmbeddings()).isTrue();
    }

    @Test
    void hasOutdatedEmbeddings_currentFingerprintNull_skipsModelCheck() throws Exception {
        // Construct saved state with some non-null fingerprint
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker old = new FileStateTracker(indexDir, 1, "some-model:4");
        old.track(file);
        old.save();

        // Tracker constructed with null fingerprint (no embedding model configured)
        FileStateTracker noModel = new FileStateTracker(indexDir, 1, null);
        // chunkingVersion matches; model check skipped → not outdated
        assertThat(noModel.hasOutdatedEmbeddings()).isFalse();
    }

    @Test
    void hasOutdatedEmbeddings_mixedVersions_returnsTrueIfAnyFileOutdated() throws Exception {
        Path a = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        Path b = Files.writeString(sourceDir.resolve("B.java"), "class B {}");

        // Track A with current version, B with old version
        FileStateTracker setup = new FileStateTracker(indexDir, 1, "model:4");
        setup.track(a);
        setup.save();

        FileStateTracker setupOld = new FileStateTracker(indexDir, 0, "model:4");
        setupOld.track(b);
        setupOld.save();

        FileStateTracker current = new FileStateTracker(indexDir, 1, "model:4");
        // B's chunkingVersion (0) != 1 → outdated
        assertThat(current.hasOutdatedEmbeddings()).isTrue();
    }

    // --- version fields persisted ---

    @Test
    void saveAndReload_persistsVersionFields() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker tracker = new FileStateTracker(indexDir, 7, "special-model:16");
        tracker.track(file);
        tracker.save();

        // Tracker with matching versions → not outdated (proves fields were persisted)
        FileStateTracker reloaded = new FileStateTracker(indexDir, 7, "special-model:16");
        assertThat(reloaded.hasOutdatedEmbeddings()).isFalse();

        // Tracker with different version → outdated (confirms it's actually reading the saved value)
        FileStateTracker mismatch = new FileStateTracker(indexDir, 8, "special-model:16");
        assertThat(mismatch.hasOutdatedEmbeddings()).isTrue();
    }

    // --- isEmbeddingOutdated (per-file) ---

    @Test
    void isEmbeddingOutdated_untrackedFile_returnsTrue() {
        FileStateTracker tracker = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(tracker.isEmbeddingOutdated(sourceDir.resolve("Unknown.java"))).isTrue();
    }

    @Test
    void isEmbeddingOutdated_currentVersion_returnsFalse() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker tracker = new FileStateTracker(indexDir, 1, "model:4");
        tracker.track(file);
        assertThat(tracker.isEmbeddingOutdated(file)).isFalse();
    }

    @Test
    void isEmbeddingOutdated_oldChunkingVersion_returnsTrue() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        FileStateTracker old = new FileStateTracker(indexDir, 0, "model:4");
        old.track(file);
        old.save();

        FileStateTracker current = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(current.isEmbeddingOutdated(file)).isTrue();
    }

    @Test
    void isEmbeddingOutdated_perFile_onlyAffectsFilesWithStaleVersion() throws Exception {
        Path stale = Files.writeString(sourceDir.resolve("Stale.java"), "class Stale {}");
        Path fresh = Files.writeString(sourceDir.resolve("Fresh.java"), "class Fresh {}");

        // Track stale with old version, fresh with current version
        FileStateTracker oldTracker = new FileStateTracker(indexDir, 0, "model:4");
        oldTracker.track(stale);
        oldTracker.save();

        FileStateTracker currentTracker = new FileStateTracker(indexDir, 1, "model:4");
        currentTracker.track(fresh);
        currentTracker.save();

        FileStateTracker checker = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(checker.isEmbeddingOutdated(stale)).isTrue();
        assertThat(checker.isEmbeddingOutdated(fresh)).isFalse();
    }

    @Test
    void backwardsCompat_legacyStateWithoutVersionFields_treatedAsOutdated() throws Exception {
        Path file = Files.writeString(sourceDir.resolve("A.java"), "class A {}");
        // Inject a legacy state file — no chunkingVersion or modelFingerprint fields.
        // @JsonIgnoreProperties(ignoreUnknown = true) means missing fields default to 0/null.
        Path stateFile = indexDir.resolve("file-state.json");
        String legacyJson = "{\"" + file.toAbsolutePath() + "\":"
                + "{\"lastModifiedMs\":1000,\"sha256\":\"deadbeef\",\"classNames\":[]}}";
        Files.writeString(stateFile, legacyJson);

        // Any non-zero CHUNKING_VERSION will see chunkingVersion=0 as outdated
        FileStateTracker tracker = new FileStateTracker(indexDir, 1, "model:4");
        assertThat(tracker.hasOutdatedEmbeddings()).isTrue();
    }
}
