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
}
