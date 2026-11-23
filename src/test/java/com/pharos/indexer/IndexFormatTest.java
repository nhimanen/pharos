package com.pharos.indexer;

import com.pharos.PharosVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexFormatTest {

    @TempDir Path dir;

    private void stamp(int format) throws Exception {
        Files.writeString(dir.resolve("index-format.json"),
                "{\"format\":" + format + ",\"pharosVersion\":\"0.9.0\",\"writtenAt\":\"2026-01-01T00:00:00Z\"}");
    }

    @Test
    void missingStampIsFormatZero() {
        IndexFormat.Stamp s = IndexFormat.read(dir);
        assertThat(s.format()).isZero();
        assertThat(s.pharosVersion()).isEqualTo("unknown");
        assertThat(s.writtenAt()).isNull();
        // Format 0 is the unstamped layout: servable while it is N-1, refused once it is older.
        assertThat(IndexFormat.status(dir)).isEqualTo(IndexFormat.MIN_READABLE_FORMAT <= 0
                ? IndexFormat.Status.UPGRADE_AVAILABLE : IndexFormat.Status.OUTDATED);
    }

    @Test
    void writeThenReadRoundTrips() {
        IndexFormat.write(dir);
        IndexFormat.Stamp s = IndexFormat.read(dir);
        assertThat(s.format()).isEqualTo(IndexVersions.INDEX_FORMAT);
        assertThat(s.pharosVersion()).isEqualTo(PharosVersion.get());
        assertThat(Duration.between(s.writtenAt(), Instant.now()).abs()).isLessThan(Duration.ofSeconds(5));
        assertThat(IndexFormat.status(dir)).isEqualTo(IndexFormat.Status.OK);
    }

    @Test
    void olderStampIsOutdatedWithFixInMessage() throws Exception {
        stamp(IndexVersions.INDEX_FORMAT - 2);
        assertThatThrownBy(() -> IndexFormat.check("p", dir))
                .isInstanceOf(IncompatibleIndexException.class)
                .hasMessageContaining("pharos index --full p")
                .hasMessageContaining("./setup.sh --upgrade-indexes")
                .hasMessageContaining("0.9.0");
    }

    @Test
    void previousFormatIsUpgradeAvailableAndServable() throws Exception {
        assertThat(IndexFormat.MIN_READABLE_FORMAT).isEqualTo(IndexVersions.INDEX_FORMAT - 1);
        stamp(IndexVersions.INDEX_FORMAT - 1);
        assertThat(IndexFormat.status(dir)).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
        assertThat(IndexFormat.servable(IndexFormat.Status.UPGRADE_AVAILABLE)).isTrue();
        assertThat(IndexFormat.Status.UPGRADE_AVAILABLE.wireName()).isEqualTo("upgrade-available");
        assertThatCode(() -> IndexFormat.check("p", dir)).doesNotThrowAnyException();
    }

    @Test
    void twoBehindIsOutdatedAndRefused() throws Exception {
        stamp(IndexVersions.INDEX_FORMAT - 2);
        assertThat(IndexFormat.status(dir)).isEqualTo(IndexFormat.Status.OUTDATED);
        assertThat(IndexFormat.servable(IndexFormat.Status.OUTDATED)).isFalse();
        assertThat(IndexFormat.servable(IndexFormat.Status.NEWER)).isFalse();
        assertThat(IndexFormat.servable(IndexFormat.Status.OK)).isTrue();
        assertThatThrownBy(() -> IndexFormat.check("p", dir))
                .isInstanceOf(IncompatibleIndexException.class);
    }

    @Test
    void newerStampSaysNewer() throws Exception {
        stamp(IndexVersions.INDEX_FORMAT + 1);
        assertThat(IndexFormat.status(dir)).isEqualTo(IndexFormat.Status.NEWER);
        assertThatThrownBy(() -> IndexFormat.check("p", dir))
                .isInstanceOf(IncompatibleIndexException.class)
                .hasMessageContaining("newer");
    }

    @Test
    void rewrittenStampClearsCachedFailure() throws Exception {
        stamp(IndexVersions.INDEX_FORMAT - 2);
        assertThatThrownBy(() -> IndexFormat.check("p", dir)).isInstanceOf(IncompatibleIndexException.class);
        IndexFormat.write(dir);
        Path f = dir.resolve("index-format.json");
        Files.setLastModifiedTime(f, FileTime.from(Instant.now().plusSeconds(60)));
        assertThatCode(() -> IndexFormat.check("p", dir)).doesNotThrowAnyException();
    }

    @Test
    void missingDirIsMissing() {
        assertThat(IndexFormat.status(dir.resolve("nope"))).isEqualTo(IndexFormat.Status.MISSING);
        assertThatCode(() -> IndexFormat.check("p", dir.resolve("nope"))).doesNotThrowAnyException();
    }

    // ── Reader generation dirs: the fix is on the reader or the writer, never a local re-index ──

    private Path readerGeneration(int format) throws Exception {
        Path gen = Files.createDirectories(dir.resolve("cache").resolve("p").resolve("gen-7"));
        Files.writeString(gen.resolve("manifest.json"), "{}");
        Files.writeString(gen.resolve("index-format.json"),
                "{\"format\":" + format + ",\"pharosVersion\":\"9.9.9\",\"writtenAt\":\"2026-01-01T00:00:00Z\"}");
        return gen;
    }

    @Test
    void newerGenerationOnReaderSaysUpgradeThisReader() throws Exception {
        Path gen = readerGeneration(IndexVersions.INDEX_FORMAT + 1);
        assertThatThrownBy(() -> IndexFormat.check("p", gen))
                .isInstanceOf(IncompatibleIndexException.class)
                .hasMessageContaining("generation gen-7")
                .hasMessageContaining("Upgrade pharos on this reader")
                .hasMessageNotContaining("setup.sh --upgrade-indexes")
                .hasMessageNotContaining("Re-index with this version");
    }

    @Test
    void outdatedGenerationOnReaderSaysRepublishFromTheWriter() throws Exception {
        Path gen = readerGeneration(IndexVersions.INDEX_FORMAT - 2);
        assertThatThrownBy(() -> IndexFormat.check("p", gen))
                .isInstanceOf(IncompatibleIndexException.class)
                .hasMessageContaining("Re-publish it from an upgraded writer")
                .hasMessageContaining("on the writer: pharos index --full p")
                .hasMessageNotContaining("setup.sh --upgrade-indexes");
    }

    @Test
    void writerDirNamedLikeAGenerationKeepsTheWriterHint() throws Exception {
        Path gen = Files.createDirectories(dir.resolve("gen-7")); // no manifest.json: not a reader dir
        Files.writeString(gen.resolve("index-format.json"),
                "{\"format\":" + (IndexVersions.INDEX_FORMAT - 2) + ",\"pharosVersion\":\"0.9.0\"}");
        assertThatThrownBy(() -> IndexFormat.check("p", gen))
                .hasMessageContaining("./setup.sh --upgrade-indexes");
    }
}
