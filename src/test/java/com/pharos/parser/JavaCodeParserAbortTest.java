package com.pharos.parser;

import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.IndexAbortedException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Gate run 2 for 1.9.0: a FULL run's parse is one {@code parseFiles} call over every file of the
 * parser (minutes for lucene on a loaded host), and the run's abort was checked only around it, so
 * a graceful stop or a lost lease could not land until the parser's last file.
 */
class JavaCodeParserAbortTest {

    @TempDir Path tmp;

    private List<Path> files(int n) throws IOException {
        Path pkg = tmp.resolve("src/main/java/com/example");
        Files.createDirectories(pkg);
        List<Path> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Path f = pkg.resolve("C" + i + ".java");
            Files.writeString(f, "package com.example;\npublic class C" + i + " { int m() { return " + i + "; } }\n");
            out.add(f);
        }
        return out;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void abortRequestedMidParseStopsBetweenFiles(int threads) throws IOException {
        List<Path> files = files(200);
        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), threads);
        IndexAbort abort = new IndexAbort();
        AtomicInteger seen = new AtomicInteger();
        parser.beforeFile = f -> {
            if (seen.incrementAndGet() == 5) abort.request("test");
        };

        assertThatThrownBy(() -> parser.parseFiles(files, tmp, "p", abort))
                .isInstanceOf(IndexAbortedException.class);
        assertThat(seen.get()).as("files started after the abort").isLessThan(5 + 2 * threads);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void withoutAnAbortEveryFileIsParsed(int threads) throws IOException {
        List<Path> files = files(20);
        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), threads);
        assertThat(parser.parseFiles(files, tmp, "p", IndexAbort.none()).files()).hasSize(20);
        assertThat(parser.parseFiles(files, tmp, "p").files()).hasSize(20);
    }
}
