package com.pharos.parser;

import com.pharos.parser.model.ParsedFile;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

/**
 * One-off probe: does JavaParser successfully parse IndexWriter.java?
 * Run manually: mvn test -Dtest=IndexWriterParseTest
 */
class IndexWriterParseTest {

    static final Path IW_FILE = Path.of(
            "/home/nhimanen/projects/lucene/lucene/lucene/core/src/java/org/apache/lucene/index/IndexWriter.java");

    @Test
    void parseIndexWriter() throws Exception {
        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), 1);

        System.out.println("File size: " + IW_FILE.toFile().length() + " bytes");

        long t0 = System.currentTimeMillis();
        try {
            ParsedFile result = parser.parseFile(IW_FILE, "lucene");
            long ms = System.currentTimeMillis() - t0;
            System.out.printf("SUCCESS in %d ms: %d methods, %d classes%n",
                    ms, result.methods().size(), result.classes().size());
            result.classes().forEach(c ->
                    System.out.println("  class: " + c.className() + " (" + c.qualifiedClassName() + ")"));
        } catch (Exception e) {
            long ms = System.currentTimeMillis() - t0;
            System.out.printf("FAILED after %d ms: %s: %s%n", ms, e.getClass().getSimpleName(), e.getMessage());
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("  caused by: " + cause.getClass().getSimpleName() + ": " + cause.getMessage());
                cause = cause.getCause();
            }
        }
    }
}
