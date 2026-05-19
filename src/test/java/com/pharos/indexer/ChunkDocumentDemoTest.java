package com.pharos.indexer;

import com.pharos.parser.GenericFileParser;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

/**
 * Manual demo: prints how a given file is chunked by {@link DefaultChunker#chunkDocument}.
 *
 * Disabled by default — enable to inspect chunking output during development.
 * Run with: mvn test -Dtest=ChunkDocumentDemoTest
 */
@Disabled("manual demo — enable to inspect chunking output")
class ChunkDocumentDemoTest {

    @Test
    void printChunks_pharosReadme() throws Exception {
        printChunks(Path.of("README.md"), "pharos");
    }

    @Test
    void printChunks_customPath() throws Exception {
        // Edit this path to inspect any other file
        printChunks(Path.of("README.md"), "pharos");
    }

    // -------------------------------------------------------------------------

    private static void printChunks(Path file, String project) throws Exception {
        GenericFileParser parser = new GenericFileParser();
        ParsedFile parsed = parser.parseFile(file.toAbsolutePath(), project);
        ParsedClass cls = parsed.classes().get(0);

        String content = DocumentMapper.readBodyFromFile(
                cls.filePath(), cls.startLine(), cls.endLine());
        DefaultChunker chunker = new DefaultChunker();
        List<Chunk> chunks = chunker.chunkDocument(cls, content);

        System.out.printf("%n=== %s → %d chunk(s) ===%n", file.getFileName(), chunks.size());
        System.out.printf("Kind: %s  Lines: %d–%d  Chars: %d%n",
                cls.kind(), cls.startLine(), cls.endLine(), content.length());
        System.out.printf("Javadoc: %s%n%n", cls.javadoc());

        for (int i = 0; i < chunks.size(); i++) {
            Chunk c = chunks.get(i);
            System.out.printf("──── Chunk %d/%d  (lines %d–%d, %d chars) ───────────────────────%n",
                    i + 1, chunks.size(), c.startLine(), c.endLine(), c.text().length());
            System.out.println(c.text());
            System.out.println();
        }
    }
}
