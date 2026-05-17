package com.pharos.indexer;

import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Diagnostic test: parse the local lucene checkout, print body-length statistics,
 * then try to chunk every method with DefaultChunker to find where it OOMs or hangs.
 *
 * Run manually against a real lucene checkout:
 *   mvn test -Dtest=LuceneChunkingDiagnosticTest -DLUCENE_PATH=/home/nhimanen/projects/lucene
 *
 * The test is skipped automatically when no lucene path is configured.
 */
class LuceneChunkingDiagnosticTest {

    private static final Path LUCENE_ROOT = Path.of(
            System.getProperty("LUCENE_PATH",
                    System.getenv() != null && System.getenv("LUCENE_PATH") != null
                            ? System.getenv("LUCENE_PATH")
                            : "/home/nhimanen/projects/lucene"));

    @Test
    void diagnoseMethodBodySizesAndChunking() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source not found at " + LUCENE_ROOT + " — set -DLUCENE_PATH=<path>");

        System.out.println("=== Parsing lucene from: " + LUCENE_ROOT + " ===");
        JavaCodeParser parser = new JavaCodeParser();
        ParsedProject project = parser.parseProject(LUCENE_ROOT, "lucene");

        List<ParsedMethod> methods = project.allMethods();
        System.out.printf("Total methods parsed: %,d%n", methods.size());

        // --- Body length statistics ---
        List<Integer> bodySizes = methods.stream()
                .map(m -> m.body() != null ? m.body().length() : 0)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

        System.out.println("\n--- Body length distribution ---");
        printPercentile(bodySizes, 50);
        printPercentile(bodySizes, 90);
        printPercentile(bodySizes, 95);
        printPercentile(bodySizes, 99);
        printPercentile(bodySizes, 99.9);

        long over8k   = bodySizes.stream().filter(s -> s > 8_000).count();
        long over32k  = bodySizes.stream().filter(s -> s > 32_000).count();
        long over100k = bodySizes.stream().filter(s -> s > 100_000).count();
        System.out.printf("Methods > 8 000 chars:   %,d%n", over8k);
        System.out.printf("Methods > 32 000 chars:  %,d%n", over32k);
        System.out.printf("Methods > 100 000 chars: %,d%n", over100k);

        // Top-10 largest methods
        System.out.println("\n--- Top 10 largest method bodies ---");
        methods.stream()
                .filter(m -> m.body() != null)
                .sorted(Comparator.comparingInt(m -> -m.body().length()))
                .limit(10)
                .forEach(m -> System.out.printf("  %,7d chars  %s#%s  %s:%d%n",
                        m.body().length(),
                        m.qualifiedClassName(), m.methodName(),
                        m.filePath(), m.startLine()));

        // --- Chunking: try to chunk every method, report where it gets expensive ---
        System.out.println("\n--- Chunking all methods (reporting progress every 5000) ---");
        DefaultChunker chunker = new DefaultChunker();
        Runtime rt = Runtime.getRuntime();

        int processed = 0;
        int failedAtMethod = -1;
        String failedFqn = null;

        // Sort by body length descending so we hit large methods early
        List<ParsedMethod> sorted = methods.stream()
                .filter(m -> m.body() != null && !m.body().isBlank())
                .sorted(Comparator.comparingInt(m -> -m.body().length()))
                .collect(Collectors.toList());

        System.out.printf("Processing %,d methods (largest first)...%n", sorted.size());

        for (ParsedMethod m : sorted) {
            long usedBefore = rt.totalMemory() - rt.freeMemory();
            try {
                List<Chunk> chunks = chunker.chunkMethod(m, true);
                long usedAfter = rt.totalMemory() - rt.freeMemory();
                long delta = usedAfter - usedBefore;

                if (processed < 20 || processed % 1000 == 0) {
                    System.out.printf("  [%,5d] body=%,7d  chunks=%d  heap_used=%,d MB  delta=%+,d KB  %s#%s%n",
                            processed, m.body().length(), chunks.size(),
                            usedAfter / (1024 * 1024), delta / 1024,
                            m.qualifiedClassName(), m.methodName());
                }
            } catch (OutOfMemoryError oom) {
                failedAtMethod = processed;
                failedFqn = m.qualifiedClassName() + "#" + m.methodName();
                System.out.printf("%n!!! OOM at method %d: %s (body=%,d chars)%n",
                        processed, failedFqn, m.body().length());
                break;
            }
            processed++;
        }

        System.out.printf("%nChunking complete: %,d methods processed%n", processed);
        if (failedAtMethod >= 0) {
            System.out.printf("OOM triggered at method %d: %s%n", failedAtMethod, failedFqn);
        } else {
            System.out.println("No OOM — all methods chunked successfully.");
        }
    }

    private static void printPercentile(List<Integer> sorted, double pct) {
        int idx = (int) Math.min(sorted.size() - 1, (sorted.size() * (1 - pct / 100)));
        System.out.printf("  p%.1f: %,d chars%n", pct, sorted.get(idx));
    }
}
