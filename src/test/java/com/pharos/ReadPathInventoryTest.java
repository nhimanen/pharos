package com.pharos;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the read-site inventory: one read path per role, no bypass of the index format check.
 * <ul>
 *   <li>Graph reads (search, CLI, MCP, web, module boundary) go through {@code GraphViews.open},
 *       which runs {@code IndexFormat.check} and resolves {@code callgraph.current}.</li>
 *   <li>Lucene reads go through {@code LuceneIndexer.openReader}, which runs the same check.</li>
 *   <li>Everything else that opens a graph or a Lucene directory is a writer build path
 *       (indexing, linking, publishing) or the reader's pre-serve integrity check.</li>
 * </ul>
 * A new call site of one of these primitives must be added here deliberately.
 */
class ReadPathInventoryTest {

    private static final Path MAIN = Path.of("src/main/java/com/pharos");

    private static Map<String, Set<String>> sitesOf(String needle) throws IOException {
        Map<String, Set<String>> out = new TreeMap<>();
        try (Stream<Path> s = Files.walk(MAIN)) {
            for (Path p : s.filter(f -> f.toString().endsWith(".java")).toList()) {
                String src = Files.readString(p);
                if (src.contains(needle)) {
                    out.computeIfAbsent(needle, k -> new TreeSet<>()).add(MAIN.relativize(p).toString());
                }
            }
        }
        return out;
    }

    private static Set<String> files(String needle) throws IOException {
        return sitesOf(needle).getOrDefault(needle, Set.of());
    }

    @Test
    void graphsAreOpenedForReadingOnlyThroughGraphViews() throws IOException {
        assertThat(files("CallGraph.openExisting(")).containsExactlyInAnyOrder(
                "graph/GraphViews.java",                          // the read path
                "graph/CrossProjectLinker.java",                  // writer: link (reads project graphs)
                "cluster/objectstore/ObjectStorePublisher.java"); // writer: compact graph snapshot
    }

    @Test
    void graphsAreCreatedOnlyByWriterBuildPaths() throws IOException {
        assertThat(files("CallGraph.open(")).containsExactlyInAnyOrder(
                "graph/CallGraph.java",                 // javadoc example + the factory itself
                "graph/CrossProjectLinker.java",        // the global cross-project graph
                "indexer/ProjectIndexManager.java");    // building/patching a project graph
    }

    @Test
    void indexFormatIsCheckedByTheReadPathsOnly() throws IOException {
        assertThat(files("IndexFormat.check(")).containsExactlyInAnyOrder(
                "graph/GraphViews.java",       // graph read path
                "indexer/LuceneIndexer.java",  // Lucene read path (openReader)
                "search/SearchEngine.java");   // pre-filter: skip refused projects with a warning
    }

    @Test
    void luceneReadersAreOpenedOnlyByLuceneIndexer() throws IOException {
        assertThat(files("DirectoryReader.open(")).containsExactly("indexer/LuceneIndexer.java");
        // LuceneIndexer opens directories through LuceneDirectoryFactory (DEFAULT = FSDirectory.open,
        // or NIOFSDirectory under the -Dpharos.diag.luceneDirectory=niofs diagnostics hook).
        assertThat(files("FSDirectory::open")).isEmpty();
        assertThat(files("FSDirectory.open(")).containsExactlyInAnyOrder(
                "indexer/LuceneDirectoryFactory.java",            // the default factory
                "cluster/ReplicaSyncer.java",                     // reader: verify before serving
                "cluster/objectstore/ObjectStorePublisher.java",  // writer: read the commit to upload
                "cluster/warm/LazyGenerations.java",              // reader factory: full-copy generations
                "cluster/warm/RemoteDirectory.java");             // lazy generation: local eager files, checksums
    }
}
