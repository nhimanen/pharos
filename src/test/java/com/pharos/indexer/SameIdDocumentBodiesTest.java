package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.CodeParser;
import com.pharos.parser.GenericFileParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.PythonCodeParser;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ruling P6-R22 (a), the root cause of the Phase 6 gate's keyword drift: lucene's
 * {@code dev-tools/scripts/releaseWizard.py} and {@code releaseWizard.yaml} yield documents with one id
 * ({@code lucene:dev-tools.scripts.releaseWizard}, a module class and a document). A full run cached the
 * synthesized body by qualified name, so both documents got the body of whichever file came first (the
 * Python module's: the YAML document lost its 74 KB of text), and a reshard, which parses the moved files in
 * another order, gave both the YAML text instead: the split project's BM25 field lengths differed from the
 * unsplit one's (+16 792 body terms) and every score drifted by up to 1.85e-3. Each document now keeps its
 * own file's body.
 */
class SameIdDocumentBodiesTest {

    @TempDir Path tmp;

    private static boolean hasPython() {
        try {
            return new ProcessBuilder("python3", "--version").redirectErrorStream(true).start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void aModuleAndADocumentWithOneIdKeepTheirOwnBodies() throws Exception {
        Assumptions.assumeTrue(hasPython(), "python3 is needed for the Python parser");
        Path root = tmp.resolve("project");
        Path scripts = Files.createDirectories(root.resolve("dev-tools/scripts"));
        Files.writeString(scripts.resolve("wizard.py"), "def build_wizard():\n    \"\"\"Builds the wizard.\"\"\"\n    return 1\n");
        StringBuilder yaml = new StringBuilder();
        for (int i = 0; i < 50; i++) yaml.append("step").append(i).append(": release candidate vote mirror\n");
        Files.writeString(scripts.resolve("wizard.yaml"), yaml.toString());

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        SelectiveIndexUpdateTest.TestRegistry registry = new SelectiveIndexUpdateTest.TestRegistry();
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        List<CodeParser> parsers = List.of(new JavaCodeParser(), new PythonCodeParser(),
                new GenericFileParser(1, config.maxFileSizeBytes(true)));
        new ProjectIndexManager(config, lucene, registry, List.of(), noopGraph, parsers).index(root, "p", false, false);

        Map<String, String> bodyByFile = new HashMap<>();
        Map<String, String> idByFile = new HashMap<>();
        try (DirectoryReader r = DirectoryReader.open(new NIOFSDirectory(lucene.getLucenePath("p")))) {
            for (LeafReaderContext ctx : r.leaves()) {
                var sf = ctx.reader().storedFields();
                for (int d = 0; d < ctx.reader().maxDoc(); d++) {
                    var doc = sf.document(d, Set.of(DocumentMapper.F_FILE_PATH, DocumentMapper.F_ID, "body", "docType"));
                    if (!"class".equals(doc.get("docType")) && !"doc".equals(doc.get("docType"))) continue;
                    String file = Path.of(doc.get(DocumentMapper.F_FILE_PATH)).getFileName().toString();
                    bodyByFile.put(file, doc.get("body"));
                    idByFile.put(file, doc.get(DocumentMapper.F_ID));
                }
            }
        }
        assertThat(idByFile.get("wizard.py")).as("the fixture's two documents share one id")
                .isEqualTo(idByFile.get("wizard.yaml"));
        assertThat(bodyByFile.get("wizard.yaml")).as("the YAML document's own text").contains("step49: release candidate");
        assertThat(bodyByFile.get("wizard.py")).as("the module's synthesized body").doesNotContain("step49")
                .contains("build_wizard");
        lucene.close();
    }
}
