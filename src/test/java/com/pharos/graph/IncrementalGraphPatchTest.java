package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An incremental index must keep the graph equal to what a full index would build. Patching an
 * edited class used to DELETE its vertices — which also deleted every call edge from unchanged
 * files into it — and then re-add only the edited file's outgoing calls: after editing B.java,
 * callers(B#b()) lost all callers in other files until the next full index.
 */
class IncrementalGraphPatchTest {

    @TempDir Path tmp;
    private Path root, pkg;
    private IndexConfig config;
    private ProjectIndexManager manager;

    static final class MemRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();
        MemRegistry() { super(IndexConfig.defaults()); }
        @Override public synchronized void register(ProjectMeta m) { store.put(m.getName(), m); }
        @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.ofNullable(store.get(n)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String a, String b) {}
        @Override public synchronized void unregister(String n) { store.remove(n); }
    }

    @BeforeEach
    void setUp() throws Exception {
        root = tmp.resolve("project");
        pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), """
                package com.example;
                public class A { public void a() { new B().b(); } }
                """);
        Files.writeString(pkg.resolve("B.java"), """
                package com.example;
                public class B { public void b() { c(); } public void c() {} }
                """);
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        MemRegistry registry = new MemRegistry();
        ModuleGraphBuilder noop = new ModuleGraphBuilder(registry) {
            @Override public synchronized List<String> incorporate(Path r, ProjectMeta m,
                    com.pharos.parser.MavenPomReader.PomInfo p) { return List.of(); }
        };
        manager = new ProjectIndexManager(config, new LuceneIndexer(config), registry, List.of(), noop,
                List.of(new JavaCodeParser()));
    }

    private Set<String> callers(String fqn) {
        try (CallGraph g = CallGraph.open(config.getIndexDir().resolve("test").resolve("callgraph.arcadedb"))) {
            return g.callers(fqn);
        }
    }

    private Set<String> callees(String fqn) {
        try (CallGraph g = CallGraph.open(config.getIndexDir().resolve("test").resolve("callgraph.arcadedb"))) {
            return g.callees(fqn);
        }
    }

    @Test
    void editingACalleeFile_keepsCallsFromUnchangedFiles() throws Exception {
        manager.index(root, "test", false, false);
        assertThat(callers("com.example.B#b()")).containsExactly("com.example.A#a()");

        Files.writeString(pkg.resolve("B.java"), """
                package com.example;
                public class B { public void b() { c(); } public void c() {} public void d() {} }
                """);
        manager.index(root, "test", true, false);

        assertThat(callers("com.example.B#b()")).as("A.java was not edited").containsExactly("com.example.A#a()");
        assertThat(callers("com.example.B#c()")).containsExactly("com.example.B#b()");
    }

    @Test
    void editedFileOutgoingCallsAreReplaced_andRemovedMethodsDisappear() throws Exception {
        manager.index(root, "test", false, false);

        Files.writeString(pkg.resolve("B.java"), """
                package com.example;
                public class B { public void b() { } }
                """);
        manager.index(root, "test", true, false);

        assertThat(callees("com.example.B#b()")).as("b() no longer calls c()").isEmpty();
        assertThat(callers("com.example.B#c()")).isEmpty();
        try (CallGraph g = CallGraph.open(config.getIndexDir().resolve("test").resolve("callgraph.arcadedb"))) {
            assertThat(g.allFqns()).contains("com.example.A#a()", "com.example.B#b()")
                    .doesNotContain("com.example.B#c()");
        }
        assertThat(callers("com.example.B#b()")).containsExactly("com.example.A#a()");
    }
}
