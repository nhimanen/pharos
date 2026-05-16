package com.pharos.cli;

import com.pharos.graph.CallGraph;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "context",
        description = "One-shot class summary: body, fields, constructors, public methods, and their callers",
        mixinStandardHelpOptions = true
)
public class ClassContextCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Qualified class name: com.example.MyClass")
    private String fqn;

    private final SearchEngine searchEngine;

    public ClassContextCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            SearchEngine.ClassContext ctx = searchEngine.getClassContext(fqn);
            if (ctx == null) {
                System.err.println("Class not found: " + fqn);
                return 1;
            }

            SearchResult cls = ctx.classResult();
            System.out.printf("Class: %s [%s]%n", cls.label(), cls.project());
            System.out.printf("File:  %s:%d-%d%n%n", cls.filePath(), cls.startLine(), cls.endLine());

            if (cls.javadoc() != null && !cls.javadoc().isBlank()) {
                System.out.printf("/** %s */%n%n", cls.javadoc().replaceAll("\\s+", " ").trim());
            }

            if (!ctx.fields().isEmpty()) {
                System.out.printf("Fields (%d):%n", ctx.fields().size());
                for (CallGraph.FieldInfo f : ctx.fields()) {
                    String type = f.fieldType() != null ? f.fieldType() : "?";
                    System.out.printf("  [%s] %s: %s%n", f.accessMod(), f.fieldName(), type);
                }
                System.out.println();
            }

            if (!ctx.constructors().isEmpty()) {
                System.out.printf("Constructors (%d):%n", ctx.constructors().size());
                for (SearchResult c : ctx.constructors()) {
                    System.out.printf("  %s%n", c.signature());
                }
                System.out.println();
            }

            if (!ctx.publicMethods().isEmpty()) {
                System.out.printf("Public methods (%d):%n", ctx.publicMethods().size());
                for (SearchResult m : ctx.publicMethods()) {
                    String mFqn = m.id().substring(m.project().length() + 1);
                    List<String> callers = ctx.publicMethodCallers().getOrDefault(mFqn, List.of());
                    System.out.printf("  %s%n", m.signature());
                    if (!callers.isEmpty()) {
                        callers.forEach(c -> System.out.printf("    ← %s%n", c));
                    }
                }
                System.out.println();
            }

            System.out.println("Body:");
            System.out.println(ctx.body());
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
