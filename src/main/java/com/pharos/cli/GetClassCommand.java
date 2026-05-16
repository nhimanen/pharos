package com.pharos.cli;

import com.pharos.graph.CallGraph;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import com.pharos.search.SourceReader;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "class",
        description = "Retrieve a class by qualified name. Use --context for full context: fields, constructors, public methods and their callers.",
        mixinStandardHelpOptions = true
)
public class GetClassCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Qualified class name: com.example.MyClass")
    private String fqn;

    @Option(names = {"--context", "-c"},
            description = "Include fields, constructors (injected dependencies), public methods, and their direct callers")
    private boolean context = false;

    private final SearchEngine searchEngine;

    public GetClassCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            if (context) {
                return printContext();
            }
            return printBody();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private int printBody() throws Exception {
        SearchResult r = searchEngine.getClassByFqn(fqn);
        if (r == null) { System.out.println("Class not found: " + fqn); return 1; }
        System.out.printf("%s [%s]%n", r.qualifiedClassName(), r.project());
        System.out.printf("%s:%d-%d%n%n", r.filePath(), r.startLine(), r.endLine());
        String body = SourceReader.readRange(r.filePath(), r.startLine(), r.endLine());
        if (body != null) {
            System.out.println(body);
        } else {
            if (r.javadoc() != null && !r.javadoc().isBlank())
                System.out.println("/** " + r.javadoc() + " */");
            System.out.println("(source file unavailable; showing indexed body)");
            if (r.body() != null) System.out.println(r.body());
        }
        return 0;
    }

    private int printContext() throws Exception {
        SearchEngine.ClassContext ctx = searchEngine.getClassContext(fqn);
        if (ctx == null) { System.err.println("Class not found: " + fqn); return 1; }

        SearchResult cls = ctx.classResult();
        System.out.printf("Class: %s [%s]%n", cls.label(), cls.project());
        System.out.printf("File:  %s:%d-%d%n%n", cls.filePath(), cls.startLine(), cls.endLine());

        if (cls.javadoc() != null && !cls.javadoc().isBlank())
            System.out.printf("/** %s */%n%n", cls.javadoc().replaceAll("\\s+", " ").trim());

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
            for (SearchResult c : ctx.constructors())
                System.out.printf("  %s%n", c.signature());
            System.out.println();
        }

        if (!ctx.publicMethods().isEmpty()) {
            System.out.printf("Public methods (%d):%n", ctx.publicMethods().size());
            for (SearchResult m : ctx.publicMethods()) {
                String mFqn = m.id().substring(m.project().length() + 1);
                List<String> callers = ctx.publicMethodCallers().getOrDefault(mFqn, List.of());
                System.out.printf("  %s%n", m.signature());
                callers.forEach(c -> System.out.printf("    ← %s%n", c));
            }
            System.out.println();
        }

        System.out.println("Body:");
        System.out.println(ctx.body());
        return 0;
    }
}
