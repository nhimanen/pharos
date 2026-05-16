package com.pharos.cli;

import com.pharos.search.SearchEngine;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "usages",
        description = "Find all usages of a method, class, field, or annotation in the knowledge graph",
        mixinStandardHelpOptions = true
)
public class UsagesCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "FQN to look up: com.example.MyClass, com.example.MyClass#myMethod(), com.example.MyClass#fieldName")
    private String fqn;

    @Option(names = {"--kind", "-k"},
            description = "all | callers | subclasses | field_readers | field_writers | annotated | type_refs (default: all)",
            defaultValue = "all")
    private String kind;

    private final SearchEngine searchEngine;

    public UsagesCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            SearchEngine.UsageResult u = searchEngine.findUsages(fqn, kind);
            System.out.printf("Usages of: %s%n%n", u.fqn());
            printSection("Callers",           u.callers());
            printSection("Subclasses",        u.subclasses());
            printSection("Super types",       u.superTypes());
            printSection("Field readers",     u.fieldReaders());
            printSection("Field writers",     u.fieldWriters());
            printSection("Annotated with",    u.annotatedWith());
            printSection("Methods returning", u.methodsReturning());
            printSection("Methods taking",    u.methodsTaking());
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static void printSection(String label, List<String> items) {
        if (items.isEmpty()) return;
        System.out.printf("%s (%d):%n", label, items.size());
        items.forEach(s -> System.out.printf("  %s%n", s));
        System.out.println();
    }
}
