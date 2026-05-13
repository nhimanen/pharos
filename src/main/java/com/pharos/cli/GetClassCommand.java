package com.pharos.cli;

import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import com.pharos.search.SourceReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(
        name = "class",
        description = "Retrieve a class body (including fields, enum constants, class-level annotations) by qualified name",
        mixinStandardHelpOptions = true
)
public class GetClassCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Qualified class name: com.example.MyClass")
    private String fqn;

    private final SearchEngine searchEngine;

    public GetClassCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            SearchResult r = searchEngine.getClassByFqn(fqn);
            if (r == null) {
                System.out.println("Class not found: " + fqn);
                return 1;
            }
            System.out.printf("%s [%s]%n", r.qualifiedClassName(), r.project());
            System.out.printf("%s:%d-%d%n%n", r.filePath(), r.startLine(), r.endLine());

            String body = SourceReader.readRange(r.filePath(), r.startLine(), r.endLine());
            if (body != null) {
                System.out.println(body);
            } else {
                // Fallback: source file not readable — emit what the index has
                if (r.javadoc() != null && !r.javadoc().isBlank()) {
                    System.out.println("/** " + r.javadoc() + " */");
                }
                System.out.println("(source file unavailable; showing indexed body)");
                if (r.body() != null) System.out.println(r.body());
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
