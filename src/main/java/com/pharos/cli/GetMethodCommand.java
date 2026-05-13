package com.pharos.cli;

import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "method",
        description = "Retrieve the full source body and metadata of a method by its FQN",
        mixinStandardHelpOptions = true
)
public class GetMethodCommand implements Callable<Integer> {

    @Parameters(index = "0",
            description = "Fully qualified method name: com.example.MyClass#methodName(ParamType1,ParamType2)")
    private String fqn;

    private final SearchEngine searchEngine;

    public GetMethodCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            SearchResult r = searchEngine.getMethodByFqn(fqn);
            if (r == null) {
                System.err.println("Method not found: " + fqn);
                return 1;
            }
            if (r.javadoc() != null && !r.javadoc().isBlank()) {
                System.out.println(r.javadoc().strip());
                System.out.println();
            }
            System.out.println(r.signature() + " {");
            System.out.println(r.body());
            System.out.println("}");
            System.out.printf("%n// %s:%d-%d  [%s]%n",
                    r.filePath(), r.startLine(), r.endLine(), r.project());
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
