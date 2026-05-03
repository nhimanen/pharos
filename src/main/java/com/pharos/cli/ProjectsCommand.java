package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import picocli.CommandLine.*;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "projects",
        description = "List all indexed projects",
        mixinStandardHelpOptions = true
)
public class ProjectsCommand implements Callable<Integer> {

    private final ProjectRegistry registry;

    public ProjectsCommand(ProjectRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Integer call() {
        List<ProjectMeta> projects = registry.listAll();
        if (projects.isEmpty()) {
            System.out.println("No projects indexed. Run 'pharos index <path>' to get started.");
            return 0;
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
                .withZone(ZoneId.systemDefault());

        System.out.printf("%-25s  %7s  %7s  %6s  %s%n",
                "PROJECT", "METHODS", "CLASSES", "FILES", "LAST INDEXED");
        System.out.println("-".repeat(75));
        for (ProjectMeta p : projects) {
            String lastIndexed = p.getLastIndexed() != null ? fmt.format(p.getLastIndexed()) : "never";
            System.out.printf("%-25s  %7d  %7d  %6d  %s%n",
                    p.getName(), p.getMethodCount(), p.getClassCount(), p.getFileCount(), lastIndexed);
            if (!p.getLinkedProjects().isEmpty()) {
                System.out.printf("  → linked: %s%n", String.join(", ", p.getLinkedProjects()));
            }
        }
        return 0;
    }
}
