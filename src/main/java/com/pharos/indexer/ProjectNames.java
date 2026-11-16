package com.pharos.indexer;

import java.nio.file.Path;

/**
 * Validation of project names. A project name becomes a directory under the index root, so a name
 * such as "." or ".." would make the project's index dir the index root itself (or its parent),
 * and deleting the project would delete every project's index.
 *
 * <p>Deliberately lenient otherwise (spaces and punctuation are fine); this is not the stricter
 * cluster shard-id rule.
 */
public final class ProjectNames {

    private ProjectNames() {}

    /** Structural check only (no index dir needed). */
    public static boolean isValid(String name) {
        return problem(name) == null;
    }

    /** @throws IllegalArgumentException if the name is unusable as a project name */
    public static void validate(String name) {
        String p = problem(name);
        if (p != null) throw new IllegalArgumentException(p);
    }

    /** Structural check plus: the resolved index dir must be a strict direct child of {@code indexDir}. */
    public static void validate(String name, Path indexDir) {
        validate(name);
        resolveProjectDir(indexDir, name);
    }

    /**
     * Resolves a project's index dir, refusing any name whose dir is not a strict direct child of
     * {@code indexDir}. Every code path that deletes or writes a project dir goes through this.
     */
    public static Path resolveProjectDir(Path indexDir, String name) {
        validate(name);
        Path root = indexDir.toAbsolutePath().normalize();
        Path dir = root.resolve(name).normalize();
        if (!root.equals(dir.getParent())) {
            throw new IllegalArgumentException("Invalid project name '" + name + "': its index directory "
                    + dir + " is not a direct child of the index directory " + root
                    + ". Choose another name with --project <name>.");
        }
        return indexDir.resolve(name);
    }

    private static String problem(String name) {
        String hint = " Choose a project name with --project <name>.";
        if (name == null || name.isBlank()) {
            return "Invalid project name: it is empty." + hint;
        }
        if (name.equals(".") || name.equals("..")) {
            return "Invalid project name '" + name + "': it would make the project's index directory "
                    + "the index root." + hint;
        }
        if (name.indexOf('/') >= 0 || name.indexOf('\\') >= 0) {
            return "Invalid project name '" + name + "': it must not contain '/' or '\\'." + hint;
        }
        return null;
    }
}
