package com.pharos.indexer;

/**
 * The exact command that re-indexes (and embeds) one registered project. {@code index} takes a
 * PATH, not a project name; {@code --single --project <name>} makes it rebuild exactly that root
 * under the registered name (without them, a root holding sub-projects would be indexed as
 * several new projects). Valid for both the Java CLI and the {@code ./pharos} client.
 */
public final class ReindexHint {
    private ReindexHint() {}

    public static String fullIndexCommand(String projectName, String rootPath) {
        String root = rootPath == null || rootPath.isBlank() ? "<root path>" : shellQuote(rootPath);
        return "pharos index --full --single --project " + shellQuote(projectName) + " " + root;
    }

    static String shellQuote(String s) {
        if (s != null && s.matches("[A-Za-z0-9_./:@%+=,-]+")) return s;
        return "'" + String.valueOf(s).replace("'", "'\\''") + "'";
    }
}
