package com.pharos.search;

/**
 * A query with key filters (module, pathPrefix, language, kind) over an index written before documents
 * carried the key fields (index format below {@link #KEY_FIELDS_FORMAT}): it would silently match
 * nothing, so it is refused, naming the project and the fix. Unfiltered queries are unaffected.
 */
public class KeyFiltersUnsupportedException extends IllegalArgumentException {

    /** The first index format whose documents carry the key fields. */
    public static final int KEY_FIELDS_FORMAT = 2;

    public KeyFiltersUnsupportedException(String project, int format) {
        super("project '" + project + "' was indexed with index format " + format + ", before documents carried"
                + " key fields; re-index it (pharos index --full " + project + ") to use the module, pathPrefix,"
                + " language and kind filters");
    }

    /** Refuses a filtered query over a project dir whose index format predates the key fields. */
    public static void check(String project, java.nio.file.Path projectIndexDir) {
        int format = com.pharos.indexer.IndexFormat.read(projectIndexDir).format();
        if (format < KEY_FIELDS_FORMAT) throw new KeyFiltersUnsupportedException(project, format);
    }
}
