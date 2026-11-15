package com.pharos;

import java.io.InputStream;
import java.util.Properties;

/** The running pharos version, filtered from the pom into {@code pharos-version.properties}. */
public final class PharosVersion {

    private static final String VERSION = load();

    private PharosVersion() {}

    /** The pom version, or {@code "dev"} when the resource is missing or unfiltered. */
    public static String get() {
        return VERSION;
    }

    private static String load() {
        try (InputStream in = PharosVersion.class.getResourceAsStream("/pharos-version.properties")) {
            if (in == null) return "dev";
            Properties p = new Properties();
            p.load(in);
            String v = p.getProperty("version");
            if (v == null || v.isBlank() || v.startsWith("${")) return "dev";
            return v.trim();
        } catch (Exception e) {
            return "dev";
        }
    }
}
