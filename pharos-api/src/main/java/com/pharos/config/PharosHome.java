package com.pharos.config;

import java.nio.file.Path;

/** The pharos state directory, shared by {@code IndexConfig} and {@link ClusterConfig} defaults. */
public final class PharosHome {

    /** {@code ~/.pharos}; override with {@code -Dpharos.home=...} (tests do, via surefire). */
    public static final Path DEFAULT_BASE = Path.of(System.getProperty("pharos.home",
            Path.of(System.getProperty("user.home"), ".pharos").toString()));

    private PharosHome() {}
}
