package com.pharos.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads C/C++ project metadata from a top-level {@code CMakeLists.txt}.
 *
 * Extracts the project name and optional version from the {@code project()} command.
 * CMake dependency management (find_package, FetchContent) is intentionally not
 * parsed — it is too varied and rarely maps cleanly to module-graph coordinates.
 */
public class CMakeReader {

    private static final Logger log = LoggerFactory.getLogger(CMakeReader.class);

    private static final String GROUP_PREFIX = "cmake";

    // project(name [CXX C LANGUAGES...] [VERSION x.y.z] ...)
    private static final Pattern RE_PROJECT =
            Pattern.compile("(?im)^\\s*project\\s*\\(\\s*(\\S+)");
    private static final Pattern RE_VERSION =
            Pattern.compile("(?i)\\bVERSION\\s+([\\d.]+)");

    public static boolean isCMakeProject(Path projectRoot) {
        return Files.exists(projectRoot.resolve("CMakeLists.txt"));
    }

    public Optional<MavenPomReader.PomInfo> read(Path projectRoot) {
        Path cmake = projectRoot.resolve("CMakeLists.txt");
        if (!Files.exists(cmake)) return Optional.empty();
        try {
            String text = Files.readString(cmake);
            Matcher mName = RE_PROJECT.matcher(text);
            if (!mName.find()) return Optional.empty();

            String name = mName.group(1).toLowerCase();
            String version = "unknown";
            Matcher mVer = RE_VERSION.matcher(text);
            if (mVer.find()) version = mVer.group(1);

            log.debug("CMake project detected: {}:{} v{}", GROUP_PREFIX, name, version);
            return Optional.of(new MavenPomReader.PomInfo(
                    new MavenPomReader.MavenCoordinates(GROUP_PREFIX, name, version),
                    List.of(), List.of()));
        } catch (IOException e) {
            log.debug("Failed to read CMakeLists.txt at {}: {}", projectRoot, e.getMessage());
            return Optional.empty();
        }
    }
}
