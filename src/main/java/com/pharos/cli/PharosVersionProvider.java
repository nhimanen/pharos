package com.pharos.cli;

import com.pharos.PharosVersion;
import picocli.CommandLine;

/** Supplies the {@code --version} output from the pom-filtered version. */
public class PharosVersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
        return new String[] { "pharos " + PharosVersion.get() };
    }
}
