package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pharos.config.ProjectMeta;
import com.pharos.indexer.IndexFormat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable description of one published shard generation ({@code manifests/gen-N.json}).
 * {@code projectMeta} never carries unresolved refs; those ship as the {@code refs} file.
 *
 * <p>{@code indexFormat} is the shard's {@code index-format.json} stamp (format, pharosVersion,
 * writtenAt) at publish time; readers check it before materialising and write it into the
 * generation dir. It is null in manifests published before it existed, and in manifests of an
 * unstamped index: both count as index format 0 (see {@link #indexFormatVersion()}).
 *
 * <p><b>Split projects</b> (Phase 6, a spec other than {@code [project]}). The call graph and the
 * unresolved refs belong to the project, not to one shard: they are uploaded once per index run under
 * {@code projects/<project>/} and listed in {@code projectFiles} (roles {@code graph}, {@code refs}) of
 * every shard manifest that run published. {@code projectGeneration} numbers those runs: every shard
 * manifest of one run carries the same value, and its project files are named after it
 * ({@code projects/<project>/graph/gen-<projectGeneration>@crc.cgf.gz}). A split shard's {@code files}
 * hold only its Lucene files. Both fields are absent (not written) for a {@code [project]} shard, whose
 * manifest is byte for byte that of Phases 1–5.
 */
@JsonPropertyOrder({"formatVersion", "shard", "generation", "parentGeneration", "createdAt",
        "writerNode", "docCount", "sizeBytes", "modelFingerprint", "indexFormat", "files", "projectMeta",
        "projectGeneration", "projectFiles"})
public record ShardManifest(int formatVersion, String shard, long generation, Long parentGeneration,
                            Instant createdAt, String writerNode, long docCount, long sizeBytes,
                            String modelFingerprint, List<ManifestFile> files, ProjectMeta projectMeta,
                            IndexFormat.Stamp indexFormat,
                            @JsonInclude(JsonInclude.Include.NON_NULL) Long projectGeneration,
                            @JsonInclude(JsonInclude.Include.NON_NULL) List<ManifestFile> projectFiles) {

    public static final int FORMAT_VERSION = 1;

    /** A {@code [project]} shard's manifest (no project-level files). */
    public ShardManifest(int formatVersion, String shard, long generation, Long parentGeneration,
                         Instant createdAt, String writerNode, long docCount, long sizeBytes,
                         String modelFingerprint, List<ManifestFile> files, ProjectMeta projectMeta,
                         IndexFormat.Stamp indexFormat) {
        this(formatVersion, shard, generation, parentGeneration, createdAt, writerNode, docCount, sizeBytes,
                modelFingerprint, files, projectMeta, indexFormat, null, null);
    }

    /** ISO-8601 timestamps, alphabetical order for properties without an explicit order, lenient on read. */
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public ShardManifest {
        files = files == null ? List.of() : List.copyOf(files);
        projectFiles = projectFiles == null ? null : List.copyOf(projectFiles);
    }

    /** True for a shard of a split project: its graph and refs are project files. */
    @JsonIgnore
    public boolean isProjectShard() {
        return projectGeneration != null;
    }

    /** The project-level files; empty for a {@code [project]} shard. */
    @JsonIgnore
    public List<ManifestFile> projectFileList() {
        return projectFiles == null ? List.of() : projectFiles;
    }

    /**
     * The call graph this generation is served with: the project's graph of a split project's shard
     * ({@code projectFiles}), else the graph in {@code files}; empty when it was published without one.
     */
    @JsonIgnore
    public java.util.Optional<ManifestFile> graphFile() {
        for (ManifestFile f : projectFileList()) if (ManifestFile.GRAPH.equals(f.role())) return java.util.Optional.of(f);
        for (ManifestFile f : files) if (ManifestFile.GRAPH.equals(f.role())) return java.util.Optional.of(f);
        return java.util.Optional.empty();
    }

    /** The shard's index format; 0 when the manifest carries no stamp (published before stamps shipped). */
    @JsonIgnore
    public int indexFormatVersion() {
        return indexFormat == null ? 0 : indexFormat.format();
    }

    /** A copy of {@code meta} with unresolved refs cleared; {@code meta} itself is not modified. */
    public static ProjectMeta stripRefs(ProjectMeta meta) {
        ProjectMeta copy = MAPPER.convertValue(meta, ProjectMeta.class);
        copy.setUnresolvedRefs(new ArrayList<>());
        return copy;
    }

    public byte[] toJson() throws IOException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
    }

    public static ShardManifest read(byte[] json) throws IOException {
        return MAPPER.readValue(json, ShardManifest.class);
    }
}
