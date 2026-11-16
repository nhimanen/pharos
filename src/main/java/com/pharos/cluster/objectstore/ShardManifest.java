package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
 */
@JsonPropertyOrder({"formatVersion", "shard", "generation", "parentGeneration", "createdAt",
        "writerNode", "docCount", "sizeBytes", "modelFingerprint", "indexFormat", "files", "projectMeta"})
public record ShardManifest(int formatVersion, String shard, long generation, Long parentGeneration,
                            Instant createdAt, String writerNode, long docCount, long sizeBytes,
                            String modelFingerprint, List<ManifestFile> files, ProjectMeta projectMeta,
                            IndexFormat.Stamp indexFormat) {

    public static final int FORMAT_VERSION = 1;

    /** ISO-8601 timestamps, alphabetical order for properties without an explicit order, lenient on read. */
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public ShardManifest {
        files = files == null ? List.of() : List.copyOf(files);
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
