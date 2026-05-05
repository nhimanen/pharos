package com.pharos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * Reads and writes the global project registry at ~/.pharos/registry.json.
 * Tracks all indexed projects and their metadata.
 */
public class ProjectRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProjectRegistry.class);

    private final Path registryFile;
    private final ObjectMapper mapper;

    public ProjectRegistry(IndexConfig config) {
        this.registryFile = IndexConfig.DEFAULT_BASE.resolve("registry.json");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public synchronized void register(ProjectMeta meta) {
        Map<String, ProjectMeta> registry = loadAll();
        registry.put(meta.getName(), meta);
        save(registry);
        log.debug("Registered project '{}' in registry", meta.getName());
    }

    public synchronized Optional<ProjectMeta> find(String name) {
        return Optional.ofNullable(loadAll().get(name));
    }

    public synchronized List<ProjectMeta> listAll() {
        return new ArrayList<>(loadAll().values());
    }

    public synchronized void link(String project1, String project2) {
        Map<String, ProjectMeta> registry = loadAll();
        ProjectMeta p1 = registry.get(project1);
        ProjectMeta p2 = registry.get(project2);
        if (p1 == null) throw new IllegalArgumentException("Project not found: " + project1);
        if (p2 == null) throw new IllegalArgumentException("Project not found: " + project2);

        if (!p1.getLinkedProjects().contains(project2)) {
            p1.getLinkedProjects().add(project2);
        }
        if (!p2.getLinkedProjects().contains(project1)) {
            p2.getLinkedProjects().add(project1);
        }
        save(registry);
        log.info("Linked projects '{}' and '{}'", project1, project2);
    }

    public synchronized void unregister(String name) {
        Map<String, ProjectMeta> registry = loadAll();
        registry.remove(name);
        save(registry);
    }

    /** Remove {@code name} from the linkedProjects list of every other project. */
    public synchronized void unlinkAll(String name) {
        Map<String, ProjectMeta> registry = loadAll();
        boolean changed = false;
        for (ProjectMeta meta : registry.values()) {
            if (meta.getLinkedProjects().remove(name)) {
                changed = true;
            }
        }
        if (changed) {
            save(registry);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, ProjectMeta> loadAll() {
        if (!Files.exists(registryFile)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, ProjectMeta> result = mapper.readValue(registryFile.toFile(),
                    mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, ProjectMeta.class));
            return result != null ? result : new LinkedHashMap<>();
        } catch (IOException e) {
            log.warn("Could not read registry {}: {}", registryFile, e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    private void save(Map<String, ProjectMeta> registry) {
        try {
            Files.createDirectories(registryFile.getParent());
            // Atomic write via temp file + rename
            Path tmp = registryFile.resolveSibling(registryFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), registry);
            Files.move(tmp, registryFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.error("Failed to save registry: {}", e.getMessage());
        }
    }
}
