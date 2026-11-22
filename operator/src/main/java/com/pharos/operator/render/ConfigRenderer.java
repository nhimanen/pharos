package com.pharos.operator.render;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;
import com.pharos.config.ClusterConfig;
import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Triggers;
import io.fabric8.kubernetes.api.model.Quantity;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Renders pharos's {@code cluster.*} config for each role (writer, reader, router) from the spec.
 *
 * <p>Each role's config is built as a {@link ClusterConfig}: the operator's own keys (role, the
 * object-store backend, {@code tokenEnv}, the node id and advertised URL, the work and cache dirs),
 * the autoscaling triggers mapped 1:1 onto {@code cluster.status.*}, then {@code spec.pharosConfig}
 * merged on top. The result is checked with {@link ClusterConfig#validate()} and serialized as the
 * role's {@code config.json}. Secrets never enter it: the token and the object-store keys are only
 * named ({@code tokenEnv}, {@code accessKeyEnv}, {@code secretKeyEnv}) and reach pods as env.
 *
 * <p>The node id and advertised URL differ per pod, but a ConfigMap is per role: they are rendered
 * with the placeholder {@link #POD_NAME_PLACEHOLDER}, which each pod's init container replaces with
 * its own name.
 */
public final class ConfigRenderer {

    /** Env var holding the cluster token (from the {@code <name>-token} Secret). */
    public static final String TOKEN_ENV = "PHAROS_TOKEN";
    /** Env vars holding the object-store keys (from the MinIO or the S3 credentials Secret). */
    public static final String ACCESS_KEY_ENV = "PHAROS_S3_ACCESS_KEY";
    public static final String SECRET_KEY_ENV = "PHAROS_S3_SECRET_KEY";
    /** Replaced by each pod's name when the pod starts (see {@link WorkloadRenderer}). */
    public static final String POD_NAME_PLACEHOLDER = "$(POD_NAME)";
    /** The file name of the rendered config in its ConfigMap. */
    public static final String CONFIG_KEY = "config.json";

    /** pharos's HTTP port on writers and readers ({@code pharos web}). */
    public static final int WEB_PORT = 7070;
    /** The router's HTTP port ({@code pharos router}). */
    public static final int ROUTER_PORT = 7080;

    /** Where a pod keeps its state; the image's {@code PHAROS_HOME}. */
    public static final String PHAROS_HOME = "/data/pharos";
    /** A writer's work dir (source checkouts), on its {@code work} PVC. */
    public static final String WORK_DIR = "/data/work";
    /** A reader's block cache, on its {@code cache} PVC. */
    public static final String CACHE_DIR = "/data/cache";
    /** Share of the reader's cache PVC its block cache may fill; the rest holds the small eager files and the model. */
    static final double CACHE_SHARE = 0.75;

    /**
     * Keys of {@code cluster.*} the operator sets itself; {@code spec.pharosConfig} may not set them (or
     * anything below them). The {@code status.*} entries are the ones {@code autoscaling.triggers} maps onto.
     */
    public static final List<String> OWNED_KEYS = List.of(
            "role", "backend", "objectStore", "token", "tokenEnv", "nodeId", "advertiseUrl",
            "writer.workQueue", "writer.workDir", "cacheDir", "reader.mode",
            "status.targets.warmP95Millis", "status.targets.qpsPerReader", "status.windows.addReadersSeconds",
            "status.removeReadersLoad", "status.windows.removeReadersSeconds",
            "status.writers.backlogShards", "status.writers.oldestPendingSeconds", "status.windows.addWritersSeconds",
            "status.writers.idleSeconds", "status.windows.removeWritersSeconds");

    /** Stands in for the token while validating: the operator never holds the real one here. */
    private static final String TOKEN_PLACEHOLDER = "operator-validation-placeholder";

    static final ObjectMapper MAPPER = JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(SerializationFeature.INDENT_OUTPUT, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .addModule(new SimpleModule().addSerializer(Path.class, new PlainPathSerializer()))
            .build();

    /** The roles a ConfigMap is rendered for. */
    public enum Role {
        WRITER("writer"), READER("reader"), ROUTER("router");

        private final String id;

        Role(String id) { this.id = id; }

        /** {@code writer} | {@code reader} | {@code router}: the ConfigMap suffix and {@code PHAROS_ROLE}. */
        public String id() { return id; }
    }

    /** One role's config: the validated {@link ClusterConfig} and the {@code config.json} text. */
    public record RoleConfig(Role role, ClusterConfig config, String json) {
        public int drainSeconds() { return config.getDrainSeconds(); }
    }

    /** Every role's config, or the problems that stopped it (each prefixed with its field). */
    public record Result(Map<Role, RoleConfig> roles, List<String> errors) {
        public RoleConfig get(Role role) { return roles.get(role); }
    }

    private ConfigRenderer() {}

    /** Renders every role's config of a valid spec (see {@code SpecValidator}). */
    public static Result render(PharosCluster cluster) {
        PharosClusterSpec spec = cluster.getSpec();
        List<String> errors = new ArrayList<>();
        ObjectNode extra = expand(spec.pharosConfig());
        List<String> leaves = new ArrayList<>();
        leafPaths(extra, "", leaves);
        TreeSet<String> owned = new TreeSet<>();
        for (String leaf : leaves) {
            for (String key : OWNED_KEYS) {
                if (leaf.equals(key) || leaf.startsWith(key + ".") || key.startsWith(leaf + ".")) owned.add(leaf);
            }
        }
        if (!owned.isEmpty()) {
            errors.add("spec.pharosConfig: keys owned by the operator: " + String.join(", ", owned));
            return new Result(Map.of(), errors);
        }
        Map<Role, RoleConfig> roles = new LinkedHashMap<>();
        for (Role role : Role.values()) {
            RoleConfig rc = renderRole(cluster, role, extra, leaves, errors);
            if (rc != null) roles.put(role, rc);
        }
        // the same pharosConfig problem shows up once per role: report it once
        return errors.isEmpty() ? new Result(Map.copyOf(roles), List.of())
                : new Result(Map.of(), List.copyOf(new java.util.LinkedHashSet<>(errors)));
    }

    private static RoleConfig renderRole(PharosCluster cluster, Role role, ObjectNode extra, List<String> leaves,
                                         List<String> errors) {
        ClusterConfig base = base(cluster, role);
        ObjectNode tree = MAPPER.valueToTree(base);
        merge(tree, extra);
        ClusterConfig merged;
        try {
            merged = MAPPER.treeToValue(tree, ClusterConfig.class);
        } catch (JsonMappingException e) {
            // name the field only: a value from the spec is never echoed
            errors.add("spec.pharosConfig." + jsonPath(e) + ": has the wrong type");
            return null;
        } catch (JsonProcessingException e) {
            errors.add("spec.pharosConfig: cannot be read as pharos's cluster config");
            return null;
        }
        ObjectNode canonical = MAPPER.valueToTree(merged);
        List<String> unknown = new ArrayList<>();
        for (String leaf : leaves) {
            JsonNode given = at(extra, leaf);
            if (given != null && !given.isNull() && at(canonical, leaf) == null) unknown.add(leaf);
        }
        if (!unknown.isEmpty()) {
            errors.add("spec.pharosConfig: unknown keys (not in pharos's cluster config): " + String.join(", ", unknown));
            return null;
        }
        String problem = validate(canonical, cluster, role);
        if (problem != null) {
            errors.add("cluster config (" + role.id() + "): " + problem);
            return null;
        }
        ObjectNode file = JsonNodeFactory.instance.objectNode();
        file.set("cluster", canonical);
        try {
            return new RoleConfig(role, merged, MAPPER.writeValueAsString(file) + "\n");
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize the " + role.id() + " config", e);
        }
    }

    /** The operator's keys for {@code role}, before {@code spec.pharosConfig}. */
    static ClusterConfig base(PharosCluster cluster, Role role) {
        String name = cluster.getMetadata().getName();
        String namespace = cluster.getMetadata().getNamespace();
        PharosClusterSpec spec = cluster.getSpec();
        ClusterConfig c = new ClusterConfig();
        // The router ignores cluster.role; 'writer' is the role whose validation needs nothing per pod.
        c.setRole(role == Role.READER ? NodeRole.READER : NodeRole.WRITER);
        c.setBackend(BackendKind.OBJECT_STORE);
        c.setObjectStore(StorageRenderer.objectStore(cluster));
        c.setTokenEnv(TOKEN_ENV);
        c.setCacheDir(Path.of(PHAROS_HOME, "cluster-cache"));
        c.getWriter().setWorkDir(Path.of(PHAROS_HOME, "work"));
        c.getPeers().setStoreDir(Path.of(PHAROS_HOME, "peer-store"));
        if (role != Role.ROUTER) {
            c.setNodeId(POD_NAME_PLACEHOLDER);
            c.setAdvertiseUrl("http://" + POD_NAME_PLACEHOLDER + "."
                    + Names.serviceHost(Names.headless(name, role.id()), namespace) + ":" + WEB_PORT);
        }
        if (role == Role.WRITER) {
            c.getWriter().setWorkQueue(true);
            c.getWriter().setWorkDir(Path.of(WORK_DIR));
        }
        if (role == Role.READER) {
            c.setCacheDir(Path.of(CACHE_DIR));
            c.getReader().setMode(Defaults.readerMode(spec));
            c.getReader().setCacheMaxBytes(cacheMaxBytes(Defaults.cacheVolume(spec)));
        }
        triggers(spec, c.getStatus());
        return c;
    }

    /** A reader's block-cache cap: {@value #CACHE_SHARE} of its cache PVC. */
    static long cacheMaxBytes(String cacheVolume) {
        return (long) (Quantity.getAmountInBytes(new Quantity(cacheVolume)).doubleValue() * CACHE_SHARE);
    }

    /** {@code autoscaling.triggers} onto pharos's proposal thresholds and windows, one key each. */
    static void triggers(PharosClusterSpec spec, ClusterConfig.StatusConfig status) {
        status.getTargets().setQpsPerReader(Defaults.qpsPerReader(spec));
        Triggers t = spec.autoscaling() == null ? null : spec.autoscaling().triggers();
        if (t == null) return;
        if (t.readers() != null && t.readers().scaleUp() != null) {
            var up = t.readers().scaleUp();
            if (up.warmP95Millis() != null) status.getTargets().setWarmP95Millis(up.warmP95Millis());
            if (up.forSeconds() != null) status.getWindows().setAddReadersSeconds(up.forSeconds());
        }
        if (t.readers() != null && t.readers().scaleDown() != null) {
            var down = t.readers().scaleDown();
            if (down.loadBelowPercent() != null) status.setRemoveReadersLoad(down.loadBelowPercent() / 100.0);
            if (down.forSeconds() != null) status.getWindows().setRemoveReadersSeconds(down.forSeconds());
        }
        if (t.writers() != null && t.writers().scaleUp() != null) {
            var up = t.writers().scaleUp();
            if (up.backlogShards() != null) status.getWriters().setBacklogShards(up.backlogShards());
            if (up.oldestPendingSeconds() != null) status.getWriters().setOldestPendingSeconds(up.oldestPendingSeconds());
            if (up.forSeconds() != null) status.getWindows().setAddWritersSeconds(up.forSeconds());
        }
        if (t.writers() != null && t.writers().scaleDown() != null && t.writers().scaleDown().idleForSeconds() != null) {
            status.getWriters().setIdleSeconds(t.writers().scaleDown().idleForSeconds().longValue());
        }
    }

    /**
     * {@link ClusterConfig#validate()} on a copy as a pod of {@code role} would see it: the placeholder
     * pod name replaced by the role's first pod, and a stand-in token in place of {@code tokenEnv} (the
     * pod gets the real one from the Secret; the operator's own environment has none).
     */
    private static String validate(ObjectNode canonical, PharosCluster cluster, Role role) {
        try {
            ClusterConfig c = MAPPER.treeToValue(canonical, ClusterConfig.class);
            String pod = (role == Role.READER ? Names.readers(cluster.getMetadata().getName())
                    : Names.writers(cluster.getMetadata().getName())) + "-0";
            if (c.getNodeId() != null) c.setNodeId(c.getNodeId().replace(POD_NAME_PLACEHOLDER, pod));
            if (c.getAdvertiseUrl() != null) c.setAdvertiseUrl(c.getAdvertiseUrl().replace(POD_NAME_PLACEHOLDER, pod));
            c.setTokenEnv(null);
            c.setToken(TOKEN_PLACEHOLDER);
            c.validate();
            return null;
        } catch (IllegalStateException e) {
            return e.getMessage();
        } catch (JsonProcessingException e) {
            return "cannot be read back";
        }
    }

    /** {@code pharosConfig} as one tree: a dotted key ({@code writer.pollSeconds}) is the same as nesting. */
    static ObjectNode expand(Map<String, JsonNode> pharosConfig) {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        if (pharosConfig == null) return root;
        new java.util.TreeMap<>(pharosConfig).forEach((key, value) -> {
            String[] parts = key.split("\\.");
            ObjectNode node = root;
            for (int i = 0; i < parts.length - 1; i++) {
                JsonNode child = node.get(parts[i]);
                if (!(child instanceof ObjectNode)) {
                    child = JsonNodeFactory.instance.objectNode();
                    node.set(parts[i], child);
                }
                node = (ObjectNode) child;
            }
            String last = parts[parts.length - 1];
            if (value instanceof ObjectNode obj && node.get(last) instanceof ObjectNode existing) {
                merge(existing, obj);
            } else {
                node.set(last, value == null ? JsonNodeFactory.instance.nullNode() : value.deepCopy());
            }
        });
        return root;
    }

    /** Deep-merges {@code from} into {@code into}: objects merge, anything else replaces. */
    static void merge(ObjectNode into, ObjectNode from) {
        for (Iterator<Map.Entry<String, JsonNode>> it = from.fields(); it.hasNext(); ) {
            var e = it.next();
            JsonNode existing = into.get(e.getKey());
            if (existing instanceof ObjectNode o && e.getValue() instanceof ObjectNode n) {
                merge(o, n); // an empty object changes nothing
            } else {
                into.set(e.getKey(), e.getValue().deepCopy());
            }
        }
    }

    /** The dotted paths of {@code node}'s leaves (scalars, arrays and nulls; an empty object sets nothing). */
    static void leafPaths(JsonNode node, String prefix, List<String> out) {
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            var e = it.next();
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            if (e.getValue() instanceof ObjectNode o) {
                leafPaths(o, path, out);
            } else {
                out.add(path);
            }
        }
    }

    private static JsonNode at(JsonNode root, String dotted) {
        JsonNode n = root;
        for (String part : dotted.split("\\.")) {
            n = n == null ? null : n.get(part);
        }
        return n;
    }

    private static String jsonPath(JsonMappingException e) {
        StringBuilder sb = new StringBuilder();
        for (JsonMappingException.Reference r : e.getPath()) {
            if (r.getFieldName() != null) {
                if (!sb.isEmpty()) sb.append('.');
                sb.append(r.getFieldName());
            } else if (r.getIndex() >= 0) {
                sb.append('[').append(r.getIndex()).append(']');
            }
        }
        return sb.isEmpty() ? "?" : sb.toString();
    }

    /** Paths as plain strings ({@code /data/work}), which pharos reads like any path. */
    private static final class PlainPathSerializer extends StdSerializer<Path> {
        PlainPathSerializer() { super(Path.class); }

        @Override
        public void serialize(Path value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.toString());
        }
    }
}
