package com.pharos.operator.validate;

import com.pharos.cluster.ShardId;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.Project;
import com.pharos.operator.api.PharosClusterSpec.RoleSpec;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;
import com.pharos.operator.api.PharosClusterSpec.Triggers;
import io.fabric8.kubernetes.api.model.Quantity;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks a {@link PharosClusterSpec} before anything is created. Every problem is reported (not just
 * the first), each prefixed with its field path, so the {@code SpecInvalid} condition lists them all.
 * Messages name fields and Secret names only, never a secret value.
 */
public final class SpecValidator {

    /** The scale-up load as a fraction of the target: pharos proposes add-readers at qps/reader ≥ qpsPerReader. */
    static final double SCALE_UP_LOAD_FRACTION = 1.0;
    /** pharos's default {@code cluster.status.targets.qpsPerReader}. */
    static final int DEFAULT_QPS_PER_READER = 50;

    private static final Pattern IMAGE_TAG = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}");
    private static final Pattern DNS_SUBDOMAIN =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    private static final Set<String> READER_MODES = Set.of("lazy", "full");
    private static final Set<String> SERVICE_TYPES = Set.of("ClusterIP", "NodePort", "LoadBalancer");

    private SpecValidator() {}

    /** @return the problems found; empty when the spec is valid */
    public static List<String> validate(PharosClusterSpec spec) {
        List<String> errors = new ArrayList<>();
        if (spec == null) {
            errors.add("spec: is missing");
            return errors;
        }
        if (spec.version() == null || !IMAGE_TAG.matcher(spec.version()).matches()) {
            errors.add("spec.version: must be a pharos image tag such as 1.6.0, got '" + spec.version() + "'");
        }
        storage(spec.storage(), errors);
        if (spec.embeddings() != null && blank(spec.embeddings().model())) {
            errors.add("spec.embeddings.model: must be a model name or 'none'");
        }
        projects(spec.projects(), errors);
        role("writers", spec.writers(), errors);
        role("readers", spec.readers(), errors);
        router(spec, errors);
        autoscaling(spec, errors);
        if (spec.pharosConfig() != null && spec.pharosConfig().containsKey("token")) {
            errors.add("spec.pharosConfig.token: not allowed; the operator generates the cluster token into a Secret");
        }
        return errors;
    }

    /** The role's floor: {@code min}, default 1. */
    public static int min(RoleSpec role) {
        return role == null || role.min() == null ? 1 : role.min();
    }

    /** The role's ceiling: {@code max}, default its {@code min}. */
    public static int max(RoleSpec role) {
        return role == null || role.max() == null ? min(role) : role.max();
    }

    private static void storage(PharosClusterSpec.Storage storage, List<String> errors) {
        boolean minio = storage != null && storage.minio() != null;
        boolean s3 = storage != null && storage.s3() != null;
        if (minio == s3) {
            errors.add("spec.storage: set exactly one of minio or s3"
                    + (minio ? ", not both" : ""));
            return;
        }
        if (minio) {
            quantity("spec.storage.minio.storage", storage.minio().storage(), errors);
            return;
        }
        PharosClusterSpec.S3 b = storage.s3();
        if (blank(b.endpoint()) || !(b.endpoint().startsWith("http://") || b.endpoint().startsWith("https://"))) {
            errors.add("spec.storage.s3.endpoint: must be an http:// or https:// URL");
        }
        if (blank(b.bucket())) errors.add("spec.storage.s3.bucket: is required");
        if (blank(b.credentialsSecret())) {
            errors.add("spec.storage.s3.credentialsSecret: is required (a Secret with the keys accessKey and secretKey)");
        } else {
            secretName("spec.storage.s3.credentialsSecret", b.credentialsSecret(), errors);
        }
    }

    private static void projects(List<Project> projects, List<String> errors) {
        if (projects == null) return;
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < projects.size(); i++) {
            Project p = projects.get(i);
            String path = "spec.projects[" + i + "]";
            if (p == null) {
                errors.add(path + ": is empty");
                continue;
            }
            try {
                ShardId.ofProject(p.name());
            } catch (IllegalArgumentException e) {
                errors.add(path + ".name: '" + p.name() + "' is not a valid cluster project name: "
                        + "use letters, digits, '.', '_' or '-', starting with a letter or digit");
            }
            if (p.name() != null && !seen.add(p.name())) {
                errors.add(path + ".name: duplicate project name '" + p.name() + "'");
            }
            if (p.git() == null || blank(p.git().url())) {
                errors.add(path + ".git.url: is required");
            } else if (p.git().credentialsSecret() != null) {
                secretName(path + ".git.credentialsSecret", p.git().credentialsSecret(), errors);
            }
            if (p.git() != null && p.git().ref() != null && blank(p.git().ref())) {
                errors.add(path + ".git.ref: must not be blank");
            }
        }
    }

    private static void role(String name, RoleSpec role, List<String> errors) {
        String path = "spec." + name;
        if (role == null) {
            errors.add(path + ": is required (min and max)");
            return;
        }
        int min = min(role);
        int max = max(role);
        if (min < 1) errors.add(path + ".min: must be at least 1, got " + min);
        if (min > max) errors.add(path + ": min (" + min + ") is above max (" + max + ")");
        if (name.equals("writers")) {
            if (role.mode() != null) errors.add(path + ".mode: applies to readers only");
            if (role.cacheVolume() != null) errors.add(path + ".cacheVolume: applies to readers only");
            quantity(path + ".workVolume", role.workVolume(), errors);
        } else {
            if (role.workVolume() != null) errors.add(path + ".workVolume: applies to writers only");
            if (role.mode() != null && !READER_MODES.contains(role.mode())) {
                errors.add(path + ".mode: must be lazy or full, got '" + role.mode() + "'");
            }
            quantity(path + ".cacheVolume", role.cacheVolume(), errors);
        }
    }

    private static void router(PharosClusterSpec spec, List<String> errors) {
        if (spec.router() == null) return;
        if (spec.router().replicas() != null && spec.router().replicas() < 1) {
            errors.add("spec.router.replicas: must be at least 1, got " + spec.router().replicas());
        }
        if (spec.router().service() != null && spec.router().service().type() != null
                && !SERVICE_TYPES.contains(spec.router().service().type())) {
            errors.add("spec.router.service.type: must be ClusterIP, NodePort or LoadBalancer, got '"
                    + spec.router().service().type() + "'");
        }
    }

    private static void autoscaling(PharosClusterSpec spec, List<String> errors) {
        Autoscaling a = spec.autoscaling();
        if (a == null) return;
        atLeast("spec.autoscaling.cooldownSeconds", a.cooldownSeconds(), 1, errors);
        atLeast("spec.autoscaling.maxStepUp", a.maxStepUp(), 1, errors);
        atLeast("spec.autoscaling.maxStepDown", a.maxStepDown(), 1, errors);
        triggers(a.triggers(), errors);
        if (a.schedule() == null) return;
        for (int i = 0; i < a.schedule().size(); i++) {
            ScheduleEntry e = a.schedule().get(i);
            String path = "spec.autoscaling.schedule[" + i + "]";
            if (e == null) {
                errors.add(path + ": is empty");
                continue;
            }
            String cron = Cron.problem(e.cron());
            if (cron != null) errors.add(path + ".cron: '" + e.cron() + "' " + cron);
            if (e.timezone() != null) {
                try {
                    ZoneId.of(e.timezone());
                } catch (DateTimeException ex) {
                    errors.add(path + ".timezone: unknown time zone '" + e.timezone() + "'");
                }
            }
            if (e.readers() == null && e.writers() == null) {
                errors.add(path + ": set readers.min and/or writers.min");
            }
            floor(path + ".readers", e.readers(), "readers", spec.readers(), errors);
            floor(path + ".writers", e.writers(), "writers", spec.writers(), errors);
        }
    }

    private static void floor(String path, Floor floor, String role, RoleSpec spec, List<String> errors) {
        if (floor == null) return;
        if (floor.min() == null || floor.min() < 1) {
            errors.add(path + ".min: must be at least 1, got " + floor.min());
        } else if (spec != null && floor.min() > max(spec)) {
            errors.add(path + ".min (" + floor.min() + ") is above spec." + role + ".max (" + max(spec) + ")");
        }
    }

    private static void triggers(Triggers t, List<String> errors) {
        if (t == null) return;
        String r = "spec.autoscaling.triggers.readers";
        String w = "spec.autoscaling.triggers.writers";
        Integer qpsPerReader = null;
        if (t.readers() != null) {
            if (t.readers().scaleUp() != null) {
                var up = t.readers().scaleUp();
                atLeast(r + ".scaleUp.warmP95Millis", up.warmP95Millis(), 1, errors);
                atLeast(r + ".scaleUp.qpsPerReader", up.qpsPerReader(), 1, errors);
                atLeast(r + ".scaleUp.forSeconds", up.forSeconds(), 1, errors);
                qpsPerReader = up.qpsPerReader();
            }
            if (t.readers().scaleDown() != null) {
                var down = t.readers().scaleDown();
                atLeast(r + ".scaleDown.forSeconds", down.forSeconds(), 1, errors);
                Integer pct = down.loadBelowPercent();
                if (pct != null) {
                    // Both loads as fractions of the target qpsPerReader: scale-down below pct/100,
                    // scale-up at SCALE_UP_LOAD_FRACTION. Scale-down must sit strictly below scale-up.
                    double downFraction = pct / 100.0;
                    int target = qpsPerReader == null ? DEFAULT_QPS_PER_READER : qpsPerReader;
                    if (pct < 1) {
                        errors.add(r + ".scaleDown.loadBelowPercent: must be at least 1, got " + pct);
                    } else if (downFraction >= SCALE_UP_LOAD_FRACTION) {
                        errors.add(r + ".scaleDown.loadBelowPercent (" + pct + "%) must be below the scale-up load "
                                + "(" + Math.round(SCALE_UP_LOAD_FRACTION * 100) + "% of scaleUp.qpsPerReader="
                                + target + ")");
                    }
                }
            }
        }
        if (t.writers() != null) {
            if (t.writers().scaleUp() != null) {
                var up = t.writers().scaleUp();
                atLeast(w + ".scaleUp.backlogShards", up.backlogShards(), 1, errors);
                atLeast(w + ".scaleUp.oldestPendingSeconds", up.oldestPendingSeconds(), 1, errors);
                atLeast(w + ".scaleUp.forSeconds", up.forSeconds(), 1, errors);
            }
            if (t.writers().scaleDown() != null) {
                atLeast(w + ".scaleDown.idleForSeconds", t.writers().scaleDown().idleForSeconds(), 1, errors);
            }
        }
    }

    private static void atLeast(String path, Integer value, int min, List<String> errors) {
        if (value != null && value < min) errors.add(path + ": must be at least " + min + ", got " + value);
    }

    private static void quantity(String path, String value, List<String> errors) {
        if (value == null) return;
        try {
            if (Quantity.getAmountInBytes(new Quantity(value)).signum() <= 0) {
                errors.add(path + ": must be a positive size such as 20Gi, got '" + value + "'");
            }
        } catch (RuntimeException e) {
            errors.add(path + ": must be a size such as 20Gi, got '" + value + "'");
        }
    }

    private static void secretName(String path, String name, List<String> errors) {
        if (name.length() > 253 || !DNS_SUBDOMAIN.matcher(name).matches()) {
            errors.add(path + ": '" + name + "' is not a valid Secret name");
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
