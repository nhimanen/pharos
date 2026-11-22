package com.pharos.operator.reconcile;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Builds a condition list that keeps {@code lastTransitionTime} while a condition's status is unchanged. */
final class StatusConditions {

    /** Kubernetes' limit on a condition message. */
    static final int MAX_MESSAGE = 32768;

    private final List<Condition> previous;
    private final long generation;
    private final Instant now;
    private final List<Condition> result = new ArrayList<>();

    StatusConditions(List<Condition> previous, long generation, Instant now) {
        this.previous = previous == null ? List.of() : previous;
        this.generation = generation;
        this.now = now.truncatedTo(ChronoUnit.SECONDS);
    }

    StatusConditions set(String type, boolean status, String reason, String message) {
        String s = status ? "True" : "False";
        String msg = message == null ? "" : message.length() > MAX_MESSAGE ? message.substring(0, MAX_MESSAGE) : message;
        String since = previous.stream()
                .filter(c -> type.equals(c.getType()) && s.equals(c.getStatus()))
                .map(Condition::getLastTransitionTime)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(now.toString());
        result.add(new ConditionBuilder()
                .withType(type).withStatus(s).withReason(reason).withMessage(msg)
                .withObservedGeneration(generation).withLastTransitionTime(since)
                .build());
        return this;
    }

    List<Condition> build() {
        return List.copyOf(result);
    }
}
