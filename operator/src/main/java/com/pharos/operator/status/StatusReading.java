package com.pharos.operator.status;

import com.pharos.cluster.status.ClusterStatus;

import java.time.Instant;

/**
 * One successful {@code GET /cluster/status}: the answer, when the operator got it (its own clock)
 * and which address answered (for logs; an IP and port, never a credential).
 */
public record StatusReading(ClusterStatus status, Instant fetchedAt, String from) {}
