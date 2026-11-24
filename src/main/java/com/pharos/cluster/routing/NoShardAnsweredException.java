package com.pharos.cluster.routing;

import java.util.List;

/**
 * A fanned-out query none of whose shards answered, and none of which this reader holds: an empty
 * answer would look like "no matches". The web API answers 503 (so {@code pharos router} retries
 * once on another reader, which may reach the shards), MCP a tool error the client may retry.
 */
public class NoShardAnsweredException extends RuntimeException {

    private final List<String> warnings;

    public NoShardAnsweredException(List<String> warnings) {
        super("no shard answered (" + String.join("; ", warnings) + ")");
        this.warnings = List.copyOf(warnings);
    }

    /** One warning per shard left out, naming why. */
    public List<String> warnings() {
        return warnings;
    }
}
