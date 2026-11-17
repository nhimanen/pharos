package com.pharos.cluster.routing;

/** A requested project name is not a valid shard id (the caller's fault: HTTP 400). */
public class InvalidProjectException extends IllegalArgumentException {
    public InvalidProjectException(String message, Throwable cause) {
        super(message, cause);
    }
}
