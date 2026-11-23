package com.pharos.cluster;

/** Another writer advanced the shard's generation concurrently. */
public class PublishConflictException extends PublishException {
    public PublishConflictException(String message) { super(message); }
    public PublishConflictException(String message, Throwable cause) { super(message, cause); }
}
