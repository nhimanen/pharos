package com.pharos.cluster;

import java.io.IOException;

/** Publishing a committed local index to the backend failed; the local index is intact. */
public class PublishException extends IOException {
    public PublishException(String message) { super(message); }
    public PublishException(String message, Throwable cause) { super(message, cause); }
}
