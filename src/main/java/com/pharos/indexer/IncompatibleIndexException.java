package com.pharos.indexer;

/** Thrown when a project's index format stamp does not match what this pharos build expects. */
public class IncompatibleIndexException extends RuntimeException {

    private final String project;
    private final IndexFormat.Status status;
    private final IndexFormat.Stamp stamp;

    public IncompatibleIndexException(String project, IndexFormat.Status status, IndexFormat.Stamp stamp, String message) {
        super(message);
        this.project = project;
        this.status = status;
        this.stamp = stamp;
    }

    public String project() { return project; }
    public IndexFormat.Status status() { return status; }
    public IndexFormat.Stamp stamp() { return stamp; }
}
