package com.pharos.cluster.internal;

/**
 * The shard has no vectors for the coordinator's search embedding model (or a different model
 * fingerprint or dimension count): the reader refuses (409, {@code "error":"model mismatch"}, with
 * {@code "reason"}) and the coordinator leaves that shard out of the vector retrieval with a warning.
 */
public class ModelMismatchException extends ShardCallException {

    /** Why the shard refused; travels as {@code "reason"} in the 409 body. */
    public enum Reason {
        /** The shard's project has no vectors of the model at all (no such vector field). */
        NO_VECTORS,
        /** The reader searches with another model, or the model's dimensions or configuration differ. */
        OTHER;

        /** The wire value; anything unknown or missing (an older reader) is {@link #OTHER}. */
        public static Reason parse(String s) {
            return "NO_VECTORS".equals(s) ? NO_VECTORS : OTHER;
        }
    }

    private final Reason reason;

    public ModelMismatchException(Reason reason, String message) {
        super(message);
        this.reason = reason == null ? Reason.OTHER : reason;
    }

    public ModelMismatchException(String message) {
        this(Reason.OTHER, message);
    }

    public Reason reason() { return reason; }
}
