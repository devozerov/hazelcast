package com.hazelcast.metadata.ap;

public class ApMetadataUpdateOperation {
    /** Metadata key. */
    private Object key;

    /** Value to be set (null for drop). */
    private Object value;

    /** {@code true} if the operation should be ignored in case of existence conflict. */
    private boolean ignoreOnExistenceConflict;

    public ApMetadataUpdateOperation() {
        // No-op.
    }

    public ApMetadataUpdateOperation(Object key, Object value, boolean ignoreOnExistenceConflict) {
        this.key = key;
        this.value = value;
        this.ignoreOnExistenceConflict = ignoreOnExistenceConflict;
    }
}
