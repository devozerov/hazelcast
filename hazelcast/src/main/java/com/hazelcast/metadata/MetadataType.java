package com.hazelcast.metadata;

public enum MetadataType {
    MAP(0),
    EXTERNAL_MAP(1);

    private final int id;

    MetadataType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static MetadataType getById(int id) {
        for (MetadataType type : MetadataType.values()) {
            if (type.id == id) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown type ID: " + id);
    }
}
