package com.hazelcast.projectx.transportable;

public enum TransportableFieldType {
    INT(4),
    LONG(8),
    STRING(-1);

    private final int length;

    TransportableFieldType(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public boolean isFixedLength() {
        return length != -1;
    }

    public static TransportableFieldType resolveType(Class<?> clazz) {
        if (clazz == int.class) {
            return INT;
        } else if (clazz == long.class) {
            return LONG;
        } else if (clazz == String.class) {
            return STRING;
        } else {
            throw new UnsupportedOperationException("Unsupported field type: " + clazz.getName());
        }
    }
}
