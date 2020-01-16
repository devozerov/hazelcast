package com.hazelcast.projectx.transportable;

import java.lang.reflect.Field;

public class TransportableField {
    private final String name;
    private final TransportableFieldType type;
    private final int index;
    private final int fixedOffset;
    private final Field field;

    public TransportableField(String name, TransportableFieldType type, int index, int fixedOffset, Field field) {
        this.name = name;
        this.type = type;
        this.index = index;
        this.fixedOffset = fixedOffset;
        this.field = field;
    }

    public String getName() {
        return name;
    }

    public TransportableFieldType getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public int getFixedOffset() {
        return fixedOffset;
    }

    public int getInt(Transportable target) {
        try {
            return field.getInt(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getLong(Transportable target) {
        try {
            return field.getLong(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(Transportable target) {
        try {
            return (T)field.get(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setInt(Transportable target, int val) {
        try {
            field.setInt(target, val);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setLong(Transportable target, long val) {
        try {
            field.setLong(target, val);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setObject(Transportable target, Object val) {
        try {
            field.set(target, val);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
