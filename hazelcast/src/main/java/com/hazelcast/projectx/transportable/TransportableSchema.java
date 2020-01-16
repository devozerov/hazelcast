package com.hazelcast.projectx.transportable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportableSchema {
    private final Class<?> clazz;
    private final int typeId;
    private final List<TransportableField> fixedLenFields;
    private final List<TransportableField> variableLenFields;

    private final int schemaId;
    private final Map<String, TransportableField> fieldNameMap;

    private int fixedPartLen;

    public TransportableSchema(Class<?> clazz, List<TransportableField> fixedLenFields, List<TransportableField> variableLenFields) {
        this.clazz = clazz;
        this.typeId = clazz.getName().hashCode();
        this.fixedLenFields = fixedLenFields;
        this.variableLenFields = variableLenFields;

        int schemaId0 = 0;
        int fixedPartLen0 = 0;
        fieldNameMap = new HashMap<>();

        for (TransportableField field : fixedLenFields) {
            schemaId0 = 31 * schemaId0 + field.getName().hashCode();
            fixedPartLen0 += field.getType().getLength();

            fieldNameMap.put(field.getName(), field);
        }

        for (TransportableField field : variableLenFields) {
            schemaId0 = 31 * schemaId0 + field.getName().hashCode();

            fieldNameMap.put(field.getName(), field);
        }

        schemaId = schemaId0;
        fixedPartLen = fixedPartLen0;
    }

    public int getTypeId() {
        return typeId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getFixedPartLen() {
        return fixedPartLen;
    }

    public List<TransportableField> getFixedLenFields() {
        return fixedLenFields;
    }

    public List<TransportableField> getVariableLenFields() {
        return variableLenFields;
    }

    public Transportable newInstance() {
        try {
            return (Transportable) clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create new instance: " + clazz.getName(), e);
        }
    }

    public TransportableField getField(String name) {
        return fieldNameMap.get(name);
    }
}
