package com.hazelcast.projectx.transportable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TransportableRegistry {
    private Map<SchemaKey, TransportableSchema> idToSchema = new HashMap<>();
    private Map<String, TransportableSchema> clazzToSchema = new HashMap<>();

    public TransportableSchema getOrCreate(Class<?> clazz) {
        String clazzName = clazz.getName();

        TransportableSchema schema = clazzToSchema.get(clazzName);

        if (schema == null) {
            schema = createSchema(clazz);

            idToSchema.put(new SchemaKey(schema.getTypeId(), schema.getSchemaId()), schema);
            clazzToSchema.put(clazzName, schema);
        }

        return schema;
    }

    public TransportableSchema get(int typeId, int schemaId) {
        TransportableSchema schema = idToSchema.get(new SchemaKey(typeId, schemaId));

        if (schema == null) {
            throw new IllegalStateException("Schema not found!");
        }

        return schema;
    }

    private TransportableSchema createSchema(Class<?> clazz) {
        int fieldPos = 0;
        int fixedOffset = 0;

        List<TransportableField> fixedLenFields = new ArrayList<>();
        List<TransportableField> variableLenFields = new ArrayList<>();

        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            TransportableFieldType type = TransportableFieldType.resolveType(field.getType());

            if (type.isFixedLength()) {
                TransportableField field0 = new TransportableField(
                    field.getName(),
                    type,
                    fieldPos,
                    fixedOffset,
                    field
                );

                fixedLenFields.add(field0);

                fieldPos++;
                fixedOffset += type.getLength();
            }
        }

        for (Field field : fields) {
            field.setAccessible(true);

            TransportableFieldType type = TransportableFieldType.resolveType(field.getType());

            if (!type.isFixedLength()) {
                TransportableField field0 = new TransportableField(
                    field.getName(),
                    type,
                    fieldPos++,
                    -1,
                    field
                );

                variableLenFields.add(field0);
            }
        }

        return new TransportableSchema(
            clazz,
            fixedLenFields,
            variableLenFields
        );
    }

    private static class SchemaKey {
        private final int typeId;
        private final int schemaId;

        private SchemaKey(int typeId, int schemaId) {
            this.typeId = typeId;
            this.schemaId = schemaId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SchemaKey schemaKey = (SchemaKey) o;

            return typeId == schemaKey.typeId && schemaId == schemaKey.schemaId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(typeId, schemaId);
        }
    }
}
