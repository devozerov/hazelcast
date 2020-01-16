package com.hazelcast.projectx.transportable;

import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class TransportableSerializer implements StreamSerializer<Transportable> {

    private final TransportableRegistry registry = new TransportableRegistry();

    @Override
    public void write(ObjectDataOutput out, Transportable object) throws IOException {
        ByteArrayObjectDataOutput out0 = (ByteArrayObjectDataOutput)out;

        TransportableSchema schema = registry.getOrCreate(object.getClass());

        // Header.
        out0.writeInt(schema.getTypeId());
        out0.writeInt(schema.getSchemaId());

        // Fixed fields.
        for (TransportableField field : schema.getFixedLenFields()) {
            switch (field.getType()) {
                case INT:
                    out0.writeInt(field.getInt(object));

                    break;

                case LONG:
                    out0.writeLong(field.getLong(object));

                    break;
            }
        }

        // Var fields.
        int varPos = out0.position();

        int[] offsets = new int[schema.getVariableLenFields().size()];
        int offsetIdx = 0;

        for (TransportableField field : schema.getVariableLenFields()) {
            switch (field.getType()) {
                case STRING:
                    offsets[offsetIdx++] = out0.position() - varPos;

                    out0.writeUTF(field.getObject(object));

                    break;
            }
        }

        // Var field offsets.
        for (int offset : offsets) {
            out0.writeByte((byte)offset);
        }
    }

    @Override
    public Transportable read(ObjectDataInput in) throws IOException {
        ByteArrayObjectDataInput in0 = (ByteArrayObjectDataInput)in;

        int typeId = in0.readInt();
        int schemaId = in0.readInt();

        TransportableSchema schema = registry.get(typeId, schemaId);

        Transportable res = schema.newInstance();

        for (TransportableField field : schema.getFixedLenFields()) {
            switch (field.getType()) {
                case INT: {
                    field.setInt(res, in0.readInt());

                    break;
                }

                case LONG: {
                    field.setLong(res, in0.readLong());

                    break;
                }
            }
        }

        for (TransportableField field : schema.getVariableLenFields()) {
            switch (field.getType()) {
                case STRING: {
                    field.setObject(res, in.readUTF());

                    break;
                }
            }
        }

        return res;
    }

    public Object readField(int totalLen, ByteArrayObjectDataInput in, String path) throws Exception {
        int typeId = in.readInt();
        int schemaId = in.readInt();

        TransportableSchema schema = registry.get(typeId, schemaId);

        TransportableField field = schema.getField(path);
        TransportableFieldType fieldType = field.getType();

        int offset;

        if (fieldType.isFixedLength()) {
            offset = field.getFixedOffset();
        } else {
            int fieldIdx = field.getIndex() - schema.getFixedLenFields().size();

            int offsetPos = totalLen - schema.getVariableLenFields().size() + fieldIdx;

            offset = schema.getFixedPartLen() + in.readByte(offsetPos);
        }

        in.position(in.position() + offset);

        switch (fieldType) {
            case INT:
                return in.readInt();

            case LONG:
                return in.readLong();

            case STRING:
                return in.readUTF();

            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    @Override
    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_TRANSPORTABLE;
    }

    @Override
    public void destroy() {
        // No-op.
    }
}
