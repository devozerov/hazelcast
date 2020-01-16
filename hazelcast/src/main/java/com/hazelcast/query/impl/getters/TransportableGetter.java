package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.projectx.transportable.TransportableSerializer;

import java.nio.ByteOrder;

public class TransportableGetter extends Getter {
    private final InternalSerializationService ss;
    private final TransportableSerializer serializer;

    public TransportableGetter(InternalSerializationService ss) {
        super(null);

        this.ss = ss;
        this.serializer = ((SerializationServiceV1)ss).getTransportableSerializer();
    }

    @Override
    public Object getValue(Object obj, String attributePath) throws Exception {
        HeapData data = (HeapData)obj;

        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(
            data.toByteArray(),
            HeapData.DATA_OFFSET,
            ss,
            ByteOrder.BIG_ENDIAN
        );

        return serializer.readField(data.totalSize(), in, attributePath);
    }

    @Override
    Object getValue(Object obj) {
        throw new IllegalArgumentException("Path agnostic value extraction unsupported");
    }

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("Non applicable for TransportableGetter");
    }

    @Override
    boolean isCacheable() {
        return false;
    }
}
