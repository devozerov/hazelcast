package com.hazelcast.projectx;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class PortablePersonFactory implements PortableFactory {
    @Override
    public Portable create(int classId) {
        if (classId == 1) {
            return new PortablePerson();
        } else {
            throw new IllegalStateException("Unsupported class ID: " + classId);
        }
    }
}
