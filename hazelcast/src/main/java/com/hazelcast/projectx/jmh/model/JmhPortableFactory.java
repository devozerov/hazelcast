package com.hazelcast.projectx.jmh.model;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class JmhPortableFactory implements PortableFactory {
    @Override
    public Portable create(int classId) {
        if (classId == 1) {
            return new SmallPortable();
        } else if (classId == 2) {
            return new BigPortable();
        } else {
            throw new IllegalArgumentException("Unsupported class ID: " + classId);
        }
    }
}
