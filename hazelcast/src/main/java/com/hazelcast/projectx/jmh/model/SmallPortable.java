package com.hazelcast.projectx.jmh.model;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class SmallPortable implements Portable {
    private int i1;
    private int i2;

    private long l1;
    private long l2;

    private String s1;
    private String s2;

    public static SmallPortable generate() {
        return new SmallPortable(
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),

            ModelUtils.randomLong(),
            ModelUtils.randomLong(),

            ModelUtils.randomString(),
            ModelUtils.randomString()
        );
    }

    public SmallPortable() {
        // No-op.
    }

    public SmallPortable(int i1, int i2, long l1, long l2, String s1, String s2) {
        this.i1 = i1;
        this.i2 = i2;
        this.l1 = l1;
        this.l2 = l2;
        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i1", i1);
        writer.writeInt("i2", i2);

        writer.writeLong("l1", l1);
        writer.writeLong("l2", l2);

        writer.writeUTF("s1", s1);
        writer.writeUTF("s2", s2);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        i1 = reader.readInt("i1");
        i2 = reader.readInt("i2");

        l1 = reader.readLong("l1");
        l2 = reader.readLong("l2");

        s1 = reader.readUTF("s1");
        s2 = reader.readUTF("s2");
    }
}
