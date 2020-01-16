package com.hazelcast.projectx.jmh.model;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class BigPortable implements Portable {
    private int i1;
    private int i2;
    private int i3;
    private int i4;
    private int i5;
    private int i6;
    private int i7;
    private int i8;
    private int i9;
    private int i10;

    private long l1;
    private long l2;
    private long l3;
    private long l4;
    private long l5;
    private long l6;
    private long l7;
    private long l8;
    private long l9;
    private long l10;

    private String s1;
    private String s2;
    private String s3;
    private String s4;
    private String s5;
    private String s6;
    private String s7;
    private String s8;
    private String s9;
    private String s10;

    public static BigPortable generate() {
        return new BigPortable(
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),

            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),

            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString(),
            ModelUtils.randomString()
        );
    }

    public BigPortable() {
        // No-op.
    }

    public BigPortable(int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, long l1, long l2, long l3, long l4, long l5, long l6, long l7, long l8, long l9, long l10, String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10) {
        this.i1 = i1;
        this.i2 = i2;
        this.i3 = i3;
        this.i4 = i4;
        this.i5 = i5;
        this.i6 = i6;
        this.i7 = i7;
        this.i8 = i8;
        this.i9 = i9;
        this.i10 = i10;
        this.l1 = l1;
        this.l2 = l2;
        this.l3 = l3;
        this.l4 = l4;
        this.l5 = l5;
        this.l6 = l6;
        this.l7 = l7;
        this.l8 = l8;
        this.l9 = l9;
        this.l10 = l10;
        this.s1 = s1;
        this.s2 = s2;
        this.s3 = s3;
        this.s4 = s4;
        this.s5 = s5;
        this.s6 = s6;
        this.s7 = s7;
        this.s8 = s8;
        this.s9 = s9;
        this.s10 = s10;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i1", i1);
        writer.writeInt("i2", i2);
        writer.writeInt("i3", i3);
        writer.writeInt("i4", i4);
        writer.writeInt("i5", i5);
        writer.writeInt("i6", i6);
        writer.writeInt("i7", i7);
        writer.writeInt("i8", i8);
        writer.writeInt("i9", i9);
        writer.writeInt("i10", i10);

        writer.writeLong("l1", l1);
        writer.writeLong("l2", l2);
        writer.writeLong("l3", l3);
        writer.writeLong("l4", l4);
        writer.writeLong("l5", l5);
        writer.writeLong("l6", l6);
        writer.writeLong("l7", l7);
        writer.writeLong("l8", l8);
        writer.writeLong("l9", l9);
        writer.writeLong("l10", l10);

        writer.writeUTF("s1", s1);
        writer.writeUTF("s2", s2);
        writer.writeUTF("s3", s3);
        writer.writeUTF("s4", s4);
        writer.writeUTF("s5", s5);
        writer.writeUTF("s6", s6);
        writer.writeUTF("s7", s7);
        writer.writeUTF("s8", s8);
        writer.writeUTF("s9", s9);
        writer.writeUTF("s10", s10);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        i1 = reader.readInt("i1");
        i2 = reader.readInt("i2");
        i3 = reader.readInt("i3");
        i4 = reader.readInt("i4");
        i5 = reader.readInt("i5");
        i6 = reader.readInt("i6");
        i7 = reader.readInt("i7");
        i8 = reader.readInt("i8");
        i9 = reader.readInt("i9");
        i10 = reader.readInt("i10");

        l1 = reader.readLong("l1");
        l2 = reader.readLong("l2");
        l3 = reader.readLong("l3");
        l4 = reader.readLong("l4");
        l5 = reader.readLong("l5");
        l6 = reader.readLong("l6");
        l7 = reader.readLong("l7");
        l8 = reader.readLong("l8");
        l9 = reader.readLong("l9");
        l10 = reader.readLong("l10");

        s1 = reader.readUTF("s1");
        s2 = reader.readUTF("s2");
        s3 = reader.readUTF("s3");
        s4 = reader.readUTF("s4");
        s5 = reader.readUTF("s5");
        s6 = reader.readUTF("s6");
        s7 = reader.readUTF("s7");
        s8 = reader.readUTF("s8");
        s9 = reader.readUTF("s9");
        s10 = reader.readUTF("s10");
    }
}
