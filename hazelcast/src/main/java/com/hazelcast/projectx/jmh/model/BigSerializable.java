package com.hazelcast.projectx.jmh.model;

import java.io.Serializable;

public class BigSerializable implements Serializable {
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

    public static BigSerializable generate() {
        return new BigSerializable(
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

    public BigSerializable() {
        // No-op.
    }

    public BigSerializable(int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, long l1, long l2, long l3, long l4, long l5, long l6, long l7, long l8, long l9, long l10, String s1, String s2, String s3, String s4, String s5, String s6, String s7, String s8, String s9, String s10) {
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
}
