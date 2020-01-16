package com.hazelcast.projectx.jmh.model;

import com.hazelcast.projectx.transportable.Transportable;

public class SmallTransportable implements Transportable {
    private int i1;
    private int i2;
    private long l1;
    private long l2;
    private String s1;
    private String s2;

    public static SmallTransportable generate() {
        return new SmallTransportable(
            ModelUtils.randomInt(),
            ModelUtils.randomInt(),
            ModelUtils.randomLong(),
            ModelUtils.randomLong(),
            ModelUtils.randomString(),
            ModelUtils.randomString()
        );
    }

    public SmallTransportable() {
        // No-op.
    }

    public SmallTransportable(int i1, int i2, long l1, long l2, String s1, String s2) {
        this.i1 = i1;
        this.i2 = i2;
        this.l1 = l1;
        this.l2 = l2;
        this.s1 = s1;
        this.s2 = s2;
    }
}
