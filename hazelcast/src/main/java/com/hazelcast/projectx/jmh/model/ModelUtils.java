package com.hazelcast.projectx.jmh.model;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class ModelUtils {
    private ModelUtils() {
        // No-op.
    }

    public static int randomInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    public static long randomLong() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }
}
