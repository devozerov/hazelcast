package com.hazelcast.projectx.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.projectx.jmh.BenchmarkState.P_BIG;
import static com.hazelcast.projectx.jmh.BenchmarkState.P_SMALL;
import static com.hazelcast.projectx.jmh.BenchmarkState.S_BIG;
import static com.hazelcast.projectx.jmh.BenchmarkState.S_SMALL;
import static com.hazelcast.projectx.jmh.BenchmarkState.T_BIG;
import static com.hazelcast.projectx.jmh.BenchmarkState.T_SMALL;

public class SerializeBenchmark {
    @Benchmark
    public Object serializeSerializableSmall(BenchmarkState state) {
        return state.ss.toData(S_SMALL);
    }

    @Benchmark
    public Object serializePortableSmall(BenchmarkState state) {
        return state.ss.toData(P_SMALL);
    }

    @Benchmark
    public Object serializeTransportableSmall(BenchmarkState state) {
        return state.ss.toData(T_SMALL);
    }

    @Benchmark
    public Object serializeSerializableBig(BenchmarkState state) {
        return state.ss.toData(S_BIG);
    }

    @Benchmark
    public Object serializePortableBig(BenchmarkState state) {
        return state.ss.toData(P_BIG);
    }

    @Benchmark
    public Object serializeTransportableBig(BenchmarkState state) {
        return state.ss.toData(T_BIG);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(SerializeBenchmark.class.getSimpleName())
            .mode(Mode.Throughput)
            .warmupIterations(1)
            .measurementIterations(5)
            .timeUnit(TimeUnit.SECONDS)
            .warmupTime(TimeValue.seconds(5))
            .measurementTime(TimeValue.seconds(5))
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
