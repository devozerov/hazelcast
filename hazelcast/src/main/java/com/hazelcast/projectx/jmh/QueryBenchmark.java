package com.hazelcast.projectx.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

public class QueryBenchmark {

    @SuppressWarnings("unchecked")
    @Benchmark
    public Object testPortable(BenchmarkState state) {
        return state.MAP_PORTABLE.values(state.PREDICATE);
    }
    @SuppressWarnings("unchecked")
    @Benchmark
    public Object testTransportable(BenchmarkState state) {
        return state.MAP_TRANSPORTABLE.values(state.PREDICATE);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(QueryBenchmark.class.getSimpleName())
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
