package com.hazelcast.projectx.jmh;

import com.hazelcast.projectx.jmh.model.BigPortable;
import com.hazelcast.projectx.jmh.model.BigSerializable;
import com.hazelcast.projectx.jmh.model.BigTransportable;
import com.hazelcast.projectx.jmh.model.SmallPortable;
import com.hazelcast.projectx.jmh.model.SmallSerializable;
import com.hazelcast.projectx.jmh.model.SmallTransportable;
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

public class DeserializeBenchmark {
    @Benchmark
    public SmallSerializable deserializeSerializableSmall(BenchmarkState state) {
        return (SmallSerializable) state.ss.toObject(state.sSmallData);
    }

    @Benchmark
    public SmallPortable deserializePortableSmall(BenchmarkState state) {
        return (SmallPortable) state.ss.toObject(state.pSmallData);
    }

    @Benchmark
    public SmallTransportable deserializeTransportableSmall(BenchmarkState state) {
        return (SmallTransportable) state.ss.toObject(state.tSmallData);
    }

    @Benchmark
    public BigSerializable deserializeSerializableBig(BenchmarkState state) {
        return (BigSerializable) state.ss.toObject(state.sBigData);
    }

    @Benchmark
    public BigPortable deserializePortableBig(BenchmarkState state) {
        return (BigPortable) state.ss.toObject(state.pBigData);
    }

    @Benchmark
    public BigTransportable deserializeTransportableBig(BenchmarkState state) {
        return (BigTransportable) state.ss.toObject(state.tBigData);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(DeserializeBenchmark.class.getSimpleName())
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
