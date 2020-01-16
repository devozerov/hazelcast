package com.hazelcast.projectx.jmh;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.projectx.jmh.model.BigPortable;
import com.hazelcast.projectx.jmh.model.BigSerializable;
import com.hazelcast.projectx.jmh.model.BigTransportable;
import com.hazelcast.projectx.jmh.model.JmhPortableFactory;
import com.hazelcast.projectx.jmh.model.SmallPortable;
import com.hazelcast.projectx.jmh.model.SmallSerializable;
import com.hazelcast.projectx.jmh.model.SmallTransportable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.HashMap;
import java.util.Map;

@State(Scope.Benchmark)
public class BenchmarkState {
    public static String MAP_P = "mp";
    public static String MAP_T = "mt";

    private static final int ENTRY_CNT = 10_000;

    public static SmallSerializable S_SMALL = SmallSerializable.generate();
    public static SmallPortable P_SMALL = SmallPortable.generate();
    public static SmallTransportable T_SMALL = SmallTransportable.generate();

    public static BigSerializable S_BIG = BigSerializable.generate();
    public static BigPortable P_BIG = BigPortable.generate();
    public static BigTransportable T_BIG = BigTransportable.generate();

    public HazelcastInstance instance;
    public InternalSerializationService ss;

    public Data sSmallData;
    public Data pSmallData;
    public Data tSmallData;

    public Data sBigData;
    public Data pBigData;
    public Data tBigData;

    public IMap<Long, BigPortable> MAP_PORTABLE;
    public IMap<Long, BigTransportable> MAP_TRANSPORTABLE;

    public Predicate PREDICATE;

    @Setup
    public void setup() {
        Map<Integer, PortableFactory> portableFactories = new HashMap<>();
        portableFactories.put(1, new JmhPortableFactory());

        Config confg = new Config().setSerializationConfig(
            new SerializationConfig().setPortableFactories(portableFactories)
        );

        confg.addMapConfig(new MapConfig(MAP_P).setInMemoryFormat(InMemoryFormat.BINARY));
        confg.addMapConfig(new MapConfig(MAP_T).setInMemoryFormat(InMemoryFormat.BINARY));

        instance = Hazelcast.newHazelcastInstance(confg);
        ss = ((HazelcastInstanceProxy)instance).getOriginal().getSerializationService();

        sSmallData = ss.toData(S_SMALL);
        pSmallData = ss.toData(P_SMALL);
        tSmallData = ss.toData(T_SMALL);

        sBigData = ss.toData(S_BIG);
        pBigData = ss.toData(P_BIG);
        tBigData = ss.toData(T_BIG);

        System.out.println(">>> S_SMALL: " + sSmallData.dataSize());
        System.out.println(">>> P_SMALL: " + pSmallData.dataSize());
        System.out.println(">>> T_SMALL: " + tSmallData.dataSize());

        System.out.println(">>> S_BIG: " + sBigData.dataSize());
        System.out.println(">>> P_BIG: " + pBigData.dataSize());
        System.out.println(">>> T_BIG: " + tBigData.dataSize());

        MAP_PORTABLE = instance.getMap(MAP_P);
        MAP_TRANSPORTABLE = instance.getMap(MAP_T);

        for (int i = 0; i < ENTRY_CNT; i++) {
            MAP_PORTABLE.put((long) i, BigPortable.generate());
            MAP_TRANSPORTABLE.put((long) i, BigTransportable.generate());
        }

        PREDICATE = Predicates.or(
            Predicates.equal("i5", 0),
            Predicates.equal("l5", 0L)
        );
    }

    @TearDown
    public void tearDown() {
        instance.shutdown();
    }
}
