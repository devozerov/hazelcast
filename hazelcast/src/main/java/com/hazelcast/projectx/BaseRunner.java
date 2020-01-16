package com.hazelcast.projectx;

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
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.TransportableGetter;

import java.util.Collection;
import java.util.Collections;

public class BaseRunner {
    private static final String TRANSPORTABLE_MAP = "tp";

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        config.setSerializationConfig(
            new SerializationConfig().setPortableFactories(Collections.singletonMap(1, new PortablePersonFactory()))
        );

        config.addMapConfig(new MapConfig().setName(TRANSPORTABLE_MAP).setInMemoryFormat(InMemoryFormat.BINARY));

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        try {
            InternalSerializationService ss =((HazelcastInstanceProxy)instance).getOriginal().getSerializationService();

            doSerializable(ss);
            doPortable(ss);
            doTransportable(ss);

            doTransportableQuery(instance);
        } finally {
            instance.shutdown();
        }
    }

    private static void doSerializable(InternalSerializationService ss)  {
        SerializablePerson person = new SerializablePerson(1, 2, "Hazel", "Cast");

        Data data = ss.toData(person);
        System.out.println(">>> Serializable size: " + data.dataSize());
    }

    private static void doPortable(InternalSerializationService ss)  {
        PortablePerson person = new PortablePerson(1, 2, "Hazel", "Cast");

        Data data = ss.toData(person);
        System.out.println(">>> Portable size: " + data.dataSize());
    }

    private static void doTransportable(InternalSerializationService ss) throws Exception {
        TransportablePerson person = new TransportablePerson(1, 2, "Hazel", "Cast");

        Data data = ss.toData(person);
        System.out.println(">>> Transportable size: " + data.dataSize());

        System.out.println(">>> Original         : " + person);

        TransportablePerson personRestored = ss.toObject(data);
        System.out.println(">>> Restored (deser) : " + personRestored);

        TransportableGetter portableGetter = new TransportableGetter(ss);

        long id = (long)portableGetter.getValue(data, "id");
        long departmentId = (long)portableGetter.getValue(data, "departmentId");
        String firstName = (String)portableGetter.getValue(data, "firstName");
        String lastName = (String)portableGetter.getValue(data, "lastName");

        TransportablePerson personRetoredThroughGetter = new TransportablePerson(id, departmentId, firstName, lastName);

        System.out.println(">>> Restored (getter): " + personRetoredThroughGetter);
    }

    private static void doTransportableQuery(HazelcastInstance instance) {
        IMap<Long, TransportablePerson> map = instance.getMap(TRANSPORTABLE_MAP);

        map.put(1L, new TransportablePerson(1L, 1L, "Hazel", "Cast"));

        Collection<TransportablePerson> persons = map.values(Predicates.equal("firstName", "Hazel"));

        System.out.println(">>> QUERY: " + persons);
    }
}
