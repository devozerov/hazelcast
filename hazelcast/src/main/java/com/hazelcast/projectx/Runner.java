package com.hazelcast.projectx;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.projectx.portable.Factory;
import com.hazelcast.projectx.portable.PortablePerson;
import com.hazelcast.projectx.transportable.TransportablePerson;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.PortableGetter;
import com.hazelcast.query.impl.getters.TransportableGetter;

import java.util.Collection;
import java.util.Collections;

public class Runner {
    private static final int ENTRY_COUNT = 10;

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        config.setSerializationConfig(
            new SerializationConfig().setPortableFactories(Collections.singletonMap(1, new Factory()))
        );

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        try {
            InternalSerializationService ss =((HazelcastInstanceProxy)instance).getOriginal().getSerializationService();

            doPortable(ss);
            doTransportable(ss);

            doTransportableQuery(instance);
        } finally {
            instance.shutdown();
        }
    }

    private static void doTransportableQuery(HazelcastInstance instance) {
        IMap<Long, TransportablePerson> map = instance.getMap("tp");

        map.put(1L, new TransportablePerson(1L, 1L, "Hazel", "Cast"));

        Collection<TransportablePerson> persons = map.values(Predicates.equal("firstName", "Hazel"));

        System.out.println(">>> QUERY: " + persons);
    }

    private static void doPortable(InternalSerializationService ss) throws Exception {
        PortablePerson person = new PortablePerson(1, 2, "Hazel", "Cast");

        Data data = ss.toData(person);
        System.out.println(">>> Portable size: " + data.dataSize());

        PortableGetter portableGetter = new PortableGetter(ss);
        System.out.println(">>> Portable field: " + portableGetter.getValue(data, "firstName"));
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
}
