/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.metadata.ap;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ApMetadataStorageTest {

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstanceProxy member1;
    private HazelcastInstanceProxy member2;

    @Before
    public void before() {
        factory = new TestHazelcastInstanceFactory(2);

        member1 = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        member2 = (HazelcastInstanceProxy) factory.newHazelcastInstance();
    }

    @After
    public void after() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void testNonExistent() {
        assertNull(getStore(member1).get("not_exists"));
    }

    @Test
    public void testCreateDrop() {
        MetadataKey key = new MetadataKey(1);
        MetadataValue value1 = new MetadataValue(2);
        MetadataValue value2 = new MetadataValue(4);

        // Regular create
        getStore(member1).create(key, value1, false);
        assertEquals(value1, getStore(member1).get(key));
        assertEquals(value1, getStore(member2).get(key));

        // Create with without ignore flag
        try {
            getStore(member1).create(key, value2, false);

            fail("Must fail");
        } catch (HazelcastException e) {
            assertTrue(e.getMessage().startsWith("Entry already exists"));

            assertEquals(value1, getStore(member1).get(key));
            assertEquals(value1, getStore(member2).get(key));
        }

        // No-op on create with ignore flag
        getStore(member1).create(key, value1, true);
        assertEquals(value1, getStore(member1).get(key));
        assertEquals(value1, getStore(member2).get(key));

        // Drop
        getStore(member1).drop(key, false);
        assertNull(getStore(member1).get(key));
        assertNull(getStore(member2).get(key));

        // Drop without ignore flag.
        try {
            getStore(member1).drop(key, false);

            fail("Must fail");
        } catch (HazelcastException e) {
            assertTrue(e.getMessage().startsWith("Entry doesn't exist"));
        }

        // Drop with ignore flag
        getStore(member1).drop(key, true);
    }

    @Test
    public void testGetWithFilter() {
        MetadataKey key1 = new MetadataKey(1);
        MetadataKey key2 = new MetadataKey(2);
        MetadataKey key3 = new MetadataKey(3);
        MetadataValue value1 = new MetadataValue(10);
        MetadataValue value2 = new MetadataValue(20);
        MetadataValue value3 = new MetadataValue(30);

        getStore(member1).create(key1, value1, false);
        getStore(member1).create(key2, value2, false);
        getStore(member1).create(key3, value3, false);

        PredicateEx<Object> filter = (PredicateEx<Object>) o -> {
            if (o instanceof MetadataKey) {
                MetadataKey key = (MetadataKey) o;

                return key.getValue() == 1 || key.getValue() == 2;
            }

            return false;
        };

        Map<Object, Object> entries = getStore(member1).getWithFilter(filter);

        assertEquals(2, entries.size());
        assertEquals(value1, entries.get(key1));
        assertEquals(value2, entries.get(key2));

        assertEquals(entries, getStore(member2).getWithFilter(filter));
    }

    @Test
    public void testPropagationOnJoin() {
        MetadataKey key = new MetadataKey(1);
        MetadataValue value = new MetadataValue(2);

        getStore(member1).create(key, value, true);

        HazelcastInstanceProxy member3 = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        MetadataValue restoredValue = (MetadataValue) getStore(member3).get(key);

        assertEquals(value, restoredValue);
    }

    private ApMetadataStorage getStore(HazelcastInstanceProxy instance) {
        return instance.getOriginal().node.getNodeEngine().getApMetadataStore();
    }

    @SuppressWarnings("unused")
    private static final class MetadataKey implements Serializable {

        private int value;

        private MetadataKey() {
            // No-op.
        }

        private MetadataKey(int value) {
            this.value = value;
        }

        private int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MetadataKey that = (MetadataKey) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    @SuppressWarnings("unused")
    private static final class MetadataValue implements Serializable {

        private int value;

        private MetadataValue() {
            // No-op.
        }

        private MetadataValue(int value) {
            this.value = value;
        }

        private int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MetadataValue value1 = (MetadataValue) o;

            return value == value1.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
