/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.metadata;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCpProxy;
import com.hazelcast.cp.internal.datastructures.metadata.operation.GetWithPredicateOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.metadata.MetadataStorageApTest.MetadataKey;
import com.hazelcast.metadata.MetadataStorageApTest.MetadataValue;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetadataStorageCpTest extends HazelcastRaftTestSupport {

    private static final int SNAPSHOT_THRESHOLD = 100;
    private HazelcastInstance member1;
    private HazelcastInstance member2;
    private HazelcastInstance member3;

    @Before
    public void setup() {
        HazelcastInstance[] instances = newInstances(3);

        member1 = instances[0];
        member2 = instances[1];
        member3 = instances[2];

    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
              .setCommitIndexAdvanceCountToSnapshot(SNAPSHOT_THRESHOLD);
        return config;
    }

    @Test
    public void testCreateOnStart() {
        assertTrueEventually(() -> {
            CPSubsystemManagementService cp = member1.getCPSubsystem().getCPSubsystemManagementService();
            CPGroup group = cp.getCPGroup("metadata_storage").toCompletableFuture().get();
            assertNotNull("metadata_storage group is missing", group);
        });
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
        Assert.assertEquals(value1, getStore(member1).get(key));
        Assert.assertEquals(value1, getStore(member2).get(key));

        // Create with without ignore flag
        try {
            getStore(member1).create(key, value2, false);

            fail("Must fail");
        } catch (HazelcastException e) {
            assertTrue(e.getMessage().startsWith("Entry already exists"));

            Assert.assertEquals(value1, getStore(member1).get(key));
            Assert.assertEquals(value1, getStore(member2).get(key));
        }

        // No-op on create with ignore flag
        getStore(member1).create(key, value1, true);
        Assert.assertEquals(value1, getStore(member1).get(key));
        Assert.assertEquals(value1, getStore(member2).get(key));

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

        Assert.assertEquals(entries, getStore(member2).getWithFilter(filter));
    }


    @Test
    public void testSnapshotRestore() throws ExecutionException, InterruptedException {
        MetadataStorage store = getStore(member1);
        store.create(new MetadataKey(-1), new MetadataValue(-1), false);

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            store.create(new MetadataKey(i), new MetadataValue(i), false);
        }

        // shutdown the last instance
        member3.shutdown();


        // create the new instance and promote to CP
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        assertTrueEventually(() -> {
            try {
                MetadataStorageCpProxy proxy = (MetadataStorageCpProxy) getStore(instance);
                CPGroupId group = proxy.getGroupId();
                Data p = Accessors.getSerializationService(instance).toData(PredicateEx.alwaysTrue());
                // must query the new member, or it can just ask one of the old members
                InternalCompletableFuture<Object> resp = getRaftInvocationManager(instance).queryLocally(group, new GetWithPredicateOp(p), QueryPolicy.ANY_LOCAL);

                Map<Object, Object> values = (Map<Object, Object>) resp.joinInternal();
                assertEquals(SNAPSHOT_THRESHOLD + 1, values.size());
            } catch (CPSubsystemException ex) {
                // Raft node may not be created yet...
                throw new AssertionError(ex);
            }
        });
    }

    private static MetadataStorage getStore(HazelcastInstance instance) {
        return instance.getCPSubsystem().getMetadataStore();
    }
}
