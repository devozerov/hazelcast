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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMetadataTest extends HazelcastRaftTestSupport {

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance[] instances = newInstances(3);

        HazelcastInstance hz = instances[0];
        MetadataStorage store = hz.getCPSubsystem().getMetadataStore();

        for (CPGroupId cpGroupId : hz.getCPSubsystem().getCPSubsystemManagementService().getCPGroupIds()
                                     .toCompletableFuture().join()) {
            System.out.println(cpGroupId);
        }

        for (int i = 0; i < 4096; i++) {
            store.create("key-" +i, "value2", true);
        }
        System.out.println("Getting value: ");
        System.out.println(store.get("key-1"));
        System.out.println(store.getWithFilter(k -> true));
        Thread.currentThread().join();
    }
}
