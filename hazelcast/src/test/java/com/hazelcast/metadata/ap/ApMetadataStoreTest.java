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

import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ApMetadataStoreTest {

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
    public void test() {
        assertNull(getStore(member1).get("not_exists"));
    }

    private ApMetadataStore getStore(HazelcastInstanceProxy instance) {
        return instance.getOriginal().node.getNodeEngine().getApMetadataStore();
    }
}
