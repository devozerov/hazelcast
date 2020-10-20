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

package com.hazelcast.sql.projectx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.SqlTestInstanceFactory;
import com.hazelcast.sql.impl.SqlTestSupport;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class SqlReplicatedMapTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String REPLICATED_MAP_NAME = "rmap";

    private SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    private HazelcastInstance member1;
    private HazelcastInstance member2;

    @Before
    public void before() throws Exception {
        member1 = factory.newHazelcastInstance();
        member2 = factory.newHazelcastInstance();

        ReplicatedMap<Integer, Integer> rmap = member1.getReplicatedMap(REPLICATED_MAP_NAME);
        rmap.put(1, 1);
    }

    @Test
    public void test() {
        ReplicatedMap<Integer, Integer> rmap1 = member1.getReplicatedMap(REPLICATED_MAP_NAME);
        Integer value1 = rmap1.get(1);
        assertEquals(Integer.valueOf(1), value1);

        ReplicatedMap<Integer, Integer> rmap2 = member2.getReplicatedMap(REPLICATED_MAP_NAME);
        Integer value2 = rmap2.get(1);
        assertEquals(Integer.valueOf(1), value2);

        assertEquals(1, rmap1.size());
        assertEquals(1, rmap2.size());

        rmap1.clear();

        assertEquals(0, rmap1.size());
        assertEquals(0, rmap2.size());
    }
}
