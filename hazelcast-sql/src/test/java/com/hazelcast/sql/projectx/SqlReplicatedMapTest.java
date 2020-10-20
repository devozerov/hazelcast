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

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlTestInstanceFactory;
import com.hazelcast.sql.impl.SqlTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class SqlReplicatedMapTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

    private HazelcastInstance member1;
    private HazelcastInstance member2;

    @Before
    public void before() {
        Config config = smallInstanceConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        member1 = factory.newHazelcastInstance(config);
        member2 = factory.newHazelcastInstance(config);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testBase() {
        ReplicatedMap<Integer, Integer> map1 = member1.getReplicatedMap(MAP_NAME);
        map1.put(1, 1);
        Integer value1 = map1.get(1);
        assertEquals(Integer.valueOf(1), value1);

        ReplicatedMap<Integer, Integer> map2 = member2.getReplicatedMap(MAP_NAME);
        Integer value2 = map2.get(1);
        assertEquals(Integer.valueOf(1), value2);

        assertEquals(1, map1.size());
        assertEquals(1, map2.size());

        map1.clear();

        assertEquals(0, map1.size());
        assertEquals(0, map2.size());
    }

    @Test
    public void testSql_direct() {
        checkSql(false);
    }

    @Test
    public void testSql_index() {
        checkSql(true);
    }

    private void checkSql(boolean useIndex) {
        ReplicatedMap<Integer, Integer> map = member1.getReplicatedMap(MAP_NAME);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        if (useIndex) {
            map.addIndex(new IndexConfig().setName("idx_this").addAttribute("this"));
        }

        checkSql(member1);
        checkSql(member2);
    }

    private static void checkSql(HazelcastInstance member) {
        try (SqlResult result = member.getSql().execute("SELECT * FROM replicated.map WHERE this < 3")) {
            Map<Integer, Integer> resMap = new HashMap<>();

            for (SqlRow row : result) {
                int key = row.getObject("__key");
                int value = row.getObject("this");

                resMap.put(key, value);
            }

            assertEquals(2, resMap.size());

            assertEquals(Integer.valueOf(1), resMap.get(1));
            assertEquals(Integer.valueOf(2), resMap.get(2));
        }
    }
}
