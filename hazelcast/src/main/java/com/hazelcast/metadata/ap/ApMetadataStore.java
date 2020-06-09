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

import com.hazelcast.metadata.MetadataStore;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Metadata service with relaxed consistency guarantees.
 */
// TODO: Implement merge on join
public class ApMetadataStore implements MetadataStore {

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentHashMap<Object, Object> entries = new ConcurrentHashMap<>();

    public ApMetadataStore(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Object get(Object key) {
        return entries.get(key);
    }

    @Override
    public Map<Object, Object> getWithFilter(Predicate<Object> filter) {
        HashMap<Object, Object> res = new HashMap<>();

        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            if (filter.test(entry.getKey())) {
                res.put(entry.getKey(), entry.getValue());
            }
        }

        return res;
    }

    @Override
    public void create(Object key, Object value, boolean ifNotExists) {
        update(key, value, ifNotExists);
    }

    @Override
    public void drop(Object key, boolean ifExists) {
        update(key, null, ifExists);
    }

    private void update(Object ket, Object value, boolean ignoreOnExistenceConflict) {
        // TODO: Implement update
    }
}
