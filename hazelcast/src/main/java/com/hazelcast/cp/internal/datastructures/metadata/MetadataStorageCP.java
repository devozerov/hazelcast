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

package com.hazelcast.cp.internal.datastructures.metadata;

import com.hazelcast.function.PredicateEx;
import com.hazelcast.metadata.MetadataStorage;

import java.util.HashMap;
import java.util.Map;

// state for MetadataStorage
public class MetadataStorageCP implements MetadataStorage {

    private final Map<Object, Object> entries = new HashMap<>();

    public Object get(Object key) {
        return entries.get(key);
    }

    @Override
    public Map<Object, Object> getWithFilter(PredicateEx<Object> filter) {
        Map<Object, Object> res = new HashMap<>();
        entries.forEach((k, v) -> {
            if (filter.test(k)) {
                res.put(k, v);
            }
        });
        return res;
    }

    @Override
    public void create(Object key, Object value, boolean ifNotExists) {
        if (ifNotExists) {
            entries.putIfAbsent(key, value);
        } else {
            entries.put(key, value);
        }
    }

    @Override
    public void drop(Object key, boolean ifExists) {
        if (ifExists && !entries.containsKey(key))  {
            throw new IllegalStateException(key + " was not present");
        }
        entries.remove(key);
    }

}
