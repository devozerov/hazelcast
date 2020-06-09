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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.metadata.MetadataStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

// state for MetadataStorage
public class MetadataStorageCP {

    private final Map<Object, Object> entries = new HashMap<>();

    public Object get(Object key) {
        return entries.get(key);
    }

    public Map<Object, Object> getWithFilter(Predicate<Object> filter) {
        Map<Object, Object> res = new HashMap<>();
        entries.forEach((k, v) -> {
            if (filter.test(k)) {
                res.put(k, v);
            }
        });
        return res;
    }

    public void update(Object key, Object value, boolean ignoreOnConflict) {
        boolean drop = value == null;

        if (drop) {
            Object oldValue = entries.remove(key);
            if (oldValue == null && !ignoreOnConflict) {
                throw new HazelcastException("Entry doesn't exist: " + key);
            }
        } else {
            Object oldValue = entries.putIfAbsent(key, value);
            if (oldValue != null && !ignoreOnConflict) {
                throw new HazelcastException("Entry already exists: " + key);
            }
        }
    }
}
