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

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.metadata.MetadataStorage;

import java.util.Map;
import java.util.function.Predicate;

// state machine implementation for MetadataStore
public class MetadataStorageCPImpl implements MetadataStorage {

    private RaftGroupId group;

    public MetadataStorageCPImpl(RaftGroupId group) {
        this.group = group;
    }

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> getWithFilter(PredicateEx<Object> filter) {
        return null;
    }

    @Override
    public void create(Object key, Object value, boolean ifNotExists) {

    }

    @Override
    public void drop(Object key, boolean isExists) {

    }
}
