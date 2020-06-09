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
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.metadata.operation.CreateOp;
import com.hazelcast.cp.internal.datastructures.metadata.operation.DropOp;
import com.hazelcast.cp.internal.datastructures.metadata.operation.GetOp;
import com.hazelcast.cp.internal.datastructures.metadata.operation.GetWithPredicateOp;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.metadata.MetadataStorage;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Map;

// proxy for MetadataStore
public class MetadataStorageProxy implements MetadataStorage {

    private final RaftInvocationManager invocationManager;
    private final SerializationService serializationService;
    private RaftGroupId group;

    public MetadataStorageProxy(NodeEngine nodeEngine, RaftGroupId group) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.serializationService = nodeEngine.getSerializationService();
        this.group = group;
    }

    @Override
    public Object get(Object key) {
        Object response = invocationManager.invoke(group, new GetOp(toData(key))).joinInternal();
        // TODO: deserialize
        return response;
    }

    @Override
    public Map<Object, Object> getWithFilter(PredicateEx<Object> filter) {
        Object response = invocationManager.invoke(group, new GetWithPredicateOp(toData(filter))).joinInternal();
        // TODO: deserialize
        return (Map<Object, Object>) response;
    }

    @Override
    public void create(Object key, Object value, boolean ifNotExists) {
        invocationManager.invoke(group, new CreateOp(ifNotExists, toData(key), toData(value))).joinInternal();
    }

    @Override
    public void drop(Object key, boolean ifExists) {
        invocationManager.invoke(group, new DropOp(ifExists, toData(key))).joinInternal();
    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }
}
