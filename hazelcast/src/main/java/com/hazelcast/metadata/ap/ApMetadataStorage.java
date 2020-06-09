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

import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.metadata.MetadataStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Metadata service with relaxed consistency guarantees.
 */
public class ApMetadataStorage implements MetadataStorage, CoreService, ManagedService, PreJoinAwareService,
    SplitBrainHandlerService {

    public static final String SERVICE_NAME = "AP_METADATA_STORE";

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentHashMap<Object, Object> entries = new ConcurrentHashMap<>();

    public ApMetadataStorage(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Object get(Object key) {
        return entries.get(key);
    }

    @Override
    public Map<Object, Object> getWithFilter(PredicateEx<Object> filter) {
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

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        System.out.println(">>> INTT");
        // No-op.
    }

    @Override
    public void reset() {
        entries.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        // No-op.
    }

    @Override
    public Operation getPreJoinOperation() {
        // TODO: Share configs on pre-join
        return null;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        // TODO: Handle split-brain
        return null;
    }

    private void update(Object ket, Object value, boolean ignoreOnExistenceConflict) {
        // TODO: Implement update
    }
}
