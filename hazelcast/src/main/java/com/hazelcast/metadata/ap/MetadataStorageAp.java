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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.metadata.MetadataStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static java.util.Collections.singleton;

/**
 * Metadata service with relaxed consistency guarantees.
 */
public class MetadataStorageAp implements MetadataStorage, CoreService, ManagedService, PreJoinAwareService,
    SplitBrainHandlerService {

    public static final String SERVICE_NAME = "AP_METADATA_STORE";
    public static final int RETRY_COUNT = 100;

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentHashMap<Object, Object> entries = new ConcurrentHashMap<>();

    public MetadataStorageAp(NodeEngineImpl nodeEngine) {
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

    private void update(Object key, Object value, boolean ignoreOnExistenceConflict) {
        broadcast(() -> new MetadataStorageApUpdateOperation(key, value, ignoreOnExistenceConflict));
    }

    void updateLocally(Object key, Object value, boolean ignoreOnExistenceConflict) {
        boolean drop = value == null;

        if (drop) {
            Object oldValue = entries.remove(key);

            if (oldValue == null && !ignoreOnExistenceConflict) {
                throw new HazelcastException("Entry doesn't exist: " + key);
            }
        } else {
            Object oldValue = entries.putIfAbsent(key, value);

            if (oldValue != null && !ignoreOnExistenceConflict) {
                throw new HazelcastException("Entry already exists: " + key);
            }
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
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
        HashMap<Object, Object> entries0 = new HashMap<>(entries);

        if (entries0.isEmpty()) {
            return null;
        }

        return new MetadataStorageApPreJoinOperation(entries0);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        HashMap<Object, Object> entries0 = new HashMap<>(entries);

        if (entries0.isEmpty()) {
            return null;
        }

        return new MergeRunnable(entries0);
    }

    void addEntriesLocally(Map<Object, Object> remoteEntries) {
        for (Map.Entry<Object, Object> entry : remoteEntries.entrySet()) {
            entries.putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    private void broadcast(Supplier<? extends Operation> operationSupplier) {
        Future<Object> future = InvocationUtil.invokeOnStableClusterSerial(nodeEngine, operationSupplier, RETRY_COUNT);

        FutureUtil.waitForever(singleton(future), FutureUtil.RETHROW_EVERYTHING);
    }

    private class MergeRunnable implements Runnable {

        private final Map<Object, Object> entries;

        public MergeRunnable(Map<Object, Object> entries) {
            this.entries = entries;
        }

        @Override
        public void run() {
            broadcast(() -> new MetadataStorageApPreJoinOperation(entries));
        }
    }
}
