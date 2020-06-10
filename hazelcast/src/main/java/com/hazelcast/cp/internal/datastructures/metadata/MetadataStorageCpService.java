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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.metadata.MetadataStorage;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MetadataStorageCpService implements RaftManagedService, SnapshotAwareService<MetadataStorageCp> {

    public static final String METADATA_STORE_GROUP_NAME = "@metadata_storage";
    public static final String SERVICE_NAME = "hz:raft:metadataStorageService";

    private final ILogger logger;
    private final NodeEngine nodeEngine;

    private final Map<CPGroupId, MetadataStorageCp> storage = new ConcurrentHashMap<>();

    private RaftService raftService;

    public MetadataStorageCpService(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(getClass().getName());
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void onCPSubsystemRestart() {
        storage.clear();
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);

        if (raftService.isCpSubsystemEnabled()) {
            nodeEngine.getExecutionService().schedule(this::registerMetadataStorage, 1, TimeUnit.SECONDS);
        }
    }

    private void registerMetadataStorage() {
        if (raftService.isCpSubsystemEnabled() && raftService.isDiscoveryCompleted()) {
            logger.info("Registering CP Metadata Storage");
            getMetadataStorage(); //force creation of group
        } else {
            nodeEngine.getExecutionService().schedule(this::registerMetadataStorage, 1, TimeUnit.SECONDS);
        }
    }

    public MetadataStorage getMetadataStorage() {
        RaftGroupId group = this.raftService.createRaftGroupForProxy(METADATA_STORE_GROUP_NAME);
        return new MetadataStorageCpProxy(this.nodeEngine, group);
    }

    public MetadataStorageCp getMetadataStorageState(CPGroupId groupId) {
        return storage.computeIfAbsent(groupId, key -> new MetadataStorageCp());
    }

    @Override
    public MetadataStorageCp takeSnapshot(CPGroupId groupId, long commitIndex) {
        return new MetadataStorageCp(storage.get(groupId));
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataStorageCp snapshot) {
        storage.put(groupId, new MetadataStorageCp(snapshot));
    }

    @Override
    public void reset() {
        if (!raftService.isCpSubsystemEnabled()) {
            storage.clear();
        }
    }

    @Override
    public void shutdown(boolean terminate) {

    }
}
