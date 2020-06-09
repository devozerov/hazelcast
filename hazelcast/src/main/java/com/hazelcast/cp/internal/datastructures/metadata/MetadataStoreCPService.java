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

import java.util.Properties;

public class MetadataStoreCPService implements RaftManagedService, SnapshotAwareService<MetadataStorage> {

    public static final String METADATA_STORE_GROUP_NAME = "__metadata_store@metadata_store";
    public static final String SERVICE_NAME = "hz:raft:metadataStoreService";

    private final ILogger logger;

    private RaftService raftService;
    private RaftGroupId groupId;

    public MetadataStoreCPService(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(getClass().getName());
    }

    @Override
    public void onCPSubsystemRestart() {

    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    public MetadataStorage getMetadataStore() {
        RaftGroupId group = this.raftService.createRaftGroupForProxy(METADATA_STORE_GROUP_NAME);
        return new MetadataStorageCPImpl(group);

    }

    @Override
    public MetadataStorage takeSnapshot(CPGroupId groupId, long commitIndex) {
        return null;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataStorage snapshot) {

    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }
}
