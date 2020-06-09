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

package com.hazelcast.cp.internal.datastructures.metadata.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCp;
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCpService;
import com.hazelcast.internal.serialization.Data;

public abstract class AbstractMetadataOp extends RaftOp {

    MetadataStorageCp getStorage(CPGroupId groupId) {
        MetadataStorageCpService service = getService();
        return service.getMetadataStoreState(groupId);
    }

    @Override
    protected String getServiceName() {
        return MetadataStorageCpService.SERVICE_NAME;
    }

    <T> T toObject(Data data) {
        return getNodeEngine().getSerializationService().toObject(data);
    }

    Data toData(Object object) {
        return getNodeEngine().getSerializationService().toData(object);
    }
}
