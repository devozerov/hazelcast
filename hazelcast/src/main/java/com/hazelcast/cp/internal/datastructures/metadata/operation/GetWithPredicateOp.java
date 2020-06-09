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
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCp;
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCpService;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.function.Predicate;

public class GetWithPredicateOp extends AbstractMetadataOp {

    private Data predicate;

    public GetWithPredicateOp() {
    }

    public GetWithPredicateOp(Data predicate) {
        this.predicate = predicate;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) throws Exception {
        MetadataStorageCp storage = getStorage(groupId);
        Predicate<Object> p = toObject(predicate);
        return storage.getWithFilter(p);
    }

    @Override
    protected String getServiceName() {
        return MetadataStorageCpService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = IOUtil.readData(in);
    }

}
