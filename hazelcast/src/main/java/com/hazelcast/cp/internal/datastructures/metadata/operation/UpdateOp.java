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
import com.hazelcast.cp.internal.datastructures.metadata.MetadataStorageCP;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class UpdateOp extends AbstractMetadataOp {

    private boolean ignoreConflict;

    private Data key;
    private Data value;

    public UpdateOp() {
    }

    public UpdateOp(boolean ignoreConflict, Data key, Data value) {
        this.ignoreConflict = ignoreConflict;
        this.key = key;
        this.value = value;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) throws Exception {
        MetadataStorageCP storage = getStorage(groupId);
        storage.update(toObject(key), toObject(value), ignoreConflict);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(ignoreConflict);
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ignoreConflict = in.readBoolean();
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
    }
}
