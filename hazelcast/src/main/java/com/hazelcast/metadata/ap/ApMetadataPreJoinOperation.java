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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;

public class ApMetadataPreJoinOperation extends ApMetadataUpdateOperation {

    private Map<Object, Object> entries;

    public ApMetadataPreJoinOperation() {
        // No-op.
    }

    public ApMetadataPreJoinOperation(Map<Object, Object> entries) {
        this.entries = entries;
    }

    @Override
    public void run() throws Exception {
        ApMetadataStorage storage = getService();

        storage.addEntriesLocally(entries);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        SerializationUtil.writeMap(entries, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        entries = SerializationUtil.readMap(in);
    }
}
