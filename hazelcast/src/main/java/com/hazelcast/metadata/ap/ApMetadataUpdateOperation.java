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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ApMetadataUpdateOperation extends AbstractApMetadataOperation {
    /** Metadata key. */
    private Object key;

    /** Value to be set (null for drop). */
    private Object value;

    /** {@code true} if the operation should be ignored in case of existence conflict. */
    private boolean ignoreOnExistenceConflict;

    public ApMetadataUpdateOperation() {
        // No-op.
    }

    public ApMetadataUpdateOperation(Object key, Object value, boolean ignoreOnExistenceConflict) {
        this.key = key;
        this.value = value;
        this.ignoreOnExistenceConflict = ignoreOnExistenceConflict;
    }

    @Override
    public void run() throws Exception {
        ApMetadataStorage storage = getService();

        storage.updateLocally(key, value, ignoreOnExistenceConflict);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(key);
        out.writeObject(value);
        out.writeBoolean(ignoreOnExistenceConflict);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        key = in.readObject();
        value = in.readObject();
        ignoreOnExistenceConflict = in.readBoolean();
    }
}
