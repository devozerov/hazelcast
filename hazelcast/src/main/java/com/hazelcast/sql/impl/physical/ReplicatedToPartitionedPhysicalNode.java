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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.hash.HashFunction;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.Objects;

/**
 * Converts replicated input into partitioned.
 */
public class ReplicatedToPartitionedPhysicalNode extends UniInputPhysicalNode {
    /** Function which should be used for hashing. */
    private HashFunction hashFunction;

    public ReplicatedToPartitionedPhysicalNode() {
        // No-op.
    }

    public ReplicatedToPartitionedPhysicalNode(int id, PhysicalNode upstream, HashFunction hashFunction) {
        super(id, upstream);

        this.hashFunction = hashFunction;
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }

    @Override
    public void visit0(PhysicalNodeVisitor visitor) {
        visitor.onReplicatedToPartitionedNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeObject(hashFunction);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        hashFunction = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, upstream, hashFunction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicatedToPartitionedPhysicalNode that = (ReplicatedToPartitionedPhysicalNode) o;

        return id == that.id && upstream.equals(that.upstream) && hashFunction.equals(that.hashFunction);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", hashFunction=" + hashFunction + ", upstream=" + upstream + '}';
    }
}
