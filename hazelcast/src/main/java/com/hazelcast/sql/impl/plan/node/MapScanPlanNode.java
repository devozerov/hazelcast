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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;

/**
 * Node to scan a partitioned map.
 */
public class MapScanPlanNode extends AbstractMapScanPlanNode implements IdentifiedDataSerializable {

    private boolean replicated;

    public MapScanPlanNode() {
        // No-op.
    }

    public MapScanPlanNode(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id, mapName, keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, filter);
    }

    public MapScanPlanNode(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        boolean replicated
    ) {
        super(id, mapName, keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, filter);

        this.replicated = replicated;
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onMapScanNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_MAP_SCAN;
    }

    public boolean isReplicated() {
        return replicated;
    }

    @Override
    protected void writeData0(ObjectDataOutput out) throws IOException {
        super.writeData0(out);

        out.writeBoolean(replicated);
    }

    @Override
    protected void readData0(ObjectDataInput in) throws IOException {
        super.readData0(in);

        replicated = in.readBoolean();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", mapName=" + mapName + ", fieldPaths=" + fieldPaths
            + ", projects=" + projects + ", filter=" + filter + '}';
    }
}
