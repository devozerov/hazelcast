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
import com.hazelcast.sql.impl.exec.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Node to scan a partitioned map.
 */
public class MapIndexScanPhysicalNode extends AbstractMapScanPhysicalNode {
    /** Index name. */
    private String indexName;

    /** Index filter. */
    private IndexFilter indexFilter;

    public MapIndexScanPhysicalNode() {
        // No-op.
    }

    public MapIndexScanPhysicalNode(
        int id,
        String mapName,
        List<String> fieldNames,
        List<DataType> fieldTypes,
        List<Integer> projects,
        String indexName,
        IndexFilter indexFilter,
        Expression<Boolean> remainderFilter
    ) {
        super(id, mapName, fieldNames, fieldTypes, projects, remainderFilter);

        this.indexName = indexName;
        this.indexFilter = indexFilter;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onMapIndexScanNode(this);
    }

    @Override
    protected void writeData0(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeUTF(indexName);
        indexFilter.writeData(out);
    }

    @Override
    protected void readData0(ObjectDataInput in) throws IOException {
        super.readData(in);

        indexName = in.readUTF();

        indexFilter = new IndexFilter();
        indexFilter.readData(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, mapName, fieldNames, projects, indexName, indexFilter, filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapIndexScanPhysicalNode that = (MapIndexScanPhysicalNode) o;

        return id == that.id
            && mapName.equals(that.mapName)
            && fieldNames.equals(that.fieldNames)
            && projects.equals(that.projects)
            && indexName.equals(that.indexName)
            && indexFilter.equals(that.indexFilter)
            && Objects.equals(filter, that.filter);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", mapName=" + mapName + ", fieldNames=" + fieldNames
            + ", projects=" + projects + ", indexName=" + indexName + ", indexFilter=" + indexFilter
            + ", remainderFilter=" + filter + '}';
    }
}
