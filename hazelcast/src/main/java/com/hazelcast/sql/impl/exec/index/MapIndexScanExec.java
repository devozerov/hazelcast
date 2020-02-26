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

package com.hazelcast.sql.impl.exec.index;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.exec.AbstractMapScanExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Index scan executor.
 */
//  TODO: Make sure to support merge joins!
@SuppressWarnings("rawtypes")
public class MapIndexScanExec extends AbstractMapScanExec {
    /** Underlying map. */
    private final MapProxyImpl map;

    /** Partitions to be scanned. */
    private final PartitionIdSet parts;

    /** Index name. */
    private final String indexName;

    /** Index filter. */
    private final IndexFilter indexFilter;

    /** All rows fetched on first access. */
    private Collection<Row> rows;

    /** Iterator over rows. */
    private Iterator<Row> rowsIter;

    /** Current row. */
    private Row currentRow;

    public MapIndexScanExec(
        int id,
        MapProxyImpl map,
        PartitionIdSet parts,
        List<String> fieldNames,
        List<DataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        String indexName,
        IndexFilter indexFilter
    ) {
        super(id, map.getName(), fieldNames, fieldTypes, projects, filter);

        this.map = map;
        this.parts = parts;
        this.indexName = indexName;
        this.indexFilter = indexFilter;
    }

    @Override
    public IterationResult advance0() {
        if (rows == null) {
            rows = new ArrayList<>();

            // TODO: Remember to check for migration stamp before returning iteration result.
            Set<QueryableEntry> entries = getIndexEntries();

            for (QueryableEntry entry : entries) {
                Record record = entry.getRecord();

                Data keyData =  entry.getKeyData();
                Object valData = record.getValue();

                Object key = serializationService.toObject(keyData);
                Object val = valData instanceof Data ? serializationService.toObject(valData) : valData;

                HeapRow row = prepareRow(key, val);

                if (row != null) {
                    rows.add(row);
                }
            }

            rowsIter = rows.iterator();
        }

        if (rowsIter.hasNext()) {
            currentRow = rowsIter.next();

            return IterationResult.FETCHED;
        } else {
            currentRow = null;

            return IterationResult.FETCHED_DONE;
        }
    }

    private Set<QueryableEntry> getIndexEntries() {
        // TODO: Either obtain the index in advance, or check for it's existence here.
        MapContainer mapContainer = map.getMapServiceContext().getMapContainer(mapName);
        InternalIndex index = mapContainer.getIndexes().getIndex(indexName);

        // TODO: Unsafe conversion. Need to check whether the entry on planning stage?
        Comparable value = (Comparable) indexFilter.getValue();

        // TODO: Return an iterator here. No sets!
        Set<QueryableEntry> res;

        switch (indexFilter.getType()) {
            case GREATER_THAN:
                res = index.getRecords(Comparison.GREATER, value);

                break;

            case GREATER_THAN_OR_EQUAL:
                res = index.getRecords(Comparison.GREATER_OR_EQUAL, value);

                break;

            case LESS_THAN:
                res = index.getRecords(Comparison.LESS, value);

                break;

            case LESS_THAN_OR_EQUAL:
                res = index.getRecords(Comparison.LESS_OR_EQUAL, value);

                break;

            default:
                assert indexFilter.getType() == IndexFilterType.EQUALS;

                res = index.getRecords(value);
        }

        return res;
    }

    @Override
    public RowBatch currentBatch0() {
        return currentRow;
    }

    @Override
    protected void reset0() {
        rows = null;
        rowsIter = null;
        currentRow = null;
    }

    @Override
    protected Extractors createExtractors() {
        return map.getMapServiceContext().getExtractors(mapName);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldNames=" + fieldNames + ", projects=" + projects
            + "indexName=" + indexName + ", indexFilter=" + indexFilter + ", remainderFilter=" + filter
            + ", partitionCount=" + parts.size() + '}';
    }
}
