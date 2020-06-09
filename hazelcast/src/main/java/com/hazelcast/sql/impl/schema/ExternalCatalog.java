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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.connector.SqlConnectorCache;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class ExternalCatalog implements TableResolver {

    private static final List<List<String>> SEARCH_PATHS = singletonList(asList(CATALOG, SCHEMA_NAME_PUBLIC));

    private final NodeEngine nodeEngine;
    private final SqlConnectorCache sqlConnectorCache;

    public ExternalCatalog(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.sqlConnectorCache = new SqlConnectorCache(nodeEngine);
    }

    public void createTable(ExternalTable table, boolean replace, boolean ifNotExists) {
        if (replace) {
            throw QueryException.error("CREATE OR REPLACE is not supported");
        }

        String name = table.name();

        try {
            tables().create(table, ifNotExists);
        } catch (Exception e) {
            throw QueryException.error("'" + name + "' table already exists");
        }
    }

    public void removeTable(String name, boolean ifExists) {
        try {
            tables().drop(name, ifExists);
        } catch (Exception e) {
            throw QueryException.error("'" + name + "' table does not exist", e);
        }
    }

    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Override @Nonnull
    public Collection<Table> getTables() {
        return tables().values().stream()
                       .map(this::toTable)
                       .collect(toList());
    }

    private ExternalCatalogAccessor tables() {
        return new ExternalCatalogAccessor(nodeEngine.getApMetadataStore());
    }

    private Table toTable(ExternalTable extTable) {
        SqlConnector connector = sqlConnectorCache.forType(extTable.type());
        return connector.createTable(nodeEngine, SCHEMA_NAME_PUBLIC, extTable.name(), extTable.fields(), extTable.options());
    }
}
