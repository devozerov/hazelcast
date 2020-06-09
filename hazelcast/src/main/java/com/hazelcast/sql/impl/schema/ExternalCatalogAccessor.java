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

import com.hazelcast.function.PredicateEx;
import com.hazelcast.metadata.MetadataStorage;

import java.util.HashSet;
import java.util.Set;

public class ExternalCatalogAccessor {

    private final MetadataStorage storage;

    public ExternalCatalogAccessor(MetadataStorage storage) {
        this.storage = storage;
    }

    public void create(ExternalTable table, boolean ifNotExists) {
        storage.create(new ExternalTableKey(table.name()), table, ifNotExists);
    }

    public void drop(String name, boolean ifExists) {
        storage.drop(new ExternalTableKey(name), ifExists);
    }

    public Set<ExternalTable> values() {
        HashSet<ExternalTable> res = new HashSet<>();

        for (Object value : storage.getWithFilter(Filter.INSTANCE).values()) {
            res.add((ExternalTable) value);
        }

        return res;
    }

    private static class Filter implements PredicateEx<Object> {

        private static final Filter INSTANCE = new Filter();

        @Override
        public boolean testEx(Object o) {
            return o instanceof ExternalTableKey;
        }
    }
}
