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

package com.hazelcast.metadata;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Metadata service, AP or CP.
 */
public interface MetadataStore {
    /**
     * Get the metadata entry.
     *
     * @param key Key.
     * @return Value or {@code null}
     */
    Object get(Object key);

    /**
     * Get objects matching the passed filter.
     *
     * @param filter Filter applied to keys.
     * @return Objects that pass the filter.
     */
    Map<Object, Object> getWithFilter(Predicate<Object> filter);

    /**
     * Create the metadata entry.
     *
     * @param key Key.
     * @param value Value.
     * @param ifNotExists If {@code true} then the command will be ignored if another entry with the given key already exists,
     *                    if {@code false} an exception will be thrown.
     */
    void create(Object key, Object value, boolean ifNotExists);


    /**
     * Drop the metadata entry.
     *
     * @param key Key.
     * @param isExists If {@code true} then the command will be ignored if an entry with the given key doens't exist,
     *                 if {@code false} an exception will be thrown.
     */
    void drop(Object key, boolean isExists);
}
