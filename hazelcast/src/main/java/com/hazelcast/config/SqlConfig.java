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

package com.hazelcast.config;

/**
 * SQL service configuration.
 */
public class SqlConfig {
    /** Default thread count. */
    public static final int DFLT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    // TODO: Evaluate this default carefully.
    /** Default max memory. */
    public static final long DFLT_MAX_MEMORY = Runtime.getRuntime().maxMemory() / 2;

    /** Number of threads which will execute SQL queries. */
    private int threadCount = DFLT_THREAD_COUNT;

    /** Max memory in bytes for SQL processing. */
    private long maxMemory = DFLT_MAX_MEMORY;

    /**
     * Gets the number of threads responsible for data processing.
     *
     * @return Number of threads responsible for data processing.
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Sets the number of threads responsible for data processing.
     *
     * @param threadCount Number of threads responsible for data processing.
     * @return This instance for chaining.
     */
    public SqlConfig setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    /**
     * Gets the maximum amount of memory in bytes SQL engine is allowed to use for query processing. When the threshold is
     * reached, the engine may attempt to spill intermediate results to disk, or to cancel the query.
     * <p>
     * Defaults to {@link #DFLT_MAX_MEMORY}.
     *
     * @return Maximum amount of memory in bytes. Zero or negative value means no limit.
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Set the maximum amount of memory in bytes SQL engine is allowed to use for query processing. When the threshold is
     * reached, the engine may attempt to spill intermediate results to disk, or to cancel the query.
     * <p>
     * Defaults to {@link #DFLT_MAX_MEMORY}.
     *
     * @param maxMemory Maximum amount of memory in bytes. Zero or negative value means no limit.
     * @return This instance for chaining.
     */
    public SqlConfig setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;

        return this;
    }

    @Override
    public String toString() {
        return "SqlConfig{threadCount=" + threadCount + ", maxMemory=" + maxMemory + '}';
    }
}
