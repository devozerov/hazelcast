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

package com.hazelcast.sql.impl.calcite.operators;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Custom functions.
 */
public final class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {
    public static final SqlFunction LENGTH =
        new SqlFunction(
            "LENGTH",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.CHARACTER,
            SqlFunctionCategory.NUMERIC
        );

    /** Function to calculate distributed average. */
    public static final SqlAggFunction DISTRIBUTED_AVG = new DistributedAvgAggFunction();

    private static final HazelcastSqlOperatorTable INSTANCE = new HazelcastSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private HazelcastSqlOperatorTable() {
        // No-op.
    }

    public static HazelcastSqlOperatorTable instance() {
        return INSTANCE;
    }
}
