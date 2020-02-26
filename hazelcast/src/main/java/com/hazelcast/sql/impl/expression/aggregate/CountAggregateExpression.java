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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.GenericType;

/**
 * Counting accumulator.
 */
public class CountAggregateExpression extends AbstractSingleOperandAggregateExpression<Long> {
    public CountAggregateExpression() {
        // No-op.
    }

    private CountAggregateExpression(Expression<?> operand, DataType resType, boolean distinct) {
        super(operand, resType, distinct);
    }

    public static CountAggregateExpression create(Expression<?> operand, boolean distinct) {
        DataType operandType = operand.getType();

        if (operandType.getType() == GenericType.LATE) {
            throw HazelcastSqlException.error("Operand type cannot be resolved: " + operandType);
        }

        return new CountAggregateExpression(operand, DataType.BIGINT, distinct);
    }

    @Override
    public AggregateCollector newCollector(QueryFragmentContext ctx) {
        return new CountAggregateCollector(distinct);
    }

    @Override
    protected boolean isIgnoreNull() {
        return true;
    }
}
