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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of ROUND/TRUNCATE functions.
 */
public abstract class RoundTruncateFunction<T> extends BiCallExpressionWithType<T> {
    public RoundTruncateFunction() {
        // No-op.
    }

    protected RoundTruncateFunction(Expression<?> operand1, Expression<?> operand2, DataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static RoundTruncateFunction<?> create(Expression<?> operand1, Expression<?> operand2, boolean truncate) {
        DataType resultType = inferReturnType(operand1.getType());

        if (operand2 != null && !operand2.getType().isNumeric()) {
            throw HazelcastSqlException.error("Operand 2 is not numeric: " + operand2.getType());
        }

        if (truncate) {
            return new TruncateFunction<>(operand1, operand2, resultType);
        } else {
            return new RoundFunction<>(operand1, operand2, resultType);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        // Get base operand.
        BigDecimal value = operand1.evalAsDecimal(row);

        if (value == null) {
            return null;
        }

        // Get length.
        Integer len = operand2 != null ? operand2.evalAsInt(row) : null;

        int len0 = len != null ? len : 0;

        // Execute.
        RoundingMode roundingMode = getRoundingMode();

        if (len0 == 0) {
            value = value.setScale(0, roundingMode);
        } else {
            value = value.movePointRight(len).setScale(0, roundingMode).movePointLeft(len);
        }

        // Cast to expected type.
        switch (resultType.getType()) {
            case INT:
                return (T) (Integer) value.intValueExact();

            case BIGINT:
                return (T) (Long) value.longValueExact();

            case DECIMAL:
                return (T) value;

            case DOUBLE:
                return (T) (Double) value.doubleValue();

            default:
                throw HazelcastSqlException.error("Unexpected result type: " + resultType);
        }
    }

    protected abstract RoundingMode getRoundingMode();

    private static DataType inferReturnType(DataType operand1Type) {
        if (!operand1Type.isNumeric()) {
            throw HazelcastSqlException.error("Operand 1 is not numeric: " + operand1Type);
        }

        switch (operand1Type.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
                return DataType.INT;

            case VARCHAR:
            case LATE:
                return DataType.DECIMAL;

            case REAL:
                return DataType.DOUBLE;

            default:
                break;
        }

        return operand1Type;
    }
}
