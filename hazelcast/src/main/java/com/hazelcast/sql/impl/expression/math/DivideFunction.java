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
import com.hazelcast.sql.SqlDaySecondInterval;
import com.hazelcast.sql.SqlYearMonthInterval;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.hazelcast.sql.impl.expression.datetime.DateTimeExpressionUtils.NANO_IN_SECONDS;

/**
 * Division.
 */
public class DivideFunction<T> extends BiCallExpressionWithType<T> {
    public DivideFunction() {
        // No-op.
    }

    private DivideFunction(Expression<?> operand1, Expression<?> operand2, DataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static DivideFunction<?> create(Expression<?> operand1, Expression<?> operand2) {
        DataType resultType = MathFunctionUtils.inferDivideResultType(operand1.getType(), operand2.getType());

        return new DivideFunction<>(operand1, operand2, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object operand1Value = operand1.eval(row);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row);

        if (operand2Value == null) {
            return null;
        }

        return (T) doDivide(operand1Value, operand1.getType(), operand2Value, operand2.getType(), resultType);
    }

    private static Object doDivide(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resultType
    ) {
        // Handle lat binding.
        if (resultType.getType() == GenericType.LATE) {
            operand1Type = DataTypeUtils.resolveType(operand1);
            operand2Type = DataTypeUtils.resolveType(operand2);

            resultType = MathFunctionUtils.inferDivideResultType(operand1Type, operand2Type);
        }

        // Handle intervals.
        if (resultType.getType() == GenericType.INTERVAL_YEAR_MONTH || resultType.getType() == GenericType.INTERVAL_DAY_SECOND) {
            return doDivideInterval(operand1, operand2, operand2Type, resultType);
        }

        // Handle numeric.
        return doDivideNumeric(operand1, operand1Type, operand2, operand2Type, resultType);
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doDivideInterval(Object operand1, Object operand2, DataType operand2Type, DataType resultType) {
        switch (resultType.getType()) {
            case INTERVAL_YEAR_MONTH: {
                SqlYearMonthInterval interval = (SqlYearMonthInterval) operand1;
                int divisor = operand2Type.getConverter().asInt(operand2);

                return new SqlYearMonthInterval(interval.getMonths() / divisor);
            }

            case INTERVAL_DAY_SECOND: {
                SqlDaySecondInterval interval = (SqlDaySecondInterval) operand1;
                long divisor = operand2Type.getConverter().asBigint(operand2);

                long totalNanos = (interval.getSeconds() * NANO_IN_SECONDS + interval.getNanos()) / divisor;

                long newValue = totalNanos / NANO_IN_SECONDS;
                int newNanos = (int) (totalNanos % NANO_IN_SECONDS);

                return new SqlDaySecondInterval(newValue, newNanos);
            }

            default:
                throw HazelcastSqlException.error("Invalid type: " + resultType);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doDivideNumeric(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resultType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        try {
            switch (resultType.getType()) {
                case TINYINT:
                    return (byte) (operand1Converter.asTinyint(operand1) / operand2Converter.asTinyint(operand2));

                case SMALLINT:
                    return (short) (operand1Converter.asSmallint(operand1) / operand2Converter.asSmallint(operand2));

                case INT:
                    return operand1Converter.asInt(operand1) / operand2Converter.asInt(operand2);

                case BIGINT:
                    return operand1Converter.asBigint(operand1) / operand2Converter.asBigint(operand2);

                case DECIMAL:
                    BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                    BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                    return op1Decimal.divide(op2Decimal, DataType.SCALE_DIVIDE, RoundingMode.HALF_DOWN);

                case REAL: {
                    float res = operand1Converter.asReal(operand1) / operand2Converter.asReal(operand2);

                    if (Float.isInfinite(res)) {
                        throw HazelcastSqlException.error("Division by zero.");
                    }

                    return res;
                }

                case DOUBLE: {
                    double res = operand1Converter.asDouble(operand1) / operand2Converter.asDouble(operand2);

                    if (Double.isInfinite(res)) {
                        throw HazelcastSqlException.error("Division by zero.");
                    }

                    return res;
                }

                default:
                    throw HazelcastSqlException.error("Invalid type: " + resultType);
            }
        } catch (ArithmeticException e) {
            throw HazelcastSqlException.error("Division by zero.");
        }
    }
}
