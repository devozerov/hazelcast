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

package com.hazelcast.sql.impl.calcite.expression;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.physical.PhysicalNodeSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Visitor which converts REX node to a Hazelcast expression.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "rawtypes"})
public class ExpressionConverterRexVisitor implements RexVisitor<Expression> {
    /** Schema of the node. */
    private final PhysicalNodeSchema schema;

    /** Parameters. */
    private final int paramsCount;

    public ExpressionConverterRexVisitor(PhysicalNodeSchema schema, int paramsCount) {
        this.schema = schema;
        this.paramsCount = paramsCount;
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();

        return ColumnExpression.create(index, schema.getType(index));
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        return ExpressionConverterUtils.convertLiteral(literal);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength",
        "checkstyle:NPathComplexity", "checkstyle:ReturnCount"})
    @Override
    public Expression visitCall(RexCall call) {
        // Convert operator.
        SqlOperator operator = call.getOperator();

        int hzOperator = ExpressionConverterUtils.convertOperator(operator);

        // Convert operands.
        List<RexNode> operands = call.getOperands();

        List<Expression<?>> hzOperands;

        if (operands == null || operands.isEmpty()) {
            hzOperands = Collections.emptyList();
        } else {
            hzOperands = new ArrayList<>(operands.size());

            for (RexNode operand : operands) {
                Expression<?> convertedOperand = operand.accept(this);

                hzOperands.add(convertedOperand);
            }
        }

        return ExpressionConverterUtils.convertCall(hzOperator, hzOperands);
    }

    @Override
    public Expression visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        int index = dynamicParam.getIndex();

        if (index >= paramsCount) {
            // TODO: Proper exception.
            throw HazelcastSqlException.error("Insufficient parameters: " + paramsCount);
        }

        return new ParameterExpression(index);
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        // TODO: Is this nested field access?
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException();
    }
}
