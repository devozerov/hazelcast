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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ProjectIntoScanLogicalRule extends RelOptRule {
    public static final ProjectIntoScanLogicalRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
            operand(Project.class,
                operandJ(TableScan.class, null, OptUtils::isProjectableFilterable, none())),
            RelFactories.LOGICAL_BUILDER,
            ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        Mappings.TargetMapping mapping = project.getMapping();

        RelNode transformed;

        if (mapping == null) {
            transformed = processComplex(project, scan);
        } else if (Mappings.isIdentity(mapping)) {
            // Project returns all the rows of the scan. Let ProjectRemoveRule do its job.
            return;
        } else {
            transformed = processSimple(mapping, scan);
        }

        call.transformTo(transformed);
    }

    /**
     * Process simple case when all project expressions are direct field access.
     *
     * @param mapping Projects mapping.
     * @param scan Scan.
     * @return Transformed node (new scan).
     */
    private static RelNode processSimple(Mappings.TargetMapping mapping, TableScan scan) {
        List<Integer> projects = getScanProjects(scan);
        RexNode filter = getScanFilter(scan);

        List<Integer> newProjects = Mappings.apply((Mapping) mapping, projects);

        return new MapScanLogicalRel(
            scan.getCluster(),
            OptUtils.toLogicalConvention(scan.getTraitSet()),
            scan.getTable(),
            newProjects,
            filter
        );
    }

    /**
     * Process complex project with expressions. Projection will remain as is, but the number of returned fields might be
     * decreased in scan.
     *
     * @param project Project.
     * @param scan Scan.
     * @return Transformed node (new project with new scan).
     */
    private RelNode processComplex(Project project, TableScan scan) {
        // Map projected field references to real scan fields.
        List<Integer> oldScanFields = getScanProjects(scan);

        ProjectFieldVisitor projectFieldVisitor = new ProjectFieldVisitor(oldScanFields);

        for (RexNode projectExp : project.getProjects()) {
            projectExp.accept(projectFieldVisitor);
        }

        // Get new scan fields. These are the only fields which are accessed by the project operator, so the rest could be
        // removed.
        List<Integer> newScanFields = projectFieldVisitor.createNewScanFields();
        RexNode filter = getScanFilter(scan);

        MapScanLogicalRel newScan = new MapScanLogicalRel(
            scan.getCluster(),
            OptUtils.toLogicalConvention(scan.getTraitSet()),
            scan.getTable(),
            newScanFields,
            filter
        );

        // Create new project nodes with references to scan fields.
        ProjectConverter projectConverter = projectFieldVisitor.createProjectConverter();

        List<RexNode> newProjects = new ArrayList<>();

        for (RexNode projectExp : project.getProjects()) {
            RexNode newProjectExp = projectExp.accept(projectConverter);

            newProjects.add(newProjectExp);
        }

        ProjectLogicalRel newProject = new ProjectLogicalRel(
            project.getCluster(),
            OptUtils.toLogicalConvention(project.getTraitSet()),
            newScan,
            newProjects,
            project.getRowType()
        );

        // If new project is trivial, i.e. it contains only references to scan fields, then it can be eliminated.
        if (ProjectRemoveRule.isTrivial(newProject)) {
            return newScan;
        } else {
            return newProject;
        }
    }

    /**
     * Get index of the fields exposed by the scan.
     *
     * @param scan Scan.
     * @return Field indexes.
     */
    private static List<Integer> getScanProjects(TableScan scan) {
        return scan instanceof MapScanLogicalRel ? ((MapScanLogicalRel) scan).getProjects() : scan.identity();
    }

    /**
     * Get filter associated with the scan, if any.
     *
     * @param scan Scan.
     * @return Filter or null.
     */
    private static RexNode getScanFilter(TableScan scan) {
        return scan instanceof MapScanLogicalRel ? ((MapScanLogicalRel) scan).getFilter() : null;
    }

    /**
     * Visitor which collects fields from project expressions and map them to respective scan fields.
     */
    private static final class ProjectFieldVisitor extends RexVisitorImpl<Void> {
        /** Originally available scan fields. */
        private final List<Integer> originalScanFields;

        /**
         * Mapped project fields.
         */
        private final Map<Integer, List<RexInputRef>> scanFieldIndexToProjectInputs = new LinkedHashMap<>();

        private ProjectFieldVisitor(List<Integer> originalScanFields) {
            super(true);

            this.originalScanFields = originalScanFields;
        }

        @Override
        public Void visitInputRef(RexInputRef projectInput) {
            Integer scanField = originalScanFields.get(projectInput.getIndex());

            assert scanField != null;

            List<RexInputRef> projectInputs = scanFieldIndexToProjectInputs.computeIfAbsent(scanField, (k) -> new ArrayList<>(1));

            projectInputs.add(projectInput);

            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            // TODO: Support "star" and "item".
            for (RexNode operand : call.operands) {
                operand.accept(this);
            }

            return null;
        }

        private List<Integer> createNewScanFields() {
            return new ArrayList<>(scanFieldIndexToProjectInputs.keySet());
        }

        private ProjectConverter createProjectConverter() {
            Map<RexNode, Integer> projectExpToScanFieldMap = new HashMap<>();

            int index = 0;

            for (List<RexInputRef> projectInputs : scanFieldIndexToProjectInputs.values()) {
                for (RexNode projectInput : projectInputs) {
                    projectExpToScanFieldMap.put(projectInput, index);
                }

                index++;
            }

            return new ProjectConverter(projectExpToScanFieldMap);
        }
    }

    /**
     * Visitor which converts old project expressions (before pushdown) to new project expressions (after pushdown).
     */
    public static final class ProjectConverter extends RexShuttle {
        /** Map from old project expression to relevant field in the new scan operator. */
        private final Map<RexNode, Integer> projectExpToScanFieldMap;

        private ProjectConverter(Map<RexNode, Integer> projectExpToScanFieldMap) {
            this.projectExpToScanFieldMap = projectExpToScanFieldMap;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            Integer index = projectExpToScanFieldMap.get(call);

            if (index != null) {
                return new RexInputRef(index, call.getType());
            }

            return super.visitCall(call);
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            Integer index = projectExpToScanFieldMap.get(inputRef);

            if (index != null) {
                return new RexInputRef(index, inputRef.getType());
            }

            return super.visitInputRef(inputRef);
        }
    }
}
