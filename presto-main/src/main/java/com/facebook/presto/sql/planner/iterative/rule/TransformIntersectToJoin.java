/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

public class TransformIntersectToJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        // return SimplePlanRewriter.rewriteWith(new Rewriter(session, idAllocator, variableAllocator), plan);
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;

        private Rewriter(Session session, PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = node.getSources().stream().collect(Collectors.toList());
            // identify projection for all fields in each of the sources
            List<ProjectNode> projectNodes = identifyProjectionFromSources(sources, node);
            List<VariableReferenceExpression> outputs = node.getOutputVariables();

            PlanNode rewritten = transformIntoJoin(projectNodes, outputs);
            return AggregationOverJoin(rewritten);
        }

        private PlanNode AggregationOverJoin(PlanNode rewritten)
        {
            return new AggregationNode(
                    idAllocator.getNextId(),
                    rewritten,
                    ImmutableMap.of(),
                    AggregationNode.singleGroupingSet(rewritten.getOutputVariables()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());
        }

        private PlanNode transformIntoJoin(List<ProjectNode> sources, List<VariableReferenceExpression> outputs)
        {
            PlanNode source = sources.get(0);
            JoinNode result = null;
            for (int i = 1; i < sources.size(); i++) {
                ProjectNode filteringSource = sources.get(i);

                if (i > 1) {
                    source = result;
                }
                List<JoinNode.EquiJoinClause> joinCondition = createJoinClause(source, filteringSource);

                AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                        idAllocator.getNextId(),
                        source,
                        variableAllocator.newVariable("unique", BIGINT));

                result = new JoinNode(
                        idAllocator.getNextId(),
                        JoinNode.Type.INNER,
                        inputWithUniqueColumns,
                        filteringSource,
                        joinCondition,
                        ImmutableList.<VariableReferenceExpression>builder().addAll(source.getOutputVariables()).build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of());
            }
            Assignments.Builder assignments = Assignments.builder();
            for (int field = 0; field < outputs.size(); field++) {
                VariableReferenceExpression variable = result.getOutputVariables().get(field);
                assignments.put(outputs.get(field), castToRowExpression(new SymbolReference(variable.getName())));
            }
            return new ProjectNode(
                    idAllocator.getNextId(),
                    result,
                    assignments.build());
        }

        private List<JoinNode.EquiJoinClause> createJoinClause(PlanNode source, PlanNode filteringSource)
        {
            List<JoinNode.EquiJoinClause> equiJoinClauseList = new ArrayList<>();
            for (int j = 0; j < source.getOutputVariables().size(); j++) {
                VariableReferenceExpression leftVariable = source.getOutputVariables().get(j);
                VariableReferenceExpression rightVariable = filteringSource.getOutputVariables().get(j);
                equiJoinClauseList.add(new JoinNode.EquiJoinClause(leftVariable, rightVariable));
            }
            return equiJoinClauseList;
        }

        private List<ProjectNode> identifyProjectionFromSources(List<PlanNode> sources, SetOperationNode node)
        {
            ImmutableList.Builder<ProjectNode> result = ImmutableList.builder();
            for (int i = 0; i < sources.size(); i++) {
                result.add(identifyProjectionFromSources(sources.get(i), Maps.transformValues(node.sourceVariableMap(i), variable -> new SymbolReference(variable.getName()))));
            }
            return result.build();
        }
        private ProjectNode identifyProjectionFromSources(PlanNode source, Map<VariableReferenceExpression, SymbolReference> projection)
        {
            Assignments.Builder assignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, SymbolReference> entry : projection.entrySet()) {
                VariableReferenceExpression variableReferenceExpression = variableAllocator.newVariable(entry.getKey().getName(), entry.getKey().getType());
                assignments.put(variableReferenceExpression, castToRowExpression(entry.getValue()));
            }
            return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
        }
    }
}
