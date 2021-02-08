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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.allowIntersectToJoinTransformation;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.Patterns.intersect;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class IntersectToJoinTransformation
        implements Rule<IntersectNode>
{
    private static final Pattern<IntersectNode> PATTERN = intersect();

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return allowIntersectToJoinTransformation(session);
    }

    /**
     * This method basically traverse the subplan and transform INTERSECT into JOIN as following:
     *
     *                  INTERSECT
     *                 /         \
     *          Project         Project
     *      [t1.c1, t1.c2]     [t2.c1, t2.c2]
     *            |                 \
     *     Tbs:{t1: c1, c2}     Tbs:{t2: c1, c2}
     *
     * <==== Transformation ====>
     *
     *                Aggregation
     *                    |
     *                  Project
     *                    |
     *                   JOIN
     *               [t1.c1=t2.c1, t1.c2=t2.c2]
     *                  /       \
     *          Project          Project
     *      [t1.c1, t1.c2]     [t2.c1, t2.c2]
     *          |                   \
     *     Aggregation          Aggregation
     *    [t1.c1, t1.c2]        [t2.c1, t2.c2]
     *          |                   \
     *      Tbs:{t1: c1, c2}        Tbs:{t2: c1, c2}
     *
     * @param node
     * @param captures
     * @param context
     * @return
     */
    @Override
    public Result apply(IntersectNode node, Captures captures, Context context)
    {
        ImmutableList.Builder<ProjectNode> projectNodes = ImmutableList.builder();
        List<PlanNode> sources = node.getSources();
        List<VariableReferenceExpression> outputs = node.getOutputVariables();

        for (int i = 0; i < sources.size(); i++) {
            projectNodes.add(identifyProjectionFromSources(
                    sources.get(i),
                    Maps.transformValues(node.sourceVariableMap(i), variable -> new SymbolReference(variable.getName())),
                    context));
        }

        PlanNode rewritten = transformIntoJoin(projectNodes.build(), outputs, context);
        return Result.ofPlanNode(AggregationOverJoin(rewritten, context.getIdAllocator()));
    }

    private ProjectNode identifyProjectionFromSources(PlanNode source, Map<VariableReferenceExpression, SymbolReference> projection, Context context)
    {
        Assignments.Builder assignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, SymbolReference> entry : projection.entrySet()) {
            VariableReferenceExpression variableReferenceExpression = context.getVariableAllocator().newVariable(entry.getKey().getName(), entry.getKey().getType());
            assignments.put(variableReferenceExpression, castToRowExpression(entry.getValue()));
        }
        return new ProjectNode(context.getIdAllocator().getNextId(), source, assignments.build());
    }

    private PlanNode transformIntoJoin(List<ProjectNode> sources, List<VariableReferenceExpression> outputs, Context context)
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
                    context.getIdAllocator().getNextId(),
                    source,
                    context.getVariableAllocator().newVariable("unique", BIGINT));

            result = new JoinNode(
                    context.getIdAllocator().getNextId(),
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
                context.getIdAllocator().getNextId(),
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

    private PlanNode AggregationOverJoin(PlanNode rewritten, PlanNodeIdAllocator idAllocator)
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
}
