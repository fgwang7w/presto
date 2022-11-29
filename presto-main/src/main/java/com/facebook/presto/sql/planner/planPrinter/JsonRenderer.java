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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonRenderer
        implements Renderer<String>
{
    private static JsonCodec<JsonRenderedNode> codec = JsonCodec.jsonCodec(JsonRenderedNode.class);
    private static JsonCodec<Map<PlanFragmentId, JsonPlanFragment>> planMapCodec = JsonCodec.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

    private FunctionAndTypeManager functionAndTypeManager;

    public JsonRenderer(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;

        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.get().enable(SerializationFeature.INDENT_OUTPUT);
        provider.get().enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT, "@type");
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(functionAndTypeManager)));

        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);
        this.codec = codecFactory.jsonCodec(JsonRenderedNode.class);
        this.planMapCodec = codecFactory.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

        /*
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(() -> new JsonObjectMapperProvider().get().registerModule(
                new SimpleModule()
                        // add serde methods for handling VariableReferenceExpression
                        .addSerializer(VariableReferenceExpression.class, new VariableStatsSerdeAdaptor.VariableStatsSerializer())
                        .addKeyDeserializer(VariableReferenceExpression.class, new VariableStatsSerdeAdaptor.VariableStatsVariableReferenceExpressionDeserializer(functionAndTypeManager))));
        CODEC = jsonCodecFactory.jsonCodec(JsonRenderedNode.class);
        PLAN_MAP_CODEC = jsonCodecFactory.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

         */
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        JsonRenderedNode node = renderJson(plan, plan.getRoot());
        String out = "";
        try {
            out = codec.toJson(node);
        }
        catch (Exception ex) {
            System.out.println(ex.getCause().toString());
        }
        return out;
    }

    public String render(Map<PlanFragmentId, JsonPlanFragment> fragmentJsonMap)
    {
        return planMapCodec.toJson(fragmentJsonMap);
    }

    private JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node.getSourceLocation(),
                node.getId().toString(),
                node.getName(),
                node.getIdentifier(),
                node.getDetails(),
                children,
                node.getRemoteSources().stream()
                        .map(PlanFragmentId::toString)
                        .collect(toImmutableList()),
                node.getEstimatedStats());
    }

    public static class JsonRenderedNode
    {
        private final Optional<SourceLocation> sourceLocation;
        private final String id;
        private final String name;
        private final String identifier;
        private final String details;
        private final List<JsonRenderedNode> children;
        private final List<String> remoteSources;
        private final List<PlanNodeStatsEstimate> estimates;

        @JsonCreator
        public JsonRenderedNode(
                @JsonProperty("source") Optional<SourceLocation> sourceLocation,
                @JsonProperty("id") String id,
                @JsonProperty("name") String name,
                @JsonProperty("identifier") String identifier,
                @JsonProperty("details") String details,
                @JsonProperty("children") List<JsonRenderedNode> children,
                @JsonProperty("remoteSources") List<String> remoteSources,
                @JsonProperty("estimates") List<PlanNodeStatsEstimate> estimates)
        {
            this.sourceLocation = sourceLocation;
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.identifier = requireNonNull(identifier, "identifier is null");
            this.details = requireNonNull(details, "details is null");
            this.children = requireNonNull(children, "children is null");
            this.remoteSources = requireNonNull(remoteSources, "id is null");
            this.estimates = requireNonNull(estimates, "estimate is null");
        }

        @JsonProperty
        public Optional<SourceLocation> getSourceLocation()
        {
            return sourceLocation;
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getIdentifier()
        {
            return identifier;
        }

        @JsonProperty
        public String getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }

        @JsonProperty
        public List<String> getRemoteSources()
        {
            return remoteSources;
        }

        @JsonProperty
        public List<PlanNodeStatsEstimate> getEstimates()
        {
            return estimates;
        }
    }

    public static class JsonPlanFragment
    {
        @JsonRawValue
        private final String plan;

        @JsonCreator
        public JsonPlanFragment(String plan)
        {
            this.plan = plan;
        }

        @JsonProperty
        public String getPlan()
        {
            return this.plan;
        }
    }
}
