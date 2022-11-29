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
package com.facebook.presto.sql;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static java.lang.String.format;

public final class Serialization
{
    // for variable SerDe; variable names might contain "()"; use angle brackets to avoid conflict
    private static final char VARIABLE_TYPE_OPEN_BRACKET = '<';
    private static final char VARIABLE_TYPE_CLOSE_BRACKET = '>';

    private Serialization() {}

    public static class ExpressionSerializer
            extends JsonSerializer<Expression>
    {
        @Override
        public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeString(ExpressionFormatter.formatExpression(expression, Optional.empty()));
        }
    }

    public static class ExpressionDeserializer
            extends JsonDeserializer<Expression>
    {
        private final SqlParser sqlParser;

        @Inject
        public ExpressionDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.getText()));
        }
    }

    public static class FunctionCallDeserializer
            extends JsonDeserializer<FunctionCall>
    {
        private final SqlParser sqlParser;

        @Inject
        public FunctionCallDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (FunctionCall) rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.getText()));
        }
    }

    public static class VariableReferenceExpressionSerializer
            extends JsonSerializer<VariableReferenceExpression>
    {
        @Override
        public void serialize(VariableReferenceExpression value, JsonGenerator jsonGenerator, SerializerProvider serializers)
                throws IOException
        {
            // serialize variable as "name<type>"
            jsonGenerator.writeFieldName(format("%s%s%s%s", value.getName(), VARIABLE_TYPE_OPEN_BRACKET, value.getType().getTypeSignature(), VARIABLE_TYPE_CLOSE_BRACKET));
        }
    }

    public static class VariableReferenceExpressionDeserializer
            extends KeyDeserializer
    {
        private final TypeManager typeManager;

        @Inject
        public VariableReferenceExpressionDeserializer(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctx)
        {
            int p = key.indexOf(VARIABLE_TYPE_OPEN_BRACKET);
            if (p <= 0 || key.charAt(key.length() - 1) != VARIABLE_TYPE_CLOSE_BRACKET) {
                throw new IllegalArgumentException(format("Expect key to be of format 'name<type>', found %s", key));
            }
            return new VariableReferenceExpression(Optional.empty(), key.substring(0, p), typeManager.getType(parseTypeSignature(key.substring(p + 1, key.length() - 1))));
        }
    }

    public static class HashTreePMapSerializer
            extends JsonSerializer<PMap<Object, ?>>
    {
        /**
         * @param value Value to serialize; can <b>not</b> be null.
         * @param gen Generator used to output resulting Json content
         * @param serializers Provider that can be used to get serializers for
         * serializing Objects value contains, if any.
         * @throws IOException
         */
        @Override
        public void serialize(PMap<Object, ?> value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException
        {
            gen.writeStartObject();
            for (Map.Entry<Object, ?> entry : value.entrySet()) {
                gen.writeObjectField(entry.getKey().getClass().getTypeName(), entry.getKey());
                gen.writeObjectField(entry.getValue().getClass().getTypeName(), entry.getValue());
            }
            gen.writeEndObject();
        }
    }
    public static class HashTreePMapDeserializer
            extends JsonDeserializer<PMap<Object, ?>>
    {
        /**
         * @param p Parsed used for reading JSON content
         * @param ctxt Context that can be used to access information about
         * this deserialization activity.
         * @return
         * @throws IOException
         * @throws JsonProcessingException
         */
        @Override
        public PMap<Object, Object> deserialize(JsonParser parser, DeserializationContext ctxt)
                throws IOException, JsonProcessingException
        {
            JsonNode node = parser.readValueAsTree();
            ObjectMapper mapper = createObjectMapper();
            if (node.fields().hasNext()) {
                Map<String, Object> jsonMap = mapper.readValue(node.toString(), new TypeReference<Map<String, Object>>(){});

                List<Object> mapResult = new ArrayList<>();
                for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                    try {
                        Class<?> clazz = Class.forName(entry.getKey());
                        // TODO: currently this will through an error due to '@' annotation in RowExpression
                        mapResult.add(mapper.readValue(entry.getValue().toString(), clazz));
                    }
                    catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }

                Map<Object, Object> result = new HashMap<>();
                result.put(mapResult.get(0), mapResult.get(1));
                return HashTreePMap.from(result);
            }
            return null;
        }
        private static ObjectMapper createObjectMapper()
        {
            return new ObjectMapper()
                    .registerModule(new Jdk8Module());
        }
    }
}
