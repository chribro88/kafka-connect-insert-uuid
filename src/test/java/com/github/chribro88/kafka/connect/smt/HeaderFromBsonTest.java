package com.github.chribro88.kafka.connect.smt;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.github.chribro88.kafka.connect.smt.field.FieldSyntaxVersion;

import org.bson.Document;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HeaderFromBsonTest {

    static class RecordBuilder {
        private final List<String> fields = new ArrayList<>(2);
        private final List<Schema> fieldSchemas = new ArrayList<>(2);
        private final List<Object> fieldValues = new ArrayList<>(2);
        private final ConnectHeaders headers = new ConnectHeaders();

        public RecordBuilder() {
        }

        public RecordBuilder withField(String name, Schema schema, Object value) {
            fields.add(name);
            fieldSchemas.add(schema);
            fieldValues.add(value);
            return this;
        }

        public RecordBuilder addHeader(String name, Schema schema, Object value) {
            headers.add(name, new SchemaAndValue(schema, value));
            return this;
        }

        public SourceRecord schemaless(boolean keyTransform) {
                Document document = new Document();
                for (int i = 0; i < this.fields.size(); i++) {
                    String fieldName = this.fields.get(i);
                    document.append(fieldName, this.fieldValues.get(i));
                }
                return sourceRecord(keyTransform, null, BsonToBinary.toBytes(document));
            }

        private Schema schema() {
            SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
            for (int i = 0; i < this.fields.size(); i++) {
                String fieldName = this.fields.get(i);
                schemaBuilder.field(fieldName, this.fieldSchemas.get(i));

            }
            return schemaBuilder.build();
        }

        private Struct struct(Schema schema) {
            Struct struct = new Struct(schema);
            for (int i = 0; i < this.fields.size(); i++) {
                String fieldName = this.fields.get(i);
                struct.put(fieldName, this.fieldValues.get(i));
            }
            return struct;
        }

        public SourceRecord withSchema(boolean keyTransform) {
            Schema schema = schema();
            Struct struct = struct(schema);
            return sourceRecord(keyTransform, schema, struct);
        }

        private SourceRecord sourceRecord(boolean keyTransform, Schema keyOrValueSchema, Object keyOrValue) {
            Map<String, ?> sourcePartition = singletonMap("foo", "bar");
            Map<String, ?> sourceOffset = singletonMap("baz", "quxx");
            String topic = "topic";
            Integer partition = 0;
            Long timestamp = 0L;

            ConnectHeaders headers = this.headers;
            if (keyOrValueSchema == null) {
                // When doing a schemaless transformation we don't expect the header to have a schema
                headers = new ConnectHeaders();
                for (Header header : this.headers) {
                    headers.add(header.key(), new SchemaAndValue(null, header.value()));
                }
            }
            return new SourceRecord(sourcePartition, sourceOffset, topic, partition,
                    keyTransform ? keyOrValueSchema : null,
                    keyTransform ? keyOrValue : "key",
                    !keyTransform ? keyOrValueSchema : null,
                    !keyTransform ? keyOrValue : "value",
                    timestamp, headers);
        }

        @Override
        public String toString() {
            return "RecordBuilder(" +
                    "fields=" + fields +
                    ", fieldSchemas=" + fieldSchemas +
                    ", fieldValues=" + fieldValues +
                    ", headers=" + headers +
                    ')';
        }
    }

    public static List<Arguments> schemalessData() {

        List<Arguments> result = new ArrayList<>();

        for (Boolean testKeyTransform : asList(true, false)) {
            result.add(
                    Arguments.of(
                            "basic copy",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic move",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic copy and remove",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), singletonList("field1"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    // field1 got removed
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));        
            result.add(
                    Arguments.of(
                            "copy with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                            
                    ));
            result.add(
                    Arguments.of(
                            "move with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            Map<String, Object> struct = Collections.singletonMap("foo", "foo-value");
            result.add(
                    Arguments.of(
                            "copy with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", null, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", null, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", null, struct)
                    ));
            result.add(
                    Arguments.of(
                            "move with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", null, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", null, struct)
                    ));
            result.add(
                    Arguments.of(
                            "copy and remove with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", null, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), singletonList("field1"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    // field1 got removed
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", null, struct)
                    ));
            result.add(
                    Arguments.of(
                            "two headers from same field",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("field1", "field1"), asList("inserted1", "inserted2"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                                    .addHeader("inserted2", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "two fields to same header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("field1", "field2"), asList("inserted1", "inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 and field2 got moved
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field2-value")
                    ));
        }
        return result;
    }

    public static List<Arguments> structData() {

        List<Arguments> result = new ArrayList<>();

        for (Boolean testKeyTransform : asList(true, false)) {
            result.add(
                    Arguments.of(
                            "basic copy",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic move",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic copy and remove",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), singletonList("field1"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    // field1 got removed
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));        
            result.add(
                    Arguments.of(
                            "copy with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "move with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                    ));
            Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("foo", STRING_SCHEMA).build();
            Struct struct = new Struct(schema).put("foo", "foo-value");
            result.add(
                    Arguments.of(
                            "copy with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", schema, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", schema, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", schema, struct)
                    ));
            result.add(
                    Arguments.of(
                            "move with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", schema, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", schema, struct)
                    ));
            result.add(
                    Arguments.of(
                            "copy and remove with struct value",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", schema, struct)
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("field1"), singletonList("inserted1"), singletonList("field1"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    // field1 got removed
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", schema, struct)
                    ));        
            result.add(
                    Arguments.of(
                            "two headers from same field",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("field1", "field1"), asList("inserted1", "inserted2"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 got moved
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                                    .addHeader("inserted2", STRING_SCHEMA, "field1-value")
                    ));
            result.add(
                    Arguments.of(
                            "two fields to same header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("field1", "field2"), asList("inserted1", "inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    // field1 and field2 got moved
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field1-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "field2-value")
                    ));
        }
        return result;
    }

    public static List<Arguments> nestedSchemalessData() {

        List<Arguments> result = new ArrayList<>();

        final Map<String, String> foo = new HashMap<>();
        foo.put("bar", "bar-value");
        foo.put("baz", "baz-value");
        final Map<String, String> onlyBaz = singletonMap("baz", "baz-value");

        for (Boolean testKeyTransform : asList(true, false)) {

            result.add(
                    Arguments.of(
                            "basic copy",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic move",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", null, onlyBaz)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic copy and remove with nested",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), singletonList("foo.bar"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got removed
                                    .withField("foo", null, onlyBaz)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
             result.add(
                    Arguments.of(
                            "basic copy and remove",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), singletonList("foo"),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo got moved
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "copy with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "move with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", null, onlyBaz)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "two headers from same field",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("foo.bar", "foo.bar"), asList("inserted1", "inserted2"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", null, onlyBaz)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                                    .addHeader("inserted2", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "two fields to same header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", null, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("foo.bar", "foo.baz"), asList("inserted1", "inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar and foo.baz got moved
                                    .withField("foo", null, Collections.emptyMap())
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "baz-value")
                    ));

        }
        return result;
    }

    public static List<Arguments> nestedStructData() {

        List<Arguments> result = new ArrayList<>();

        final Schema fooSchema = new SchemaBuilder(Type.STRUCT)
                .field("bar", STRING_SCHEMA)
                .field("baz", STRING_SCHEMA)
                .build();
        final Struct foo = new Struct(fooSchema)
                .put("bar", "bar-value")
                .put("baz", "baz-value");
        final Schema onlyBazSchema = new SchemaBuilder(Type.STRUCT)
                .field("baz", STRING_SCHEMA)
                .build();
        final Struct onlyBaz = new Struct(onlyBazSchema).put("baz", "baz-value");
        final Schema emptySchema = new SchemaBuilder(Type.STRUCT).build();
        final Struct empty = new Struct(emptySchema);

        for (Boolean testKeyTransform : asList(true, false)) {
            result.add(
                    Arguments.of(
                            "basic copy",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "basic move",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", onlyBazSchema, onlyBaz)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "copy with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.COPY,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "move with preexisting header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value"),
                            singletonList("foo.bar"), singletonList("inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", onlyBazSchema, onlyBaz)
                                    .addHeader("inserted1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "two headers from same field",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("foo.bar", "foo.bar"), asList("inserted1", "inserted2"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar got moved
                                    .withField("foo", onlyBazSchema, onlyBaz)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                                    .addHeader("inserted2", STRING_SCHEMA, "bar-value")
                    ));
            result.add(
                    Arguments.of(
                            "two fields to same header",
                            testKeyTransform,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    .withField("foo", fooSchema, foo)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value"),
                            // two headers from the same field
                            asList("foo.bar", "foo.baz"), asList("inserted1", "inserted1"), emptyList(),
                            HeaderFromBson.Operation.MOVE,
                            new RecordBuilder()
                                    .withField("field1", STRING_SCHEMA, "field1-value")
                                    .withField("field2", STRING_SCHEMA, "field2-value")
                                    // foo.bar and foo.baz got moved
                                    .withField("foo", emptySchema, empty)
                                    .addHeader("header1", STRING_SCHEMA, "existing-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "bar-value")
                                    .addHeader("inserted1", STRING_SCHEMA, "baz-value")
                    ));

        }
        return result;
    }

    private Map<String, Object> config(List<String> headers, List<String> transformFields,
            HeaderFromBson.Operation operation) {
        return config(headers, transformFields, operation, FieldSyntaxVersion.V1);
    }

    private Map<String, Object> config(List<String> headers, List<String> transformFields,
            List<String> removeFields, 
            HeaderFromBson.Operation operation) {
        return config(headers, transformFields, removeFields, operation, FieldSyntaxVersion.V1);
    }

    private Map<String, Object> config(List<String> headers, List<String> transformFields, 
            List<String> removeFields,
            HeaderFromBson.Operation operation, FieldSyntaxVersion version) {
        Map<String, Object> result = new HashMap<>();
        result.put(HeaderFromBson.HEADERS_FIELD, headers);
        result.put(HeaderFromBson.FIELDS_FIELD, transformFields);
        result.put(HeaderFromBson.OPERATION_FIELD, operation.toString());
        result.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, version.name());
        result.put(HeaderFromBson.REMOVES_FIELD, removeFields);
        return result;
    }

     private Map<String, Object> config(List<String> headers, List<String> transformFields, 
            HeaderFromBson.Operation operation, FieldSyntaxVersion version) {
        Map<String, Object> result = new HashMap<>();
        result.put(HeaderFromBson.HEADERS_FIELD, headers);
        result.put(HeaderFromBson.FIELDS_FIELD, transformFields);
        result.put(HeaderFromBson.OPERATION_FIELD, operation.toString());
        result.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, version.name());
        result.put(HeaderFromBson.REMOVES_FIELD, emptyList());
        return result;
    }

    @ParameterizedTest
    @MethodSource("schemalessData")
    public void schemaless(
            String description,
            boolean keyTransform,
            RecordBuilder originalBuilder,
            List<String> transformFields, List<String> headers1, List<String> removeFields, HeaderFromBson.Operation operation,
            RecordBuilder expectedBuilder
    ) {
        HeaderFromBson<SourceRecord> xform =
                keyTransform ? new HeaderFromBson.Key<>() : new HeaderFromBson.Value<>();

        xform.configure(config(headers1, transformFields, removeFields, operation));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");

        SourceRecord originalRecord = originalBuilder.schemaless(keyTransform);
        SourceRecord expectedRecord = expectedBuilder.schemaless(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }


    @ParameterizedTest
    @MethodSource("nestedSchemalessData")
    public void schemalessWithNestedData(
            String description,
            boolean keyTransform,
            RecordBuilder originalBuilder,
            List<String> transformFields, List<String> headers1, List<String> removeFields, HeaderFromBson.Operation operation,
            RecordBuilder expectedBuilder
    ) {
        HeaderFromBson<SourceRecord> xform =
                keyTransform ? new HeaderFromBson.Key<>() : new HeaderFromBson.Value<>();

        xform.configure(config(headers1, transformFields, removeFields, operation, FieldSyntaxVersion.V2));
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("existing", "existing-value");

        SourceRecord originalRecord = originalBuilder.schemaless(keyTransform);
        SourceRecord expectedRecord = expectedBuilder.schemaless(keyTransform);
        SourceRecord xformed = xform.apply(originalRecord);
        assertSameRecord(expectedRecord, xformed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigExtraHeaderConfig(boolean keyTransform) {
        Map<String, Object> config = config(singletonList("foo"), asList("foo", "bar"), HeaderFromBson.Operation.COPY);
        HeaderFromBson<?> xform = keyTransform ? new HeaderFromBson.Key<>() : new HeaderFromBson.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigExtraFieldConfig(boolean keyTransform) {
        Map<String, Object> config = config(asList("foo", "bar"), singletonList("foo"), HeaderFromBson.Operation.COPY);
        HeaderFromBson<?> xform = keyTransform ? new HeaderFromBson.Key<>() : new HeaderFromBson.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void invalidConfigEmptyHeadersAndFieldsConfig(boolean keyTransform) {
        Map<String, Object> config = config(emptyList(), emptyList(), HeaderFromBson.Operation.COPY);
        HeaderFromBson<?> xform = keyTransform ? new HeaderFromBson.Key<>() : new HeaderFromBson.Value<>();
        assertThrows(ConfigException.class, () -> xform.configure(config));
    }

    private static void assertSameRecord(SourceRecord expected, SourceRecord xformed) {
        assertEquals(expected.sourcePartition(), xformed.sourcePartition());
        assertEquals(expected.sourceOffset(), xformed.sourceOffset());
        assertEquals(expected.topic(), xformed.topic());
        assertEquals(expected.kafkaPartition(), xformed.kafkaPartition());
        assertEquals(expected.keySchema(), xformed.keySchema());
        if ((expected.key() instanceof byte[])) {
            assertEquals(new String(Base64.getEncoder().encode((byte[]) expected.key())), new String(Base64.getEncoder().encode((byte[]) xformed.key())));
        } else {
            assertEquals(expected.key(), xformed.key());
        }    
        assertEquals(expected.valueSchema(), xformed.valueSchema());
        if ((expected.value() instanceof byte[])) {
            assertEquals(new String(Base64.getEncoder().encode((byte[]) expected.value())), new String(Base64.getEncoder().encode((byte[]) xformed.value())));
        } else {
            assertEquals(expected.value(), xformed.value());
        } 
        assertEquals(expected.timestamp(), xformed.timestamp());
        assertEquals(expected.headers(), xformed.headers());
    }

}