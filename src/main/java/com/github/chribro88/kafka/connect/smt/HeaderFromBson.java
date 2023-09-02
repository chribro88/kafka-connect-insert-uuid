package com.github.chribro88.kafka.connect.smt;

import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
// import org.apache.kafka.connect.transforms.field.SingleFieldPath;
// import org.apache.kafka.connect.transforms.field.MultiFieldPaths;
// import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

import com.github.chribro88.kafka.connect.smt.field.FieldSyntaxVersion;
import com.github.chribro88.kafka.connect.smt.field.MultiFieldPaths;
import com.github.chribro88.kafka.connect.smt.field.SingleFieldPath;
import com.github.chribro88.kafka.connect.smt.validators.ListValidator;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public abstract class HeaderFromBson<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_FIELD = "fields";
    public static final String HEADERS_FIELD = "headers";
    public static final String OPERATION_FIELD = "operation";
    private static final String MOVE_OPERATION = "move";
    private static final String COPY_OPERATION = "copy";
    public static final String REMOVES_FIELD = "remove";

    public static final String OVERVIEW_DOC =
            "Moves or copies fields in the key/value of a record into that record's headers. " +
                    "Corresponding elements of <code>" + FIELDS_FIELD + "</code> and " +
                    "<code>" + HEADERS_FIELD + "</code> together identify a field and the header it should be " +
                    "moved or copied to. " +
                    "Use the concrete transformation type designed for the record " +
                    "key (<code>" + Key.class.getName() + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    public static final ConfigDef CONFIG_DEF = new ExtendedConfigDef()
        .addDefinition(FieldSyntaxVersion.configDef())
            .define(FIELDS_FIELD, ConfigDef.Type.LIST,
                    NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "Field names in the record whose values are to be copied or moved to headers.")
            .define(HEADERS_FIELD, ConfigDef.Type.LIST,
                    NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "Header names, in the same order as the field names listed in the fields configuration property.")
            .define(OPERATION_FIELD, ConfigDef.Type.STRING, NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(MOVE_OPERATION, COPY_OPERATION), ConfigDef.Importance.HIGH,
                    "Either <code>move</code> if the fields are to be moved to the headers (removed from the key/value), " +
                            "or <code>copy</code> if the fields are to be copied to the headers (retained in the key/value).")
            .define(REMOVES_FIELD, ConfigDef.Type.LIST,
                    emptyList(), new ListValidator(),
                    ConfigDef.Importance.LOW,
                    "Field names in the record whose values are to be removed. Used to tidy up temporary objects holding header fields.");

    enum Operation {
        MOVE(MOVE_OPERATION),
        COPY(COPY_OPERATION);

        private final String name;

        Operation(String name) {
            this.name = name;
        }

        static Operation fromName(String name) {
            switch (name) {
                case MOVE_OPERATION:
                    return MOVE;
                case COPY_OPERATION:
                    return COPY;
                default:
                    throw new IllegalArgumentException();
            }
        }

        public String toString() {
            return name;
        }
    }

    private MultiFieldPaths fieldPaths;

    private Map<String, List<SingleFieldPath>> headersMap;

    private MultiFieldPaths removePaths;

    private boolean hasRemove;

    private Operation operation;

    private final Cache<Schema, Schema> moveSchemaCache = new SynchronizedCache<>(new LRUCache<>(16));

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        FieldSyntaxVersion syntaxVersion = FieldSyntaxVersion.fromConfig(config);
        List<String> fields = config.getList(FIELDS_FIELD);
        fieldPaths = MultiFieldPaths.of(fields, syntaxVersion);
        List<String> headers = config.getList(HEADERS_FIELD);
        if (headers.size() != fields.size()) {
            throw new ConfigException(format("'%s' config must have the same number of elements as '%s' config.",
                FIELDS_FIELD, HEADERS_FIELD));
        }
        headersMap = new HashMap<>(headers.size());
        for (int i = 0; i < headers.size(); i++) {
            final String headerName = headers.get(i);
            final SingleFieldPath field = new SingleFieldPath(fields.get(i), syntaxVersion);
            headersMap.computeIfPresent(headerName, (s, p) -> {
                p.add(field);
                return p;
            });
            headersMap.computeIfAbsent(headerName, s -> {
                List<SingleFieldPath> paths = new ArrayList<>();
                paths.add(field);
                return paths;
            });
        }
        operation = Operation.fromName(config.getString(OPERATION_FIELD));

        List<String> remove = config.getList(REMOVES_FIELD);
        hasRemove = !remove.isEmpty();
        removePaths = MultiFieldPaths.of(remove, syntaxVersion);
        
    }

    @Override
    public R apply(R record) {
        Object operatingValue = operatingValue(record);
        Schema operatingSchema = operatingSchema(record);

        if (operatingSchema == null) {
            return applySchemaless(record, operatingValue);
        } else {
            return applyWithSchema(record, operatingValue, operatingSchema);
        }
    }

    private R applyWithSchema(R record, Object operatingValue, Schema operatingSchema) {
        if (!(operatingValue instanceof byte[])) {
            throw new UnsupportedOperationException(String.format("Output format is type %s. Please set \"output.format.key/value\": \"bson\"", operatingValue.getClass().getName()));
        }
        // System.out.println("PRINT " + new String((byte[]) operatingValue));
        // System.out.println("PRINT " + new String(Base64.getEncoder().encode((byte[]) operatingValue)));
        Document operatingDocument = BsonToBinary.toDocument((byte[]) operatingValue);
        Headers updatedHeaders = record.headers().duplicate();
        Map<String, Object> value = Requirements.requireMap(operatingDocument, "header " + operation);
        Map<String, Object> updatedValue = new LinkedHashMap<>(value);
        Map<SingleFieldPath, Map.Entry<String, Object>> values = fieldPaths.fieldAndValuesFrom(value);
        if (operation == Operation.MOVE) {
            updatedValue = fieldPaths.updateValuesFrom(
                    updatedValue,
                    (original, map, fieldPath, fieldName) -> map.remove(fieldName)
            );
        }
        if (hasRemove) {
            updatedValue = removePaths.updateValuesFrom(
                    updatedValue,
                    (original, map, fieldPath, fieldName) -> map.remove(fieldName)
            );
        }
        for (Map.Entry<String, List<SingleFieldPath>> entry : headersMap.entrySet()) {
            // headers may point to many values, though it's usually close to 1
            for (SingleFieldPath fieldPath : entry.getValue()) {
                final Map.Entry<String, Object> fieldAndValue = values.get(fieldPath);
                updatedHeaders.add(entry.getKey(), fieldAndValue != null ? fieldAndValue.getValue() : null, null);
            }
        }

        return newRecord(record, operatingSchema, BsonToBinary.toBytes(new Document(updatedValue)), updatedHeaders);
    }

    private R applySchemaless(R record, Object operatingValue) {
        if (!(operatingValue instanceof byte[])) {
            throw new UnsupportedOperationException(String.format("Output format is type %s. Please set \"output.format.key/value\": \"bson\"", operatingValue.getClass().getName()));
        }
        Document operatingDocument = BsonToBinary.toDocument((byte[]) operatingValue);
        Headers updatedHeaders = record.headers().duplicate();
        Map<String, Object> value = Requirements.requireMap(operatingDocument, "header " + operation);
        Map<String, Object> updatedValue = new LinkedHashMap<>(value);
        Map<SingleFieldPath, Map.Entry<String, Object>> values = fieldPaths.fieldAndValuesFrom(value);
        if (operation == Operation.MOVE) {
            updatedValue = fieldPaths.updateValuesFrom(
                    updatedValue,
                    (original, map, fieldPath, fieldName) -> map.remove(fieldName)
            );
        }
        if (hasRemove) {
            updatedValue = removePaths.updateValuesFrom(
                    updatedValue,
                    (original, map, fieldPath, fieldName) -> map.remove(fieldName)
            );
        }
        for (Map.Entry<String, List<SingleFieldPath>> entry : headersMap.entrySet()) {
            // headers may point to many values, though it's usually close to 1
            for (SingleFieldPath fieldPath : entry.getValue()) {
                final Map.Entry<String, Object> fieldAndValue = values.get(fieldPath);
                updatedHeaders.add(entry.getKey(), fieldAndValue != null ? fieldAndValue.getValue() : null, null);
            }
        }
        return newRecord(record, null, BsonToBinary.toBytes(new Document(updatedValue)), updatedHeaders);
    }

    protected abstract Object operatingValue(R record);
    protected abstract Schema operatingSchema(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders);

    public static class Key<R extends ConnectRecord<R>> extends HeaderFromBson<R> {

        @Override
        public Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends HeaderFromBson<R> {

        @Override
        public Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp(), updatedHeaders);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

}