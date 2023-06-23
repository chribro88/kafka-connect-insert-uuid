/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package com.github.cjmatta.kafka.connect.smt;

import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertUuid<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Find element in array that meets condition and insert into a connect record";

  private interface ConfigName {
    String ARRAY_FIELD_NAME = "array.field.name";
    String ELEMENT_FIELD_PATH = "array.element.path";
    String PATH_VALUE = "path.value";
    String PATH_VALUE_PATTERN = "path.value.pattern";
    String UUID_FIELD_NAME = "uuid.field.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.ARRAY_FIELD_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "The field name to search."
            + "If empty, return null")
    .define(ConfigName.ELEMENT_FIELD_PATH,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "The element path to compare condition to search."
            + "If empty, compare element")
    .define(ConfigName.PATH_VALUE,
           ConfigDef.Type.STRING,
           null,
           ConfigDef.Importance.HIGH,
           "Expected value to match. Either define this, or a regex pattern")
    .define(ConfigName.PATH_VALUE_PATTERN,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "The pattern to match. Either define this, or an expected value")
    .define(ConfigName.UUID_FIELD_NAME, 
            ConfigDef.Type.STRING, 
            "uuid", 
            ConfigDef.Importance.HIGH,
            "Field name for UUID");

  private static final String PURPOSE = "adding UUID to record";

  private String arrayFieldName;
  private String fieldName;
  private Optional<String> elementFieldAccessor;
  private Optional<String> fieldExpectedValue;
  private Optional<String> fieldValuePattern;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    arrayFieldName = config.getString(ConfigName.ARRAY_FIELD_NAME);
    elementFieldAccessor = Optional.ofNullable(config.getString(ConfigName.ELEMENT_FIELD_PATH));
    fieldExpectedValue = Optional.ofNullable(config.getString(ConfigName.PATH_VALUE));
    fieldValuePattern = Optional.ofNullable(config.getString(ConfigName.PATH_VALUE_PATTERN));
    fieldName = config.getString(ConfigName.UUID_FIELD_NAME);
    boolean expectedValuePresent = fieldExpectedValue.isPresent();
    boolean regexPatternPresent = fieldValuePattern.map(s -> !s.isEmpty()).orElse(false);
    if (expectedValuePresent == regexPatternPresent) {
      throw new ConfigException(
        "Either path.value or path.value.pattern have to be set to apply filter transform");
    }
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  public static Object getField(Object structOrMap, String fieldName) {
    // validate(structOrMap, fieldName);

    Object field;
    if (structOrMap instanceof Struct) {
      field = ((Struct) structOrMap).get(fieldName);
    } else if (structOrMap instanceof Map) {
      field = ((Map<?, ?>) structOrMap).get(fieldName);
      if (field == null) {
        throw new DataException(String.format("Unable to find nested field '%s'", fieldName));
      }
      return field;
    } else {
      throw new DataException(String.format(
            "Argument not a Struct or Map. Cannot get field '%s'.",
            fieldName
      ));
    }
    // if (field == null) {
    //   throw new DataException(
    //         String.format("The field '%s' does not exist.", fieldName));
    // }
    return field;
  }
  
  public static Object getNestedFieldValue(Object structOrMap, String fieldName) {
    // validate(structOrMap, fieldName);

    try {
      Object innermost = structOrMap;
      // Iterate down to final struct
      for (String name : fieldName.split("\\.")) {
        innermost = getField(innermost, name);
      }
      return innermost;
    } catch (DataException e) {
      return null;
      // throw new DataException(
      //       String.format("The field '%s' does not exist.", fieldName),
      //       e
      // );
    }
  }

  public static Field getNestedField(Schema schema, String fieldName) {
    // validate(schema, fieldName);

    final String[] fieldNames = fieldName.split("\\.");
    try {
      Field innermost = schema.field(fieldNames[0]);
      // Iterate down to final schema
      for (int i = 1; i < fieldNames.length; ++i) {
        innermost = innermost.schema().field(fieldNames[i]);
      }
      return innermost;
    } catch (DataException e) {
      return null;
      // throw new DataException(
      //       String.format("Unable to get field '%s' from schema %s.", fieldName, schema),
      //       e
      // );
    }
  }
  
  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

    final Map<String, Object> updatedValue = new HashMap<>(value);
    
    final Object fieldValue = updatedValue.get(arrayFieldName);
    final Object[] arr = (fieldValue instanceof Object[])? (Object[]) fieldValue : null;
    final String[] tokens = (elementFieldAccessor.isPresent()) ? elementFieldAccessor.get().split("\\.") : new String[0];
    Object element = null;
    // DataUtils.getNestedFieldValue ?
    for (Object el : arr) {
      Object elValue = el;
      boolean found = true;

      for (String token : tokens) {
        if (!(elValue instanceof Map) || !((Map<?, ?>) elValue).containsKey(token)) {
          found = false;
          break;
        }
        elValue = ((Map<?, ?>) elValue).get(token);
      }
      
      if (found && elValue instanceof String) {
        if (fieldExpectedValue.isPresent()) {
            if (((String) elValue).equals(fieldExpectedValue.get())) {
              element = el;
              break;
            }
          } else {
            if (((String) elValue).matches(fieldValuePattern.get())) {
              element = el;
              break;
          }
        }
      } 
      
    }
      
    updatedValue.put(fieldName, element);

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if(updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema());
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));
    }

    // final List<?> fieldValue = updatedValue.get(arrayFieldName) != null  ? (List<?>) updatedValue.get(arrayFieldName) : Arrays.asList();
    boolean fieldAndValueExists = (updatedValue.schema().field(arrayFieldName) != null && updatedValue.get(arrayFieldName) != null);
    final List<?> fieldValue = fieldAndValueExists ? (List<?>) updatedValue.get(arrayFieldName) : (List<?>) Arrays.asList();
    List<?> arr = fieldValue;
    // final String[] tokens = (elementFieldAccessor.isPresent()) ? elementFieldAccessor.get().split("\\.") : new String[0];
    Object element = null;
    for ( Object el : arr) {
      boolean found = true;
      Object elPathValue = el;

      if (elementFieldAccessor.isPresent()){
        String accessor = elementFieldAccessor.get();
        elPathValue = getNestedFieldValue(el, accessor);
        found = elPathValue != null;
      }

      if (found && elPathValue instanceof String) {
        if (fieldExpectedValue.isPresent()) {
            if (((String) elPathValue).equals(fieldExpectedValue.get())) {
              element = el;
              break;
            }
          } else {
            if (((String) elPathValue).matches(fieldValuePattern.get())) {
              element = el;
              break;
          }
        }
      }
    }
    updatedValue.put(fieldName, element);

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private String getRandomUuid() {
    return UUID.randomUUID().toString();
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    Schema elementSchema = Schema.OPTIONAL_STRING_SCHEMA;

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    try {
      // Schema{ARRAY}}
      // Schema{name:STRUCT}
      elementSchema = builder.field(arrayFieldName).schema().valueSchema();
    } catch (NullPointerException e) {
        // throw new DataException(
        //       String.format("Unable to get field '%s' from schema %s.", arrayFieldName, schema),
        //       e
        // );
    }
    
    builder.field(fieldName, elementSchema);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends InsertUuid<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends InsertUuid<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


