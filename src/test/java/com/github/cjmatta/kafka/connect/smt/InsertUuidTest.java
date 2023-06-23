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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;


import static org.junit.Assert.*;

public class InsertUuidTest {

  private InsertUuid<SourceRecord> xform = new InsertUuid.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test(expected = DataException.class)
  public void topLevelStructRequired() {
    final Map<String, Object> props = new HashMap<>();
    props.put("uuid.field.name", "myUuid");
    props.put("path.value.pattern", "myValue");
    xform.configure(props);
    xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
  }

  @Test
  public void copySchemaAndFindPatternElement() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("array.element.path", "quux");
    props.put("path.value.pattern", "C.t");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);

    final Schema elementStructSchema = SchemaBuilder.struct()
        .name("name")
        .version(1)
        .doc("doc")
        .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .field("bar", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .field("baz", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .field("qux", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .field("quux", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .field("corge", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    
    final Schema simpleStructSchema = SchemaBuilder.struct()
        .name("name")
        .version(1)
        .doc("doc")
        .field("arrayField", SchemaBuilder.array(elementStructSchema).optional().build())
        .build();
        
    final Struct m1 = new Struct(elementStructSchema)
        .put("foo", "Cat")
        .put("bar", "Cut")
        .put("baz", "Cot");
    final Struct m2 = new Struct(elementStructSchema)
        .put("qux", "Cat")
        .put("quux", "Cut")
        .put("corge", "Cot");
    final Struct m3 = new Struct(elementStructSchema)
        .put("qux", "Bat")
        .put("quux", "But")
        .put("corge", "Bot");
    List<Struct> arr = new ArrayList<>(Arrays.asList(m1, m2, m3)); 
    final Struct simpleStruct = new Struct(simpleStructSchema)
        .put("arrayField", arr);

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    // lazy hash, cant use assertEquals
    assertTrue(
      SchemaBuilder.array(elementStructSchema).optional().build().equals(
        transformedRecord.valueSchema().field("arrayField").schema()
        ));
    assertEquals(arr, ((Struct) transformedRecord.value()).get("arrayField"));
    assertEquals(elementStructSchema, transformedRecord.valueSchema().field("elementValue").schema());
    assertNotNull(((Struct) transformedRecord.value()).get("elementValue").toString());

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }


  @Test
  public void copySchemaAndFindExpectedElement() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("path.value", "bar");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct()
        .name("name")
        .version(1)
        .doc("doc")
        .field("arrayField", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
        .build();
        
    final Struct simpleStruct = new Struct(simpleStructSchema)
        .put("arrayField", Arrays.asList("foo", "bar"));

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
    assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
    assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

    // lazy hash, cant use assertEquals
    assertTrue(
      SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build().equals(
        transformedRecord.valueSchema().field("arrayField").schema()
        ));
    assertEquals(Arrays.asList("foo", "bar"), ((Struct) transformedRecord.value()).get("arrayField"));
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("elementValue").schema());
    assertNotNull(((Struct) transformedRecord.value()).getString("elementValue"));

    // Exercise caching
    final SourceRecord transformedRecord2 = xform.apply(
      new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
    assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

  }

  @Test
  public void schemalessFindExpectedValuePath() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("array.element.path", "quux");
    props.put("path.value", "Cut");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);
    Map<String, String> m1 = new HashMap<>();
    m1.put("foo", "Cat");
    m1.put("bar", "Cut");
    m1.put("baz", "Cot");
    Map<String, String> m2 = new HashMap<>();
    m2.put("qux", "Cat");
    m2.put("quux", "Cut");
    m2.put("corge", "Cot");
    Map<String, String> m3 = new HashMap<>();
    m3.put("qux", "Bat");
    m3.put("quux", "But");
    m3.put("corge", "Bot");
    Object[] arr = {m1, m2, m3};
    final SourceRecord record = new SourceRecord(null, null, "test", 0,
    null, Collections.singletonMap("arrayField", arr));

    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(arr, ((Map) transformedRecord.value()).get("arrayField"));
    assertNotNull(((Map) transformedRecord.value()).get("elementValue"));
    assertEquals(m2, ((Map) transformedRecord.value()).get("elementValue"));    
  }

  @Test
  public void schemalessFindExpectedElement() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("path.value", "bar");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);
    String[] arr = {"foo", "bar"};
    final SourceRecord record = new SourceRecord(null, null, "test", 0,
    null, Collections.singletonMap("arrayField", arr));

    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(arr, ((Map) transformedRecord.value()).get("arrayField"));
    assertNotNull(((Map) transformedRecord.value()).get("elementValue"));
    assertEquals("bar", ((Map) transformedRecord.value()).get("elementValue"));    
  }

  @Test
  public void schemalessFindPatternPath() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("array.element.path", "quux");
    props.put("path.value.pattern", "C.t");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);
    Map<String, String> m1 = new HashMap<>();
    m1.put("foo", "Cat");
    m1.put("bar", "Cut");
    m1.put("baz", "Cot");
    Map<String, String> m2 = new HashMap<>();
    m2.put("qux", "Cat");
    m2.put("quux", "Cut");
    m2.put("corge", "Cot");
    Map<String, String> m3 = new HashMap<>();
    m3.put("qux", "Bat");
    m3.put("quux", "But");
    m3.put("corge", "Bot");
    Object[] arr = {m1, m2, m3};
    final SourceRecord record = new SourceRecord(null, null, "test", 0,
    null, Collections.singletonMap("arrayField", arr));

    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(arr, ((Map) transformedRecord.value()).get("arrayField"));
    assertNotNull(((Map) transformedRecord.value()).get("elementValue"));
    assertEquals(m2, ((Map) transformedRecord.value()).get("elementValue"));    
  }

  @Test
  public void schemalessPatternElement() {
    final Map<String, Object> props = new HashMap<>();

    props.put("array.field.name", "arrayField");
    props.put("path.value.pattern", "ba.");
    props.put("uuid.field.name", "elementValue");

    xform.configure(props);
    String[] arr = {"foo", "bar","baz"};
    final SourceRecord record = new SourceRecord(null, null, "test", 0,
    null, Collections.singletonMap("arrayField", arr));

    final SourceRecord transformedRecord = xform.apply(record);

    assertEquals(arr, ((Map) transformedRecord.value()).get("arrayField"));
    assertNotNull(((Map) transformedRecord.value()).get("elementValue"));
    assertEquals("bar", ((Map) transformedRecord.value()).get("elementValue"));    
  }

}

