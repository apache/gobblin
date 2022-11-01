/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util;

import com.google.common.collect.Lists;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSchemaUtilsTest {
  
  @Test
  public void testGetValueAsInteger() throws IOException {
    Schema schema = readSchemaFromJsonFile("props/schema_with_logical_field.json");
    Schema.Field field = schema.getField("logicalFieldDecimal");
    Schema fieldSchema = field.schema();
    Assert.assertEquals(AvroSchemaUtils.getValueAsInteger(fieldSchema, "precision").intValue(), 4);
    Assert.assertEquals(AvroSchemaUtils.getValueAsInteger(fieldSchema, "scale").intValue(), 2);
  }
  
  @Test
  public void testCopySchemaProperties() throws IOException {

    Schema fromSchema = Schema.createRecord("name", "", "namespace", false);
    fromSchema.addProp("prop1", "val1");
    fromSchema.addProp("prop2", "val2");
    List<Schema.Field> fieldList = Lists.newArrayList();
    Schema.Field field1 =
        AvroCompatibilityHelper.createSchemaField("key", Schema.create(Schema.Type.LONG), "", 0L);
    fieldList.add(field1);
    Schema.Field field2 =
        AvroCompatibilityHelper.createSchemaField("double", Schema.create(Schema.Type.DOUBLE), "", 0.0);
    fieldList.add(field2);

    fromSchema.setFields(Lists.newArrayList(fieldList));

    Schema toSchema = readSchemaFromJsonFile("props/schema_without_props.json");
    AvroSchemaUtils.copySchemaProperties(fromSchema, toSchema);

    
    Assert.assertEquals(fromSchema.toString(), toSchema.toString());
    for(Schema.Field field : toSchema.getFields()) {
      Schema.Field oldField = fromSchema.getField(field.name());
      Assert.assertEquals(field.toString(), oldField.toString());
    }

    Assert.assertTrue(fromSchema.getObjectProps().equals(toSchema.getObjectProps()));
  }

  @Test
  public void testCopySchemaPropertiesWithAdditionalProps() throws IOException {

    Schema fromSchema = readSchemaFromJsonFile("props/schema_with_props.json");
    fromSchema.addProp("prop3", "val3");
    fromSchema.addProp("prop4", "val4");

    Schema toSchema = readSchemaFromJsonFile("props/schema_without_props.json");
    AvroSchemaUtils.copySchemaProperties(fromSchema, toSchema);


    Assert.assertEquals(fromSchema.toString(), toSchema.toString());
    for(Schema.Field field : toSchema.getFields()) {
      Schema.Field oldField = fromSchema.getField(field.name());
      Assert.assertEquals(field.toString(), oldField.toString());
    }

    Assert.assertTrue(fromSchema.getObjectProps().equals(toSchema.getObjectProps()));
  }

  @Test
  public void testCopyFieldProperties() throws IOException {

    Schema fromSchema = Schema.createRecord("name", "", "namespace", false);
    fromSchema.addProp("prop1", "val1");
    fromSchema.addProp("prop2", "val2");
    List<Schema.Field> fieldList = Lists.newArrayList();
    Schema.Field field1 =
        AvroCompatibilityHelper.createSchemaField("key", Schema.create(Schema.Type.LONG), "", 0L);
    field1.addProp("primaryKey", "true");
    fieldList.add(field1);
    Schema.Field field2 =
        AvroCompatibilityHelper.createSchemaField("double", Schema.create(Schema.Type.DOUBLE), "", 0.0);
    fieldList.add(field2);

    fromSchema.setFields(Lists.newArrayList(fieldList));

    Schema toSchema = readSchemaFromJsonFile("props/schema_without_field_props.json");
    AvroSchemaUtils.copyFieldProperties(fromSchema.getField("key"), toSchema.getField("key"));


    Assert.assertEquals(fromSchema.toString(), toSchema.toString());
    for(Schema.Field field : toSchema.getFields()) {
      Schema.Field oldField = fromSchema.getField(field.name());
      Assert.assertEquals(field.toString(), oldField.toString());
    }

    Assert.assertTrue(fromSchema.getObjectProps().equals(toSchema.getObjectProps()));
  }

  @Test
  public void testCopyFieldPropertiesWithAdditionalProps() throws IOException {

    Schema fromSchema = readSchemaFromJsonFile("props/schema_with_field_props.json");
    Schema.Field keyField = fromSchema.getField("key");
    keyField.addProp("key1", "val1");
    Schema.Field doubleField = fromSchema.getField("double");
    doubleField.addProp("key1", "val1");
    doubleField.addProp("key2", "val2");

    Schema toSchema = readSchemaFromJsonFile("props/schema_without_field_props.json");
    Schema.Field toKeyField = toSchema.getField("key");
    Schema.Field toDoubleField = toSchema.getField("double");
    AvroSchemaUtils.copyFieldProperties(keyField, toKeyField);
    AvroSchemaUtils.copyFieldProperties(doubleField, toDoubleField);

    Assert.assertEquals(fromSchema.toString(), toSchema.toString());
    for(Schema.Field field : toSchema.getFields()) {
      Schema.Field oldField = fromSchema.getField(field.name());
      Assert.assertEquals(field.toString(), oldField.toString());
    }

    Assert.assertTrue(fromSchema.getObjectProps().equals(toSchema.getObjectProps()));
  }

  private static Schema readSchemaFromJsonFile(String filename)
      throws IOException {
    return new Schema.Parser()
        .parse(AvroSchemaUtilsTest.class.getClassLoader().getResourceAsStream(filename));
  }

}
