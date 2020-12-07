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

package org.apache.gobblin.converter.avro;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Unit test for {@link JsonRecordAvroSchemaToAvroConverter}
 */
@Test(groups = {"gobblin.converter"})
public class JsonRecordAvroSchemaToAvroConverterTest {

  @Test
  public void testConverter() throws Exception {
    String avroSchemaString = readResource("/converter/jsonToAvroSchema.avsc");
    WorkUnitState workUnitState = createWorkUnitState(avroSchemaString, "fieldToIgnore");
    JsonObject jsonObject = new JsonParser().parse(readResource("/converter/jsonToAvroRecord.json")).getAsJsonObject();

    JsonRecordAvroSchemaToAvroConverter<String> converter = new JsonRecordAvroSchemaToAvroConverter<>();
    converter.init(workUnitState);
    Schema avroSchema = converter.convertSchema(avroSchemaString, workUnitState);
    GenericRecord record = converter.convertRecord(avroSchema, jsonObject, workUnitState).iterator().next();

    Assert.assertNull(record.get("fieldToIgnore"));
    Assert.assertNull(record.get("nullableField"));
    Assert.assertEquals(record.get("longField"), 1234L);

    Assert.assertTrue(record.get("arrayField") instanceof GenericArray);

    Assert.assertTrue(record.get("mapField") instanceof Map);

    Assert.assertEquals(((GenericRecord) record.get("nestedRecords")).get("nestedField").toString(), "test");
    Assert.assertEquals(((GenericRecord) record.get("nestedRecords")).get("nestedField2").toString(), "test2");

    Assert.assertTrue(((GenericArray) record.get("emptyArrayOfRecords")).isEmpty());

    GenericRecord recordInArray = (GenericRecord) (((GenericArray) record.get("arrayOfRecords")).get(0));
    Assert.assertEquals(recordInArray.get("field1").toString(), "test1");

    Assert.assertEquals((record.get("enumField")).toString(), "ENUM2");

    Assert.assertTrue(record.get("arrayFieldWithUnion") instanceof GenericArray);
    GenericArray arrayWithUnion =  (GenericArray) record.get("arrayFieldWithUnion");
    Assert.assertEquals(arrayWithUnion.size(), 4);
    Assert.assertEquals(arrayWithUnion.get(0).toString(), "arrU1");
    Assert.assertEquals(arrayWithUnion.get(1).toString(), "arrU2");
    Assert.assertEquals(arrayWithUnion.get(2).toString(), "arrU3");
    Assert.assertEquals(arrayWithUnion.get(3), 123L);

    Assert.assertTrue(record.get("nullArrayFieldWithUnion") instanceof GenericArray);
    GenericArray nullArrayWithUnion =  (GenericArray) record.get("nullArrayFieldWithUnion");
    Assert.assertEquals(nullArrayWithUnion.size(), 1);
    Assert.assertNull(nullArrayWithUnion.get(0));

    Assert.assertTrue(record.get("arrayFieldWithUnion2") instanceof GenericArray);
    GenericArray arrayWithUnion2 =  (GenericArray) record.get("arrayFieldWithUnion2");
    Assert.assertEquals(arrayWithUnion2.size(), 3);
    Assert.assertEquals(arrayWithUnion2.get(0).toString(), "arrU1");
    Assert.assertNull(arrayWithUnion2.get(1));
    Assert.assertEquals(arrayWithUnion2.get(2).toString(), "arrU3");  }

  @Test(expectedExceptions = DataConversionException.class)
  public void testConverterThrowsOnUnrecognizedEnumSymbols() throws IOException, DataConversionException {
    String avroSchemaString = "{\"name\": \"TestRecord\", "
        + "\"type\": \"record\","
        + "\"namespace\": \"org.apache.gobblin.test\", "
        + "\"fields\": [ "
        + "    { "
        + "      \"name\": \"color\", "
        + "      \"type\": { "
        + "         \"type\": \"enum\","
        + "         \"name\": \"Colors\","
        + "         \"symbols\" : [\"RED\", \"GREEN\", \"BLUE\"]"
        + "       }"
        + "    } "
        + "  ]"
        + "}";
    String jsonString = "{\"color\": \"PURPLE\"}";  // PURPLE isn't a member of the Colors enum

    WorkUnitState workUnitState = createWorkUnitState(avroSchemaString, null);
    JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
    JsonRecordAvroSchemaToAvroConverter<String> converter = new JsonRecordAvroSchemaToAvroConverter<>();
    converter.init(workUnitState);
    Schema avroSchema = converter.convertSchema(avroSchemaString, workUnitState);

    converter.convertRecord(avroSchema, jsonObject, workUnitState);
  }

  private static WorkUnitState createWorkUnitState(String avroSchemaString, String fieldToIgnore) {
    SourceState sourceState = new SourceState();
    WorkUnitState workUnitState = new WorkUnitState(
        sourceState.createWorkUnit(sourceState.createExtract(TableType.SNAPSHOT_ONLY, "test_table", "test_namespace")));
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_SCHEMA_KEY, avroSchemaString);
    if (fieldToIgnore != null) {
      workUnitState.setProp(ConfigurationKeys.CONVERTER_IGNORE_FIELDS, fieldToIgnore);
    }
    return workUnitState;
  }

  private static String readResource(String path) throws IOException {
    return IOUtils.toString(JsonRecordAvroSchemaToAvroConverterTest.class.getResourceAsStream(path), StandardCharsets.UTF_8);
  }
}