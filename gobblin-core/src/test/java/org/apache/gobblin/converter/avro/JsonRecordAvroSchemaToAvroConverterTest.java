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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Unit test for {@link JsonRecordAvroSchemaToAvroConverter}
 */
@Test(groups = {"gobblin.converter"})
public class JsonRecordAvroSchemaToAvroConverterTest {
  private JsonObject jsonRecord;
  private WorkUnitState state;

  @BeforeClass
  public void setUp()
      throws Exception {
    String avroSchemaString = IOUtils.toString(this.getClass().getResourceAsStream("/converter/jsonToAvroSchema.avsc"), StandardCharsets.UTF_8);

    this.jsonRecord = new JsonParser().parse(IOUtils.toString(this.getClass().getResourceAsStream(
        "/converter/jsonToAvroRecord.json"), StandardCharsets.UTF_8)).getAsJsonObject();

    SourceState source = new SourceState();
    this.state = new WorkUnitState(
        source.createWorkUnit(source.createExtract(TableType.SNAPSHOT_ONLY, "test_table", "test_namespace")));
    this.state.setProp(JsonRecordAvroSchemaToAvroConverter.AVRO_SCHEMA_KEY, avroSchemaString);
    this.state.setProp(JsonRecordAvroSchemaToAvroConverter.IGNORE_FIELDS, "fieldToIgnore");
  }

  @Test
  public void testConverter()
      throws Exception {
    JsonRecordAvroSchemaToAvroConverter<String> converter = new JsonRecordAvroSchemaToAvroConverter<>();

    converter.init(this.state);

    Schema avroSchema = converter.convertSchema("dummy", this.state);

    GenericRecord record = converter.convertRecord(avroSchema, this.jsonRecord, this.state).iterator().next();

    Assert.assertEquals(record.get("fieldToIgnore"), null);
    Assert.assertEquals(record.get("nullableField"), null);
    Assert.assertEquals(record.get("longField"), 1234L);

    Assert.assertTrue(record.get("arrayField") instanceof GenericArray);

    Assert.assertTrue(record.get("mapField") instanceof Map);

    Assert.assertEquals(((GenericRecord)record.get("nestedRecords")).get("nestedField").toString(), "test");
    Assert.assertEquals(((GenericRecord)record.get("nestedRecords")).get("nestedField2").toString(), "test2");
  }
}
