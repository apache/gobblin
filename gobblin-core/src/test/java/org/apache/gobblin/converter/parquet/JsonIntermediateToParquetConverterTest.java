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
package org.apache.gobblin.converter.parquet;

import java.io.InputStreamReader;
import java.lang.reflect.Type;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.Extract;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import parquet.schema.MessageType;

import static org.testng.Assert.assertEquals;


@Test(groups = {"gobblin.converter"})
public class JsonIntermediateToParquetConverterTest {
  private static final String RESOURCE_PATH = "/converter/parquet/JsonIntermediateToParquetConverter.json";
  private static JsonObject testCases;
  private static WorkUnitState workUnit;
  private static JsonIntermediateToParquetConverter parquetConverter;

  @BeforeClass
  public static void setUp() {
    Type listType = new TypeToken<JsonObject>() {
    }.getType();
    Gson gson = new Gson();
    JsonObject testData = gson.fromJson(
        new InputStreamReader(JsonIntermediateToParquetConverter.class.getResourceAsStream(RESOURCE_PATH)), listType);

    testCases = testData.getAsJsonObject();
    SourceState source = new SourceState();
    workUnit = new WorkUnitState(
        source.createWorkUnit(source.createExtract(Extract.TableType.SNAPSHOT_ONLY, "test_namespace", "test_table")));
  }

  @Test
  public void testPrimitiveTypes()
      throws Exception {
    JsonObject test = testCases.get("simplePrimitiveTypes").getAsJsonObject();
    parquetConverter = new JsonIntermediateToParquetConverter();

    MessageType schema = parquetConverter.convertSchema(test.get("schema").getAsJsonArray(), workUnit);
    ParquetGroup record =
        parquetConverter.convertRecord(schema, test.get("record").getAsJsonObject(), workUnit).iterator().next();

    assertEqualsIgnoreSpaces(schema.toString(), test.get("expectedSchema").getAsString());
    assertEqualsIgnoreSpaces(record.toString(), test.get("expectedRecord").getAsString());
  }

  @Test
  public void testArrayType()
      throws Exception {
    JsonObject test = testCases.get("array").getAsJsonObject();
    parquetConverter = new JsonIntermediateToParquetConverter();

    MessageType schema = parquetConverter.convertSchema(test.get("schema").getAsJsonArray(), workUnit);
    ParquetGroup record =
        parquetConverter.convertRecord(schema, test.get("record").getAsJsonObject(), workUnit).iterator().next();

    assertEqualsIgnoreSpaces(schema.toString(), test.get("expectedSchema").getAsString());
    assertEqualsIgnoreSpaces(record.toString(), test.get("expectedRecord").getAsString());
  }

  @Test
  public void testEnumType()
      throws Exception {
    JsonObject test = testCases.get("enum").getAsJsonObject();
    parquetConverter = new JsonIntermediateToParquetConverter();

    MessageType schema = parquetConverter.convertSchema(test.get("schema").getAsJsonArray(), workUnit);
    ParquetGroup record =
        parquetConverter.convertRecord(schema, test.get("record").getAsJsonObject(), workUnit).iterator().next();

    assertEqualsIgnoreSpaces(schema.toString(), test.get("expectedSchema").getAsString());
    assertEqualsIgnoreSpaces(record.toString(), test.get("expectedRecord").getAsString());
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Symbol .* does not belong to set \\[.*?\\]")
  public void testEnumTypeBelongsToEnumSet()
      throws Exception {
    JsonObject test = testCases.get("enum").getAsJsonObject();
    parquetConverter = new JsonIntermediateToParquetConverter();

    MessageType schema = parquetConverter.convertSchema(test.get("schema").getAsJsonArray(), workUnit);
    JsonObject jsonRecord = test.get("record").getAsJsonObject();
    jsonRecord.addProperty("some_enum", "HELL");

    parquetConverter.convertRecord(schema, jsonRecord, workUnit).iterator().next();
  }

  @Test
  public void testRecordType()
      throws Exception {
    JsonObject test = testCases.get("record").getAsJsonObject();
    parquetConverter = new JsonIntermediateToParquetConverter();

    MessageType schema = parquetConverter.convertSchema(test.get("schema").getAsJsonArray(), workUnit);
    ParquetGroup record =
        parquetConverter.convertRecord(schema, test.get("record").getAsJsonObject(), workUnit).iterator().next();

    assertEqualsIgnoreSpaces(schema.toString(), test.get("expectedSchema").getAsString());
    assertEqualsIgnoreSpaces(record.toString(), test.get("expectedRecord").getAsString());
  }
  private void assertEqualsIgnoreSpaces(String actual, String expected) {
    assertEquals(actual.replaceAll("\\n", ";").replaceAll("\\s|\\t", ""),
        expected.replaceAll("\\n", ";").replaceAll("\\s|\\t", ""));
  }
}