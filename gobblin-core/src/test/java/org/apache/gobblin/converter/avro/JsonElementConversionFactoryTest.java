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

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.NullConverter;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.RecordConverter;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.ArrayConverter;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.JsonElementConverter.buildNamespace;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.ARRAY;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.MAP;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.RECORD;
import static org.apache.gobblin.converter.avro.JsonElementConversionFactory.Type.UNION;


/**
 * Unit test for {@link JsonElementConversionFactory}
 *
 * @author Tilak Patidar
 */
public class JsonElementConversionFactoryTest {

  private static WorkUnitState state;

  @BeforeClass
  public static void setUp() {
    WorkUnit workUnit = new WorkUnit(new SourceState(),
        new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
    state = new WorkUnitState(workUnit);
  }

  @Test
  public void schemaWithArrayOfMaps()
      throws Exception {
    String schema =
        "{\"columnName\":\"b\",\"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"map\", \"values\":\"string\"}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"source.type\":\"string\"},\"source.type\":\"map\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter = new ArrayConverter("dummy", true, ARRAY.toString(), buildJsonObject(schema), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithArrayOfRecords()
      throws Exception {
    String schema =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"record\", \"namespace\":\"org.foo\", \"values\":[{\"columnName\": \"name\", \"dataType\":{\"type\":\"string\"}},{\"columnName\": \"c\", \"dataType\":{\"type\":\"long\"}},{\"columnName\": \"cc\", \"dataType\":{\"type\":\"array\", \"items\":\"int\"}}]}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"\",\"source.type\":\"string\"},{\"name\":\"c\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"\",\"source.type\":\"long\"},{\"name\":\"cc\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"},\"doc\":\"\",\"source.type\":\"array\"}],\"source.type\":\"record\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schema), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecord()
      throws DataConversionException, SchemaConversionException, UnsupportedDateTypeException {
    String schemaStr =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"record\", \"values\":[{\"columnName\":\"c\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"d\",\"dataType\":{\"type\":\"int\"}}]}}";
    String expected =
        "{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"c\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"\",\"source.type\":\"string\"},{\"name\":\"d\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"\",\"source.type\":\"int\"}],\"source.type\":\"record\"}";

    RecordConverter recordConverter =
        new RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr), state,
            buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithArrayOfInts()
      throws Exception {
    String schemaStr = "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":\"int\"}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"}";

    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithNullType() {
    NullConverter nullConverter = new NullConverter("dummy1", ARRAY.toString());

    Assert.assertEquals(nullConverter.schema().toString(), "{\"type\":\"null\",\"source.type\":\"array\"}");
  }

  @Test
  public void schemaWithArrayOfEnums()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":\"array\", \"items\":{\"dataType\":{\"type\":\"enum\", \"namespace\":\"org.foo\", \"dataType\":{\"name\":\"choice\", \"symbols\":[\"YES\", \"NO\"]}}}}}";
    String expected =
        "{\"type\":\"array\",\"items\":{\"type\":\"enum\",\"name\":\"choice\",\"doc\":\"\",\"symbols\":[\"YES\",\"NO\"],\"source.type\":\"enum\"},\"source.type\":\"array\"}";
    ArrayConverter arrayConverter =
        new ArrayConverter("dummy1", true, ARRAY.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(arrayConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithMap()
      throws Exception {
    String schemaStr = "{\"columnName\":\"b\",\"dataType\":{\"type\":\"map\", \"values\":\"string\"}}";
    String expected =
        "{\"type\":\"map\",\"values\":{\"type\":\"string\",\"source.type\":\"string\"},\"source.type\":\"map\"}";

    JsonElementConversionFactory.MapConverter mapConverter =
        new JsonElementConversionFactory.MapConverter("dummy1", true, MAP.toString(), buildJsonObject(schemaStr),
            state);

    Assert.assertEquals(mapConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithMapOfRecords()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"record\",\"values\":[{\"columnName\":\"name\", \"dataType\":{\"type\":\"string\"}}, {\"columnName\":\"age\", \"dataType\":{\"type\":\"int\"}}]}}}}";
    String expected =
        "{\"type\":\"map\",\"values\":{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"\",\"source.type\":\"string\"},{\"name\":\"age\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"\",\"source.type\":\"int\"}],\"source.type\":\"record\"},\"source.type\":\"map\"}";
    JsonElementConversionFactory.MapConverter mapConverter =
        new JsonElementConversionFactory.MapConverter("dummy1", true, MAP.toString(), buildJsonObject(schemaStr),
            state);

    Assert.assertEquals(mapConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithMapOfArrays()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"array\",\"items\":\"int\"}}}}";
    String expected =
        "{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"},\"source.type\":\"map\"}";
    JsonElementConversionFactory.MapConverter mapConverter =
        new JsonElementConversionFactory.MapConverter("dummy1", true, MAP.toString(), buildJsonObject(schemaStr),
            state);

    Assert.assertEquals(mapConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithMapOfEnum()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"map\", \"values\":{\"dataType\":{\"type\":\"enum\",\"name\":\"choice\",\"symbols\":[\"YES\",\"NO\"]}}}}";
    String expected =
        "{\"type\":\"map\",\"values\":{\"type\":\"enum\",\"name\":\"choice\",\"doc\":\"\",\"symbols\":[\"YES\",\"NO\"],\"source.type\":\"enum\"},\"source.type\":\"map\"}";
    JsonElementConversionFactory.MapConverter mapConverter =
        new JsonElementConversionFactory.MapConverter("dummy1", true, MAP.toString(), buildJsonObject(schemaStr),
            state);

    Assert.assertEquals(mapConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecordOfMap()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"map\",\"values\":\"string\"}}]}}";
    String expected =
        "{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"source.type\":\"string\"},\"source.type\":\"map\"},\"doc\":\"\",\"source.type\":\"map\"}],\"source.type\":\"record\"}";
    JsonElementConversionFactory.RecordConverter recordConverter =
        new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
            state, buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecordOfArray()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"array\",\"items\":\"int\"}}]}}";
    String expected =
        "{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"int\",\"source.type\":\"int\"},\"source.type\":\"array\"},\"doc\":\"\",\"source.type\":\"array\"}],\"source.type\":\"record\"}";
    JsonElementConversionFactory.RecordConverter recordConverter =
        new RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr), state,
            buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecordOfEnum()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"name\": \"choice\", \"type\":\"enum\",\"symbols\":[\"YES\", \"NO\"]}}]}}";
    String expected =
        "{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"enum\",\"name\":\"choice\",\"doc\":\"\",\"symbols\":[\"YES\",\"NO\"],\"source.type\":\"enum\"},\"doc\":\"\",\"source.type\":\"enum\"}],\"source.type\":\"record\"}";
    JsonElementConversionFactory.RecordConverter recordConverter =
        new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
            state, buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void schemaWithMapValuesAsJsonArray()
      throws Exception {

    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"map\",\"values\":[\"string\"]}}]}}";

    new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
        state, buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void schemaWithMapValuesAsJsonNull()
      throws Exception {

    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"type\":\"map\",\"values\":null}}]}}";
    new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
        state, buildNamespace(state.getExtract().getNamespace(), "something"));
  }

  @Test
  public void schemaWithRecordOfRecord()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"name\": \"choice\", \"type\":\"record\",\"values\":[{\"columnName\":\"s\", \"dataType\":{\"type\":\"int\"}}]}}]}}";
    String expected =
        "{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"record\",\"name\":\"choice\",\"doc\":\"\",\"fields\":[{\"name\":\"s\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"\",\"source.type\":\"int\"}],\"source.type\":\"record\"},\"doc\":\"\",\"source.type\":\"record\"}],\"source.type\":\"record\"}";
    JsonElementConversionFactory.RecordConverter recordConverter =
        new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
            state, buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithRecordOfRecordCheckNamespace()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\", \"name\":\"myrecord\", \"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"name\": \"choice\", \"type\":\"record\",\"values\":[{\"columnName\":\"s\", \"dataType\":{\"type\":\"int\"}}]}}]}}";
    String expected =
        "{\"type\":\"record\",\"name\":\"myrecord\",\"namespace\":\"namespace.person\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"record\",\"name\":\"choice\",\"namespace\":\"namespace.person.myrecord\",\"doc\":\"\",\"fields\":[{\"name\":\"s\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"\",\"source.type\":\"int\"}],\"source.type\":\"record\"},\"doc\":\"\",\"source.type\":\"record\"}],\"source.type\":\"record\"}";

    JsonElementConversionFactory.RecordConverter recordConverter =
        new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
            state, buildNamespace(state.getExtract().getNamespace(), "person"));
    Assert.assertEquals(recordConverter.schema().toString(), expected);
    Assert.assertEquals(recordConverter.schema().getField("someperson").schema().getNamespace(),
        "namespace.person.myrecord");
    Assert.assertEquals(recordConverter.schema().getNamespace(), "namespace.person");
  }

  @Test
  public void schemaWithRecordOfEnumCheckNamespace()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"persons\", \"dataType\": {\"type\":\"record\",\"name\":\"myrecord\",\"values\":[{\"columnName\": \"someperson\", \"dataType\":{\"name\": \"choice\", \"type\":\"enum\",\"symbols\":[\"YES\", \"NO\"]}}]}}";
    String expected =
        "{\"type\":\"record\",\"name\":\"myrecord\",\"namespace\":\"namespace.something\",\"doc\":\"\",\"fields\":[{\"name\":\"someperson\",\"type\":{\"type\":\"enum\",\"name\":\"choice\",\"namespace\":\"namespace.something.myrecord\",\"doc\":\"\",\"symbols\":[\"YES\",\"NO\"],\"source.type\":\"enum\"},\"doc\":\"\",\"source.type\":\"enum\"}],\"source.type\":\"record\"}";
    JsonElementConversionFactory.RecordConverter recordConverter =
        new JsonElementConversionFactory.RecordConverter("dummy1", true, RECORD.toString(), buildJsonObject(schemaStr),
            state, buildNamespace(state.getExtract().getNamespace(), "something"));

    Assert.assertEquals(recordConverter.schema().toString(), expected);
    Assert.assertEquals(recordConverter.schema().getField("someperson").schema().getNamespace(),
        "namespace.something.myrecord");
    Assert.assertEquals(recordConverter.schema().getNamespace(), "namespace.something");
  }

  @Test
  public void schemaWithUnion()
      throws Exception {
    String schemaStr = "{\"columnName\":\"b\", \"dataType\":{\"type\": [\"null\", \"string\"]}}";
    String expected = "[{\"type\":\"null\",\"source.type\":\"null\"},{\"type\":\"string\",\"source.type\":\"string\"}]";

    JsonElementConversionFactory.UnionConverter unionConverter =
        new JsonElementConversionFactory.UnionConverter("dummy1", UNION.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(unionConverter.schema().toString(), expected);
  }

  @Test
  public void schemaWithComplexUnion()
      throws Exception {
    String schemaStr =
        "{\"columnName\":\"b\", \"dataType\":{\"type\":[\"null\",{\"dataType\":{\"type\":\"enum\",\"name\":\"someenum\",\"symbols\":[\"HELL\",\"BELLS\"]}}]}}";
    String expected =
        "[{\"type\":\"null\",\"source.type\":\"null\"},{\"type\":\"enum\",\"name\":\"someenum\",\"doc\":\"\",\"symbols\":[\"HELL\",\"BELLS\"],\"source.type\":\"enum\"}]";

    JsonElementConversionFactory.UnionConverter unionConverter =
        new JsonElementConversionFactory.UnionConverter("dummy1", UNION.toString(), buildJsonObject(schemaStr), state);

    Assert.assertEquals(unionConverter.schema().toString(), expected);
  }

  /**
   * Build a JsonObject from a json string using a JsonParser.
   * @param jsonStr
   * @return
   */
  private static JsonObject buildJsonObject(String jsonStr) {
    JsonParser parser = new JsonParser();
    return parser.parse(jsonStr).getAsJsonObject();
  }
}