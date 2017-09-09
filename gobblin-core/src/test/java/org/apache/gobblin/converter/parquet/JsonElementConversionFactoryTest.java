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

import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.BooleanConverter;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.DoubleConverter;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.FloatConverter;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.IntConverter;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.LongConverter;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.StringConverter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;

import static org.testng.Assert.assertEquals;

@Test(groups = {"gobblin.converter"})
public class JsonElementConversionFactoryTest {

  private static final String RESOURCE_PATH = "/converter/parquet/JsonElementConversionFactory.json";
  private static JsonObject testCases;

  @BeforeClass
  public static void setUp() {
    Type listType = new TypeToken<JsonObject>() {
    }.getType();
    Gson gson = new Gson();
    JsonObject testData = gson.fromJson(
        new InputStreamReader(JsonIntermediateToParquetConverter.class.getResourceAsStream(RESOURCE_PATH)), listType);

    testCases = testData.getAsJsonObject();
  }

  @Test
  public void testPrimitiveConverters()
      throws Exception {
    JsonArray testCase = testCases.get("int").getAsJsonArray();
    JsonArray testCase1 = testCases.get("float").getAsJsonArray();
    JsonArray testCase2 = testCases.get("boolean").getAsJsonArray();
    JsonArray testCase3 = testCases.get("string").getAsJsonArray();
    IntConverter intConverter = new IntConverter("some", false, "");
    LongConverter longConverter = new LongConverter("some", false, "");
    FloatConverter floatConverter = new FloatConverter("some", false, "");
    DoubleConverter doubleConverter = new DoubleConverter("some", false, "");
    BooleanConverter booleanConverter = new BooleanConverter("some", false, "");
    StringConverter stringConverter = new StringConverter("some", false, "");

    Object result = intConverter.convert(testCase.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());
    Object result1 = longConverter.convert(testCase.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());
    Object result2 = floatConverter.convert(testCase1.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());
    Object result3 = doubleConverter.convert(testCase1.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());
    Object result4 = booleanConverter.convert(testCase2.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());
    Object result5 = stringConverter.convert(testCase3.get(0).getAsJsonObject().get("temp").getAsJsonPrimitive());

    assertEquals(result.getClass(), IntegerValue.class);
    assertEquals(result1.getClass(), LongValue.class);
    assertEquals(result2.getClass(), FloatValue.class);
    assertEquals(result3.getClass(), DoubleValue.class);
    assertEquals(result4.getClass(), BooleanValue.class);
    assertEquals(result5.getClass(), BinaryValue.class);
    assertEquals(Integer.parseInt(result.toString()), 5);
    assertEquals(Integer.parseInt(result1.toString()), 5);
    assertEquals(Float.parseFloat(result2.toString()), 5.0f);
    assertEquals(Double.parseDouble(result3.toString()), 5.0);
    assertEquals(Boolean.parseBoolean(result4.toString()), true);
    assertEquals(result5.toString(), "somestring");
  }

  @Test
  public void testPrimitiveConverterSchema()
      throws Exception {
    IntConverter intConverter = new IntConverter("some", false, "");
    LongConverter longConverter = new LongConverter("some", false, "");
    FloatConverter floatConverter = new FloatConverter("some", false, "");
    DoubleConverter doubleConverter = new DoubleConverter("some", false, "");
    BooleanConverter booleanConverter = new BooleanConverter("some", false, "");
    StringConverter stringConverter = new StringConverter("some", false, "");

    Object result = intConverter.schema();
    Object result1 = longConverter.schema();
    Object result2 = floatConverter.schema();
    Object result3 = doubleConverter.schema();
    Object result4 = booleanConverter.schema();
    Object result5 = stringConverter.schema();

    assertEquals(result.toString(), "required int32 some");
    assertEquals(result1.toString(), "required int64 some");
    assertEquals(result2.toString(), "required float some");
    assertEquals(result3.toString(), "required double some");
    assertEquals(result4.toString(), "required boolean some");
    assertEquals(result5.toString(), "required binary some");
  }

}