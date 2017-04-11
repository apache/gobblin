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

package gobblin.converter.csv;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;

import java.io.IOException;
import java.io.InputStreamReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVParser;

@Test(groups = {"gobblin.converter"})
public class CsvToJsonConverterV2Test {
  private String row11Cols = "20160924,desktop,Dynamic Segment,42935,0.0446255968324211,1590.4702457202748,348380,8.1141260044252945,232467,206.98603475430664,33028";
  private String row10Cols = "20160924,desktop,42935,0.0446255968324211,1590.4702457202748,348380,8.1141260044252945,232467,206.98603475430664,33028";

  public void convertOutput() throws IOException {
    JsonParser parser = new JsonParser();
    JsonElement jsonElement = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/schema_with_10_fields.json")));

    JsonArray outputSchema = jsonElement.getAsJsonArray();
    CSVParser csvParser = new CSVParser();
    String[] inputRecord = csvParser.parseLine(row10Cols);

    CsvToJsonConverterV2 converter = new CsvToJsonConverterV2();
    converter.init(new WorkUnitState());
    JsonObject actual = converter.createOutput(outputSchema, inputRecord);
    JsonObject expected = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/10_fields.json")))
                                .getAsJsonObject();

    Assert.assertEquals(expected, actual);
    converter.close();
  }

  public void convertOutputSkippingField() throws IOException, DataConversionException {
    JsonParser parser = new JsonParser();
    JsonElement jsonElement = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/schema_with_10_fields.json")));

    JsonArray outputSchema = jsonElement.getAsJsonArray();
    CSVParser csvParser = new CSVParser();
    String[] inputRecord = csvParser.parseLine(row11Cols);

    CsvToJsonConverterV2 converter = new CsvToJsonConverterV2();
    WorkUnitState wuState = new WorkUnitState();
    wuState.setProp(CsvToJsonConverterV2.CUSTOM_ORDERING, "0,1,3,4,5,6,7,8,9,10");
    converter.init(wuState);

    JsonObject actual = converter.convertRecord(outputSchema, inputRecord, wuState).iterator().next();
    JsonObject expected = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/10_fields.json")))
                                .getAsJsonObject();

    Assert.assertEquals(expected, actual);
    converter.close();
  }

  public void convertOutputMismatchFields() throws IOException {
    JsonParser parser = new JsonParser();
    JsonElement jsonElement = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/schema_with_10_fields.json")));

    JsonArray outputSchema = jsonElement.getAsJsonArray();
    CSVParser csvParser = new CSVParser();
    String[] inputRecord = csvParser.parseLine(row11Cols);

    CsvToJsonConverterV2 converter = new CsvToJsonConverterV2();
    try {
      converter.createOutput(outputSchema, inputRecord);
      Assert.fail();
    } catch (Exception e) {

    }
    converter.close();
  }

  public void convertOutputAddingNull() throws IOException, DataConversionException {
    JsonParser parser = new JsonParser();
    JsonElement jsonElement = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/schema_with_11_fields.json")));

    JsonArray outputSchema = jsonElement.getAsJsonArray();
    CSVParser csvParser = new CSVParser();
    String[] inputRecord = csvParser.parseLine(row11Cols);

    CsvToJsonConverterV2 converter = new CsvToJsonConverterV2();
    WorkUnitState wuState = new WorkUnitState();
    wuState.setProp(CsvToJsonConverterV2.CUSTOM_ORDERING, "0,1,-1,3,4,5,6,7,8,9,10");
    converter.init(wuState);

    JsonObject actual = converter.convertRecord(outputSchema, inputRecord, wuState).iterator().next();
    JsonObject expected = parser.parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/11_fields_with_null.json")))
                                .getAsJsonObject();
    Assert.assertEquals(expected, actual);
    converter.close();
  }
}
