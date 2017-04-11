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

package gobblin.converter.filter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;

import org.apache.avro.Schema;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Test;

@Test(groups = { "gobblin.converter.filter" })
public class AvroFieldsPickConverterTest {

  @Test
  public void testFieldsPick() throws Exception {

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_color");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickExpected.avsc"));

      JSONAssert.assertEquals(expected.toString(), converted.toString(), false);
    }
  }

  @Test (expectedExceptions=SchemaConversionException.class)
  public void testFieldsPickWrongFieldFailure() throws Exception {

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS, "name,favorite_number,favorite_food");

    try (AvroFieldsPickConverter converter = new AvroFieldsPickConverter()) {
      Schema converted = converter.convertSchema(inputSchema, workUnitState);
      Schema expected = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickExpected.avsc"));

      JSONAssert.assertEquals(expected.toString(), converted.toString(), false);
    }
  }
}