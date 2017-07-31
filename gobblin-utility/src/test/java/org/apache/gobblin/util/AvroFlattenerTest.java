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

import java.io.IOException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroFlattenerTest {

  private static Schema readSchemaFromJsonFile(String filename)
      throws IOException {
    return new Schema.Parser()
        .parse(AvroFlattenerTest.class.getClassLoader().getResourceAsStream("flattenAvro/" + filename));
  }

  /**
   * Test flattening for Record within another Record
   * Record R1 {
   *  fields: { Record R2 }
   * }
   */
  @Test
  public void testRecordWithinRecord() throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for Record within Record within another Record
   * Record R1 {
   *  fields:
   *    { Record R2
   *       fields:
   *         {
   *            Record R3
   *         }
   *    }
   * }
   */
  @Test
  public void testRecordWithinRecordWithinRecord() throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinRecordWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinRecordWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for Record within an Option within another Record
   * Record R1 {
   *  fields: { Union [ null, Record R2 ] }
   * }
   */
  @Test
  public void testRecordWithinOptionWithinRecord () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinOptionWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinOptionWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for Record within an Union within another Record
   * Record R1 {
   *  fields: { Union [ Record R2, null ] }
   * }
   */
  @Test
  public void testRecordWithinUnionWithinRecord () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinUnionWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinUnionWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for Option within an Option within another Record
   * Record R1 {
   *  fields: {
   *    Union [ null,
   *            Record 2 {
   *              fields: { Union [ null, Record 3] }
   *            }
   *          ]
   *    }
   * }
   */
  @Test
  public void testOptionWithinOptionWithinRecord () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("optionWithinOptionWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("optionWithinOptionWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for a Record within Array within Array
   * (no flattening should happen)
   * Array A1 {
   *   [
   *     Array A2 {
   *       [
   *          Record R1
   *       ]
   *     }
   *   ]
   * }
   */
  @Test
  public void testRecordWithinArrayWithinArray () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinArrayWithinArray_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinArrayWithinArray_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for an Array within Record within Array within Record
   * (no flattening should happen)
   * Record R1 {
   *   fields: { [
   *     Array A1 {
   *       [
   *         Record R2 {
   *           fields: { [
   *             Array A2
   *           ] }
   *         }
   *       ]
   *     }
   *   ] }
   * }
   */
  @Test
  public void testArrayWithinRecordWithinArrayWithinRecord () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("arrayWithinRecordWithinArrayWithinRecord_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("arrayWithinRecordWithinArrayWithinRecord_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }

  /**
   * Test flattening for a Record within Map within Map
   * (no flattening should happen)
   * Map M1 {
   *   values: {
   *     Map M2 {
   *       values: {
   *          Record R1
   *       }
   *     }
   *   }
   * }
   */
  @Test
  public void testRecordWithinMapWithinMap () throws IOException {

    Schema originalSchema = readSchemaFromJsonFile("recordWithinMapWithinMap_original.json");
    Schema expectedSchema = readSchemaFromJsonFile("recordWithinMapWithinMap_flattened.json");

    Assert.assertEquals(new AvroFlattener().flatten(originalSchema, false), expectedSchema);
  }


}
