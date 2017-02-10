/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.converter;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.test.TestUtils;
import gobblin.type.RecordWithMetadata;


@Test
public class AnyToRecordWithMetadataConverterTest {

  @Test
  public void testFailures()
      throws DataConversionException {
    AnyToRecordWithMetadataConverter converter = new AnyToRecordWithMetadataConverter();
    Object randomObject = new Object();

    try {
      Iterator<RecordWithMetadata> recordWithMetadataIterator = converter.convertRecord("", randomObject, null).iterator();
      Assert.fail("Should have thrown an exception");
    } catch (DataConversionException e) {
    } catch (Exception e) {
      Assert.fail("Should only throw DataConversionException");
    }

    randomObject = null;
    try {
      Iterator<RecordWithMetadata> recordWithMetadataIterator = converter.convertRecord("", randomObject, null).iterator();
      Assert.fail("Should have thrown an exception");
    } catch (DataConversionException e) {
    } catch (Exception e) {
      Assert.fail("Should only throw DataConversionException", e);
    }


  }

  @Test
  public void testSuccessWithJson() {

    HashMap<String, String> map = new HashMap<>();
    map.put("test", "test");
    map.put("value", "value");

    Gson gson = new Gson();

    JsonElement jsonElement = gson.toJsonTree(map);
    AnyToRecordWithMetadataConverter converter = new AnyToRecordWithMetadataConverter();


    try {
      Iterator<RecordWithMetadata> recordWithMetadataIterator = converter.convertRecord("", jsonElement, null).iterator();
      RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();
      Assert.assertEquals(recordWithMetadata.getRecord(), jsonElement);
    } catch (Exception e) {
      Assert.fail("Should not have thrown an exception");
    }


  }

  @Test
  public void testSuccessWithAvro() {

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    AnyToRecordWithMetadataConverter converter = new AnyToRecordWithMetadataConverter();


    try {
      Iterator<RecordWithMetadata> recordWithMetadataIterator = converter.convertRecord("", record, null).iterator();
      RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();
      // A Json Element has been created. Not testing if it is what we expect it to be
      Assert.assertTrue(recordWithMetadata.getRecord() instanceof JsonElement);

    } catch (Exception e) {
      Assert.fail("Should not have thrown an exception");
    }


  }



}
