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

package org.apache.gobblin.couchbase.converter;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.couchbase.client.java.document.RawJsonDocument;
import com.google.gson.Gson;

import lombok.AllArgsConstructor;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;

import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AnyToCouchbaseJsonConverterTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testBasicConvertDefaultConfig()
      throws Exception {
    // default config
    testBasicConvert("key", false);
  }

  @Test
  public void testBasicConvertWithConfig()
      throws Exception {
    // with config
    testBasicConvert("foobar", true);


  }
  private void testBasicConvert(String keyField, boolean setConfig)
      throws Exception {

    String key = "hello";
    String testContent = "hello world";
    Map<String, String> content = new HashMap<>();
    content.put(keyField, key);
    content.put("value", testContent);

    AnyToCouchbaseJsonConverter recordConverter = new AnyToCouchbaseJsonConverter();

    WorkUnitState workUnitState = mock(WorkUnitState.class);
    if (setConfig) {
      when(workUnitState.getProp(AnyToCouchbaseJsonConverter.KEY_FIELD_CONFIG)).thenReturn(keyField);
      when(workUnitState.contains(AnyToCouchbaseJsonConverter.KEY_FIELD_CONFIG)).thenReturn(true);
      recordConverter.init(workUnitState);
    } else {
      recordConverter.init(workUnitState);
    }

    RawJsonDocument returnDoc = recordConverter.convertRecord("", content, null).iterator().next();
    System.out.println(returnDoc.toString());
    Assert.assertEquals(key.getBytes(), returnDoc.id().getBytes(), "key should be equal");


    Map<String, String> convertedMap = GSON.fromJson(returnDoc.content(), Map.class);
    Assert.assertEquals(key, convertedMap.get(keyField), "key in content should be equal");
    Assert.assertEquals(testContent, convertedMap.get("value"), "value in content should be equal");
    Assert.assertEquals(2, convertedMap.keySet().size(), "should have 2 fields");
  }

  @AllArgsConstructor
  class Record {
    int key;
    String value;
  };

  @Test
  public void testBasicConvertIntKey()
      throws Exception {

    int key = 5;
    String testContent = "hello world";
    Record record = new Record(key, testContent);
    Converter<String, String, Object, RawJsonDocument> recordConverter = new AnyToCouchbaseJsonConverter();

    RawJsonDocument returnDoc = recordConverter.convertRecord("", record, null).iterator().next();
    Assert.assertEquals(key+"", returnDoc.id(), "key should be equal");
    Record convertedRecord = GSON.fromJson(returnDoc.content(), Record.class);
    Assert.assertEquals(convertedRecord.key, key);
    Assert.assertEquals(convertedRecord.value, testContent, "value in content should be equal");
  }




  private void testFailure(AnyToCouchbaseJsonConverter recordConverter, Object obj)
  {
    try {
      recordConverter.convertRecord("", obj, null);
      Assert.fail("Expecting to throw an exception");
    } catch (DataConversionException dce) {
    } catch (Exception e) {
      Assert.fail("Expecting to throw only a DataConversionException", e);
    }

  }

  @Test
  public void testExpectedFailures()
      throws Exception {

    AnyToCouchbaseJsonConverter recordConverter = new AnyToCouchbaseJsonConverter();
    testFailure(recordConverter, new Integer(5));
    testFailure(recordConverter, new String("hello"));
    Map<String, Object> missingKey = new HashMap<>();
    missingKey.put("value", "value");
    testFailure(recordConverter, missingKey);
  }
}
