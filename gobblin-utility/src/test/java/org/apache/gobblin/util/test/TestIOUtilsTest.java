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

package org.apache.gobblin.util.test;

import java.util.Collection;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for {@link TestIOUtils}
 */
public class TestIOUtilsTest {

  @Test
  public void testReadAllRecords()
      throws Exception {

    List<GenericRecord> testData = TestIOUtils.readAllRecords(
        getClass().getResource("/test_data.json").getPath(),
        getClass().getResource("/test_data.avsc").getPath());

    Assert.assertEquals(testData.size(), 2);
    Assert.assertEquals(find(testData, "string1", "string1").toString(),
      "{\"string1\": \"string1\", \"long1\": 1234, \"double1\": 1234.12}");
    Assert.assertEquals(find(testData, "string1", "string2").toString(),
        "{\"string1\": \"string2\", \"long1\": 4567, \"double1\": 4567.89}");
  }

  private static GenericRecord find(Collection<GenericRecord> records, String field, String value) {

    for (GenericRecord record : records) {
      if (null == record.getSchema().getField(field)) {
        continue;
      }

      if (null != record.get(field) && record.get(field).toString().equals(value)) {
        return record;
      }
    }

    return null;
  }
}
