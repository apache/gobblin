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
package gobblin.recordaccess;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.testng.annotations.Test;


public class RecordAccessorProviderFactoryTest {

  @Test
  public void testWithAvroRecord()
      throws IOException {
    Schema recordSchema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("converter/fieldPickInput.avsc"));

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("name", "foo");

    RecordAccessor accessor = RecordAccessorProviderFactory.getRecordAccessorForObject(record);

    Assert.assertNotNull(accessor);
    Assert.assertEquals(accessor.getAsString("name"), "foo");
  }

  @Test
  public void testFactoryRegistration() {
    // TestAccessorBuilder should be invoked
    RandomObject1 obj = new RandomObject1("foo");
    RecordAccessor accessor = RecordAccessorProviderFactory.getRecordAccessorForObject(obj);

    Assert.assertNotNull(accessor);
    Assert.assertTrue(accessor instanceof TestAccessor);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUnregisteredObject()
      throws IOException {
    RandomObject2 obj = new RandomObject2("foo");
    RecordAccessor accessor = RecordAccessorProviderFactory.getRecordAccessorForObject(obj);
  }

  private static class RandomObject1 {
    String name;

    public RandomObject1(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static class RandomObject2 {
    String name;

    public RandomObject2(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  // Dummy accessor - we just need to make sure it is constructed
  public static class TestAccessor implements RecordAccessor {
    @Override
    public String getAsString(String fieldName) {
      return null;
    }

    @Override
    public Integer getAsInt(String fieldName) {
      return null;
    }

    @Override
    public Long getAsLong(String fieldName) {
      return null;
    }

    @Override
    public void set(String fieldName, String value) {

    }

    @Override
    public void set(String fieldName, Integer value) {

    }

    @Override
    public void set(String fieldName, Long value) {

    }

    @Override
    public void setToNull(String fieldName) {

    }

    @Override
    public Map<String, String> getMultiAsString(String fieldName) {
      return null;
    }

    @Override
    public Map<String, Integer> getMultiAsInt(String fieldName) {
      return null;
    }

    @Override
    public Map<String, Long> getMultiAsLong(String fieldName) {
      return null;
    }
  }

  public static class TestAccessorBuilder implements RecordAccessorProvider {
    @Override
    public RecordAccessor recordAccessorForObject(Object obj) {
      if (obj instanceof RandomObject1) {
        return new TestAccessor();
      }

      return null;
    }
  }
}
