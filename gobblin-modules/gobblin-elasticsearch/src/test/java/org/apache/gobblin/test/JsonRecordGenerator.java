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
package org.apache.gobblin.test;

import java.util.Collections;

import org.apache.gobblin.elasticsearch.typemapping.JsonTypeMapper;

import com.google.gson.Gson;
import com.google.gson.JsonElement;


/**
 * A generator of {@link JsonElement} records
 */
public class JsonRecordGenerator implements RecordTypeGenerator<JsonElement> {
  private final Gson gson = new Gson();

  @Override
  public String getName() {
    return "json";
  }

  @Override
  public String getTypeMapperClassName() {
    return JsonTypeMapper.class.getCanonicalName();
  }

  static class TestObject<T> {
    private String id;
    private T key;

    TestObject(String id, T payload) {
      this.id = id;
      this.key = payload;
    }
  }

  @Override
  public JsonElement getRecord(String id, PayloadType payloadType) {
    Object testObject;
    switch (payloadType) {
      case STRING: {
        testObject = new TestObject(id, TestUtils.generateRandomAlphaString(20));
        break;
      }
      case LONG: {
        testObject = new TestObject(id, TestUtils.generateRandomLong());
        break;
      }
      case MAP: {
        testObject = new TestObject(id, Collections.EMPTY_MAP);
        break;
      }
      default:
        throw new RuntimeException("Do not know how to handle this type of payload");
    }
    JsonElement jsonElement = gson.toJsonTree(testObject);
    return jsonElement;
  }
}
