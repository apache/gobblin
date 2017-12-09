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

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.ToAvroConverterBase;


/**
 * An extension to {@link ToAvroConverterBase} for integration test.
 *
 * @author Yinan Li
 */
public class TestConverter extends ToAvroConverterBase<String, String> {

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Override
  public Schema convertSchema(String schema, WorkUnitState workUnit) {
    return new Schema.Parser().parse(schema);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, String inputRecord, WorkUnitState workUnit) {

    JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }

    return new SingleRecordIterable<GenericRecord>(record);
  }
}
