/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.example.wikipedia;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.ToAvroConverterBase;


/**
 * An implementation of {@link gobblin.converter.Converter} for the Wikipedia example.
 *
 *<p>
 *   This converter converts the input string schema into an Avro {@link org.apache.avro.Schema}
 *   and each input JSON document into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 *
 * @author ziliu
 */
public class WikipediaConverter extends ToAvroConverterBase<String, JsonElement>{

  private static final String JSON_CONTENT_MEMBER = "content";

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Override
  public Schema convertSchema(String schema, WorkUnitState workUnit) {
    return new Schema.Parser().parse(schema);
  }

  public Iterable<GenericRecord> convertRecord(Schema outputSchema, JsonElement inputRecord, WorkUnitState workUnit) {
    JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord record = new GenericData.Record(outputSchema);
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      if (entry.getKey().equals("*")) {
        //switch '*' to 'content' since '*' is not a valid avro schema field name
        record.put(JSON_CONTENT_MEMBER, entry.getValue());
      }
      else {
        record.put(entry.getKey(), entry.getValue());
      }
    }

    return new SingleRecordIterable<GenericRecord>(record);
  }
}
