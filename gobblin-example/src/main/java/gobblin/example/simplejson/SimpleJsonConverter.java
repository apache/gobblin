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

package gobblin.example.simplejson;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.ToAvroConverterBase;


/**
 * An implementation of {@link Converter} for the simple JSON example.
 *
 * <p>
 *   This converter converts the input string schema into an Avro {@link org.apache.avro.Schema}
 *   and each input json document into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class SimpleJsonConverter extends ToAvroConverterBase<String, String> {

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    return new Schema.Parser().parse(inputSchema);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }

    return new SingleRecordIterable<GenericRecord>(record);
  }
}
