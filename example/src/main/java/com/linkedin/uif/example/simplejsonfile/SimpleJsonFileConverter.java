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

package com.linkedin.uif.example.simplejsonfile;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.ToAvroConverterBase;


/**
 * A demo implementation of {@link Converter}.
 *
 * <p>
 *   This converter converts the input string schema into an Avro {@link org.apache.avro.Schema}
 *   and each input json document into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class SimpleJsonFileConverter extends ToAvroConverterBase<String, String> {

  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    return new Schema.Parser().parse(inputSchema);
  }

  @Override
  public GenericRecord convertRecord(Schema schema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    Gson gson = new Gson();
    JsonElement element = gson.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = gson.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }

    return record;
  }
}
