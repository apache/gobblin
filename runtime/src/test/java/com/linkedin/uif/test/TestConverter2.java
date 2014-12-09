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

package com.linkedin.uif.test;

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
import com.linkedin.uif.fork.CopyNotSupportedException;
import com.linkedin.uif.fork.CopyableGenericRecord;
import com.linkedin.uif.fork.CopyableSchema;


/**
 * An implementation of {@link Converter} for tests related to the
 * {@link com.linkedin.uif.fork.ForkOperator}.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class TestConverter2 extends Converter<String, CopyableSchema, String, CopyableGenericRecord> {

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Override
  public CopyableSchema convertSchema(String schema, WorkUnitState workUnit) {
    return new CopyableSchema(new Schema.Parser().parse(schema));
  }

  @Override
  public CopyableGenericRecord convertRecord(CopyableSchema schema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
    try {
      Schema avroSchema = schema.copy();
      GenericRecord record = new GenericData.Record(avroSchema);
      for (Map.Entry<String, Object> entry : fields.entrySet()) {
        if (entry.getValue() instanceof Double) {
          // Gson reads the integers in the input Json documents as doubles, so we have
          // to convert doubles to integers here as the Avro schema specifies integers.
          record.put(entry.getKey(), ((Double) entry.getValue()).intValue());
        } else {
          record.put(entry.getKey(), entry.getValue());
        }
      }

      return new CopyableGenericRecord(record);
    } catch (CopyNotSupportedException cnse) {
      throw new DataConversionException(cnse);
    }
  }
}

