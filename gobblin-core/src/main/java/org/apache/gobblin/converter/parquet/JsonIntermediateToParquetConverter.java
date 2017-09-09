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
package org.apache.gobblin.converter.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.JsonElementConverter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;
import parquet.example.data.simple.SimpleGroup;
import parquet.schema.MessageType;
import parquet.schema.Type;


public class JsonIntermediateToParquetConverter extends Converter<JsonArray, MessageType, JsonObject, SimpleGroup> {
  private HashMap<String, JsonElementConverter> converters = new HashMap<>();

  @Override
  public MessageType convertSchema(JsonArray inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    List<Type> parquetTypes = new ArrayList<>();
    for (JsonElement element : inputSchema) {
      JsonObject map = (JsonObject) element;

      String columnName = map.get("columnName").getAsString();
      String dataType = map.get("dataType").getAsJsonObject().get("type").getAsString();
      boolean nullable = map.has("isNullable") && map.get("isNullable").getAsBoolean();
      Type schemaType;
      try {
        JsonElementConverter convertor =
            JsonElementConversionFactory.getConvertor(columnName, dataType, map, workUnit, nullable);
        schemaType = convertor.schema();
        this.converters.put(columnName, convertor);
      } catch (UnsupportedDateTypeException e) {
        throw new SchemaConversionException(e);
      }
      parquetTypes.add(schemaType);
    }
    String docName = workUnit.getExtract().getTable();
    return new MessageType(docName, parquetTypes);
  }

  @Override
  public Iterable<SimpleGroup> convertRecord(MessageType outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    SimpleGroup r1 = new SimpleGroup(outputSchema);
    for (Map.Entry<String, JsonElementConverter> entry : this.converters.entrySet()) {
      JsonElementConverter converter = entry.getValue();
      addBasedOnType(inputRecord, r1, entry, converter);
    }
    return new SingleRecordIterable<>(r1);
  }

  private void addBasedOnType(JsonObject inputRecord, SimpleGroup r1, Map.Entry<String, JsonElementConverter> entry,
      JsonElementConverter converter) {
    JsonElement value = inputRecord.get(entry.getKey());
    Object result = converter.convert(value);
    switch (converter.getSourceType()) {
      case INT:
        r1.add(entry.getKey(), ((IntegerValue) result).getInteger());
        break;
      case FLOAT:
        r1.add(entry.getKey(), ((FloatValue) result).getFloat());
        break;
      case DOUBLE:
        r1.add(entry.getKey(), ((DoubleValue) result).getDouble());
        break;
      case LONG:
        r1.add(entry.getKey(), ((LongValue) result).getLong());
        break;
      case BOOLEAN:
        r1.add(entry.getKey(), ((BooleanValue) result).getBoolean());
        break;
      case STRING:
        r1.add(entry.getKey(), ((BinaryValue) result).getString());
        break;
      default:
    }
  }
}
