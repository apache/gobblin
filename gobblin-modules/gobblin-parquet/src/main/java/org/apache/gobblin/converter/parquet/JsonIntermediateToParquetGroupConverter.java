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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.parquet.JsonElementConversionFactory.RecordConverter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import parquet.example.data.Group;
import parquet.schema.MessageType;

import static org.apache.gobblin.converter.parquet.JsonElementConversionFactory.RecordConverter.RecordType.ROOT;


/**
 * A converter to Convert JsonIntermediate to Parquet
 * @author tilakpatidar
 */
public class JsonIntermediateToParquetGroupConverter extends Converter<JsonArray, MessageType, JsonObject, Group> {
  private RecordConverter recordConverter;

  @Override
  public MessageType convertSchema(JsonArray inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    String fieldName = workUnit.getExtract().getTable();
    JsonSchema jsonSchema = new JsonSchema(inputSchema);
    jsonSchema.setColumnName(fieldName);
    recordConverter = new RecordConverter(jsonSchema, ROOT);
    return (MessageType) recordConverter.schema();
  }

  @Override
  public Iterable<Group> convertRecord(MessageType outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<>((Group) recordConverter.convert(inputRecord));
  }
}
