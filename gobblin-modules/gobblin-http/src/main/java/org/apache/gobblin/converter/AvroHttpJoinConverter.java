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
package org.apache.gobblin.converter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.http.HttpOperation;
import org.apache.gobblin.http.HttpRequestResponseRecord;
import org.apache.gobblin.http.ResponseStatus;
import org.apache.gobblin.utils.HttpUtils;


/**
 * A type of {@link HttpJoinConverter} with AVRO as input and output format
 *
 * Input:
 *    User provided record
 *
 * Output:
 *    User provided record plus http request & response record
 */
@Slf4j
public abstract class AvroHttpJoinConverter<RQ, RP> extends AsyncHttpJoinConverter<Schema, Schema, GenericRecord, GenericRecord, RQ, RP> {
  public static final String HTTP_REQUEST_RESPONSE_FIELD = "HttpRequestResponse";

  @Override
  public Schema convertSchemaImpl(Schema inputSchema, WorkUnitState workUnitState)
      throws SchemaConversionException {

    if (inputSchema == null) {
      throw new SchemaConversionException("input schema is empty");
    }

    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : inputSchema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order());
      fields.add(newField);
    }

    Schema.Field requestResponseField = new Schema.Field(HTTP_REQUEST_RESPONSE_FIELD, HttpRequestResponseRecord.getClassSchema(), "http output schema contains request url and return result", null);
    fields.add(requestResponseField);

    Schema combinedSchema = Schema.createRecord(inputSchema.getName(), inputSchema.getDoc() + " (Http request and response are contained)", inputSchema.getNamespace(), false);
    combinedSchema.setFields(fields);
    return combinedSchema;
  }

  /**
   * Extract user defined keys by looking at "gobblin.converter.http.keys"
   * If keys are defined, extract key-value pair from inputRecord and set it to HttpOperation
   * If keys are not defined, generate HttpOperation by HttpUtils.toHttpOperation
   */
  @Override
  protected HttpOperation generateHttpOperation (GenericRecord inputRecord, State state) {
    Map<String, String> keyAndValue = new HashMap<>();
    Optional<Iterable<String>> keys = getKeys(state);
    HttpOperation operation;

    if (keys.isPresent()) {
      for (String key : keys.get()) {
        String value = inputRecord.get(key).toString();
        log.debug("Http join converter: key is {}, value is {}", key, value);
        keyAndValue.put(key, value);
      }
      operation = new HttpOperation();
      operation.setKeys(keyAndValue);
    } else {
      operation = HttpUtils.toHttpOperation(inputRecord);
    }
    return operation;
  }

  private Optional<Iterable<String>> getKeys (State state) {
    if (!state.contains(CONF_PREFIX + "keys")) {
      return Optional.empty();
    }
    Iterable<String> keys = state.getPropAsList(CONF_PREFIX + "keys");
    return Optional.ofNullable(keys);
  }

  @Override
  public final GenericRecord convertRecordImpl(Schema outputSchema, GenericRecord inputRecord, RQ rawRequest, ResponseStatus status) throws DataConversionException {

    if (outputSchema == null) {
      throw new DataConversionException("output schema is empty");
    }

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    Schema httpOutputSchema = null;
    for (Schema.Field field : outputSchema.getFields()) {
      if (!field.name().equals(HTTP_REQUEST_RESPONSE_FIELD)) {
        log.debug ("Copy {}", field.name());
        Object inputValue = inputRecord.get(field.name());
        outputRecord.put(field.name(), inputValue);
      } else {
        httpOutputSchema = field.schema();
      }
    }

    try {
      fillHttpOutputData (httpOutputSchema, outputRecord, rawRequest, status);
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
    return outputRecord;
  }

  protected abstract void fillHttpOutputData (Schema httpOutputSchema, GenericRecord outputRecord, RQ rawRequest,
      ResponseStatus status) throws IOException;
}
