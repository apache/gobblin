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

package gobblin.converter.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.CaseFormat;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;
import gobblin.util.ConfigUtils;

import static gobblin.util.AvroUtils.FIELD_LOCATION_DELIMITER;


/**
 * Flatten a nested key and create a camel-cased name of a field which has the same value
 *
 * <p>
 *   Given configuration:
 *   <code>FlattenNestedKeyConverter.fieldsToFlatten = "address,address.city"</code>.
 *   A {@link FlattenNestedKeyConverter} will only process <code>"address.city"</code>. It makes
 *   a copy of the {@link Field} with a new name <code>"addressCity"</code> and adds it to the
 *   top level fields of the output schema. The value of field <code>"addressCity"</code> is equal
 *   to the one referred by <code>"address.city"</code>
 * </p>
 */
public class FlattenNestedKeyConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {
  public static final String FIELDS_TO_FLATTEN = "fieldsToFlatten";
  // A map from new field name to the nested key
  private Map<String, String> fieldNameMap = Maps.newHashMap();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    // Clear previous state
    fieldNameMap.clear();

    Config config = ConfigUtils.propertiesToConfig(workUnit.getProperties()).getConfig(getClass().getSimpleName());
    List<String> nestedKeys = ConfigUtils.getStringList(config, FIELDS_TO_FLATTEN);
    // No keys need flatten
    if (nestedKeys == null || nestedKeys.size() == 0) {
      return inputSchema;
    }

    List<Field> fields = new ArrayList<>();
    // Clone the existing fields
    for (Field field : inputSchema.getFields()) {
      fields.add(new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
    }

    // Convert each of nested keys into a top level field
    for (String key : nestedKeys) {
      if (!key.contains(FIELD_LOCATION_DELIMITER)) {
        continue;
      }

      String nestedKey = key.trim();
      // Create camel-cased name
      String hyphenizedKey = nestedKey.replace(FIELD_LOCATION_DELIMITER, "-");
      String name = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, hyphenizedKey);
      if (fieldNameMap.containsKey(name)) {
        // Duplicate
        continue;
      }
      fieldNameMap.put(name, nestedKey);

      // Find the field
      Optional<Field> optional = AvroUtils.getField(inputSchema, nestedKey);
      if (!optional.isPresent()) {
        throw new SchemaConversionException("Unable to get field with location: " + nestedKey);
      }
      Field field = optional.get();

      // Make a copy under a new name
      Field copy = new Field(name, field.schema(), field.doc(), field.defaultValue(), field.order());
      fields.add(copy);
    }

    Schema outputSchema = Schema
        .createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError());
    outputSchema.setFields(fields);
    return outputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    // No fields need flatten
    if (fieldNameMap.size() == 0) {
      return new SingleRecordIterable<>(inputRecord);
    }

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    for (Field field : outputSchema.getFields()) {
      String fieldName = field.name();
      if (fieldNameMap.containsKey(fieldName)) {
        // Skip new field for now
        continue;
      }

      outputRecord.put(fieldName, inputRecord.get(fieldName));
    }

    // Deal with new fields
    for (Map.Entry<String, String> entry : fieldNameMap.entrySet()) {
      Optional<Object> optional = AvroUtils.getFieldValue(inputRecord, entry.getValue());
      if (!optional.isPresent()) {
        throw new DataConversionException("Unable to get field value with location: " + entry.getValue());
      }
      outputRecord.put(entry.getKey(), optional.get());
    }

    return new SingleRecordIterable<>(outputRecord);
  }
}
