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

package gobblin.converter.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;
import gobblin.util.ConfigUtils;


/**
 * Flatten and filter the map field in GobblinTrackingEvent.
 */
public class GobblinTrackingEventFlattenFilterConverter extends AvroToAvroConverterBase {

  public static final String FIELDS_TO_FLATTEN = "fieldsToFlatten";
  public static final String NEW_SCHEMA_NAME = "outputSchemaName";

  private Schema gobblinTrackingEventSchema;
  private Set<String> nonMapFields;
  private String mapFieldName;
  private List<Field> newFields;
  private Config config;

  @Override
  public Converter init(WorkUnitState workUnitState) {
    try {
      gobblinTrackingEventSchema =
          new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse GobblinTrackingEvent schema.", e);
    }
    config = ConfigUtils.propertiesToConfig(workUnitState.getProperties()).getConfig(this.getClass().getSimpleName());
    this.nonMapFields = new HashSet<>();
    this.newFields = new ArrayList<>();
    List<String> mapFieldNames = new ArrayList<>();
    for (Field field : gobblinTrackingEventSchema.getFields()) {
      if (!field.schema().getType().equals(Schema.Type.MAP)) {
        newFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
        this.nonMapFields.add(field.name());
      } else {
        mapFieldNames.add(field.name());
      }
    }

    Preconditions.checkArgument(mapFieldNames.size() == 1);
    this.mapFieldName = mapFieldNames.get(0);

    for (String fieldToFlatten : ConfigUtils.getStringList(config, FIELDS_TO_FLATTEN)) {
      newFields.add(new Field(fieldToFlatten, Schema.create(Schema.Type.STRING), "", null));
    }

    return this;
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    Preconditions.checkArgument(inputSchema.getFields().equals(gobblinTrackingEventSchema.getFields()));
    Schema outputSchema = Schema
        .createRecord(ConfigUtils.getString(config, NEW_SCHEMA_NAME, inputSchema.getName()), inputSchema.getDoc(),
            inputSchema.getNamespace(), inputSchema.isError());
    outputSchema.setFields(newFields);
    return outputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecord genericRecord = new GenericData.Record(outputSchema);

    for (Schema.Field field : outputSchema.getFields()) {
      if (this.nonMapFields.contains(field.name())) {
        genericRecord.put(field.name(), inputRecord.get(field.name()));
      } else {
        genericRecord.put(field.name(),
            AvroUtils.getFieldValue(inputRecord, Joiner.on('.').join(this.mapFieldName, field.name())).get());
      }
    }
    return new SingleRecordIterable<>(genericRecord);
  }
}
