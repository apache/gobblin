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
package org.apache.gobblin.converter.filter;

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
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.AvroToAvroConverterBase;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Flatten and filter the map field in GobblinTrackingEvent.
 */
public class GobblinTrackingEventFlattenFilterConverter extends AvroToAvroConverterBase {

  public static final String FIELDS_TO_FLATTEN = "fieldsToFlatten";
  public static final String NEW_SCHEMA_NAME = "outputSchemaName";
  public static final String FIELDS_RENAME_MAP = "fieldsRenameMap";
  private static final char OLD_NEW_NAME_SEPARATOR = ':';

  private Schema gobblinTrackingEventSchema;
  private Set<String> nonMapFields;
  private String mapFieldName;
  private List<Field> newFields;
  private Config config;
  private BiMap<String, String> fieldsRenameMap;

  @Override
  public Converter init(WorkUnitState workUnitState) {
    try {
      gobblinTrackingEventSchema =
          new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse GobblinTrackingEvent schema.", e);
    }
    config = ConfigUtils.propertiesToConfig(workUnitState.getProperties()).getConfig(this.getClass().getSimpleName());
    List<String> entryList = ConfigUtils.getStringList(config, FIELDS_RENAME_MAP);
    this.fieldsRenameMap = HashBiMap.create();
    for (String entry : entryList) {
      List<String> oldNewNames = Splitter.on(OLD_NEW_NAME_SEPARATOR).omitEmptyStrings().splitToList(entry);
      Preconditions.checkArgument(oldNewNames.size() == 2, "Wrong format for key " + FIELDS_RENAME_MAP);
      this.fieldsRenameMap.put(oldNewNames.get(0), oldNewNames.get(1));
    }

    this.nonMapFields = new HashSet<>();
    this.newFields = new ArrayList<>();
    List<String> mapFieldNames = new ArrayList<>();
    for (Field field : gobblinTrackingEventSchema.getFields()) {
      String curFieldName = field.name();
      if (!field.schema().getType().equals(Schema.Type.MAP)) {
        if (fieldsRenameMap.containsKey(curFieldName)) {
          newFields.add(
              new Schema.Field(fieldsRenameMap.get(curFieldName), field.schema(), field.doc(), field.defaultValue()));
        } else {
          newFields.add(new Schema.Field(curFieldName, field.schema(), field.doc(), field.defaultValue()));
        }
        this.nonMapFields.add(curFieldName);
      } else {
        mapFieldNames.add(curFieldName);
      }
    }

    Preconditions.checkArgument(mapFieldNames.size() == 1, "Input schema does not match GobblinTrackingEvent.");
    this.mapFieldName = mapFieldNames.get(0);

    for (String fieldToFlatten : ConfigUtils.getStringList(config, FIELDS_TO_FLATTEN)) {
      String newFieldName =
          this.fieldsRenameMap.containsKey(fieldToFlatten) ? this.fieldsRenameMap.get(fieldToFlatten) : fieldToFlatten;
      newFields.add(new Field(newFieldName, Schema.create(Schema.Type.STRING), "", null));
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

    BiMap<String, String> inversedViewOfFieldsRenameMap = this.fieldsRenameMap.inverse();
    for (Schema.Field field : outputSchema.getFields()) {
      String curFieldName = field.name();
      String originalFieldName =
          inversedViewOfFieldsRenameMap.containsKey(curFieldName) ? inversedViewOfFieldsRenameMap.get(curFieldName)
              : curFieldName;
      if (this.nonMapFields.contains(originalFieldName)) {
        genericRecord.put(curFieldName, inputRecord.get(originalFieldName));
      } else {
        genericRecord.put(curFieldName,
            AvroUtils.getFieldValue(inputRecord, Joiner.on('.').join(this.mapFieldName, originalFieldName)).or(""));
      }
    }
    return new SingleRecordIterable<>(genericRecord);
  }
}
