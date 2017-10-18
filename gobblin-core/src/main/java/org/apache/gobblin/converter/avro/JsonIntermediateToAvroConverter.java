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

package org.apache.gobblin.converter.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.ToAvroConverterBase;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory.RecordConverter;
import org.apache.gobblin.converter.json.JsonSchema;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


/**
 * Converts Integra's intermediate data format to avro
 *
 * @author kgoodhop
 *
 */
public class JsonIntermediateToAvroConverter extends ToAvroConverterBase<JsonArray, JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonIntermediateToAvroConverter.class);
  private static final String CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED = "converter.avro.nullify.fields.enabled";
  private static final boolean DEFAULT_CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED = Boolean.FALSE;
  private static final String CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH =
      "converter.avro.nullify.fields.original.schema.path";

  private RecordConverter recordConverter;

  @Override
  public Schema convertSchema(JsonArray schema, WorkUnitState workUnit)
      throws SchemaConversionException {
    try {
      JsonSchema jsonSchema = new JsonSchema(schema);
      jsonSchema.setColumnName(workUnit.getExtract().getTable());
      recordConverter = new RecordConverter(jsonSchema, workUnit, workUnit.getExtract().getNamespace());
    } catch (UnsupportedDateTypeException e) {
      throw new SchemaConversionException(e);
    }
    Schema recordSchema = recordConverter.schema();
    if (workUnit
        .getPropAsBoolean(CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED, DEFAULT_CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED)) {
      return this.generateSchemaWithNullifiedField(workUnit, recordSchema);
    }
    return recordSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    return new SingleRecordIterable<>((GenericRecord) recordConverter.convert(inputRecord));
  }

  /**
   * Generate new avro schema by nullifying fields that previously existed but not in the current schema.
   *
   * @param workUnitState work unit state
   * @param currentAvroSchema current schema
   * @return merged schema with previous fields nullified.
   * @throws SchemaConversionException
   */
  protected Schema generateSchemaWithNullifiedField(WorkUnitState workUnitState, Schema currentAvroSchema) {
    Configuration conf = new Configuration();
    for (String key : workUnitState.getPropertyNames()) {
      conf.set(key, workUnitState.getProp(key));
    }
    // Get the original schema for merging.
    Path originalSchemaPath = null;
    if (workUnitState.contains(CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH)) {
      originalSchemaPath = new Path(workUnitState.getProp(CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH));
    } else {
      // If the path to get the original schema is not specified in the configuration,
      // adopt the best-try policy to search adjacent output folders.
      LOG.info("Property " + CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH
          + "is not specified. Trying to get the orignal schema from previous avro files.");
      originalSchemaPath = WriterUtils
          .getDataPublisherFinalDir(workUnitState, workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1),
              workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY, 0)).getParent();
    }
    try {
      Schema prevSchema = AvroUtils.getDirectorySchema(originalSchemaPath, conf, false);
      Schema mergedSchema = AvroUtils.nullifyFieldsForSchemaMerge(prevSchema, currentAvroSchema);
      return mergedSchema;
    } catch (IOException ioe) {
      LOG.error("Unable to nullify fields. Will retain the current avro schema.", ioe);
      return currentAvroSchema;
    }
  }
}
