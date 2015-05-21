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

package gobblin.converter.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.ToAvroConverterBase;
import gobblin.util.AvroUtils;
import gobblin.util.WriterUtils;


/**
 * Converts Integra's intermediate data format to avro
 *
 * @author kgoodhop
 *
 */
public class JsonIntermediateToAvroConverter extends ToAvroConverterBase<JsonArray, JsonObject> {
  private Map<String, JsonElementConversionFactory.JsonElementConverter> converters =
      new HashMap<String, JsonElementConversionFactory.JsonElementConverter>();
  private static final Logger LOG = LoggerFactory.getLogger(JsonIntermediateToAvroConverter.class);
  private long numFailedConversion = 0;

  @Override
  public Schema convertSchema(JsonArray schema, WorkUnitState workUnit) throws SchemaConversionException {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();

    for (JsonElement elem : schema) {
      JsonObject map = (JsonObject) elem;

      String columnName = map.get("columnName").getAsString();
      String comment = map.get("comment").getAsString();
      boolean nullable = map.has("isNullable") ? map.get("isNullable").getAsBoolean() : false;
      Schema fldSchema;

      try {
        JsonElementConversionFactory.JsonElementConverter converter =
            JsonElementConversionFactory.getConvertor(columnName, map.get("dataType").getAsJsonObject().get("type")
                .getAsString(), map, workUnit, nullable);
        converters.put(columnName, converter);
        fldSchema = converter.getSchema();
      } catch (UnsupportedDateTypeException e) {
        throw new SchemaConversionException(e);
      }

      Field fld = new Field(columnName, fldSchema, comment, nullable ? JsonNodeFactory.instance.nullNode() : null);
      fld.addProp("source.type", map.get("dataType").getAsJsonObject().get("type").getAsString());
      fields.add(fld);
    }

    Schema avroSchema =
        Schema.createRecord(workUnit.getExtract().getTable(), "", workUnit.getExtract().getNamespace(), false);
    avroSchema.setFields(fields);

    if (workUnit.getPropAsBoolean(ConfigurationKeys.CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED,
        ConfigurationKeys.DEFAULT_CONVERTER_AVRO_NULLIFY_FIELDS_ENABLED)) {
      return this.generateSchemaWithNullifiedField(workUnit, avroSchema);
    }

    return avroSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    GenericRecord avroRecord = new GenericData.Record(outputSchema);
    long maxFailedConversions =
        workUnit.getPropAsLong(ConfigurationKeys.CONVERTER_AVRO_MAX_CONVERSION_FAILURES,
            ConfigurationKeys.DEFAULT_CONVERTER_AVRO_MAX_CONVERSION_FAILURES);

    for (Map.Entry<String, JsonElement> entry : inputRecord.entrySet()) {
      try {
        avroRecord.put(entry.getKey(), converters.get(entry.getKey()).convert(entry.getValue()));
      } catch (Exception e) {
        numFailedConversion++;
        if (numFailedConversion < maxFailedConversions) {
          LOG.error("Dropping record " + inputRecord + " because it cannot be converted to Avro", e);
          return new EmptyIterable<GenericRecord>();
        } else {
          throw new DataConversionException("Unable to convert field:" + entry.getKey() + " for value:"
              + entry.getValue() + " for record: " + inputRecord, e);
        }
      }
    }

    return new SingleRecordIterable<GenericRecord>(avroRecord);
  }

  /**
   * Generate new avro schema by nullifying fields that previously existed but not in the current schema.
   *
   * @param workUnitState work unit state
   * @param currentAvroSchema current schema
   * @return merged schema with previous fields nullified.
   * @throws SchemaConversionException
   */
  protected Schema generateSchemaWithNullifiedField(WorkUnitState workUnitState, Schema currentAvroSchema)
      throws SchemaConversionException {
    Configuration conf = new Configuration();
    for (String key : workUnitState.getPropertyNames()) {
      conf.set(key, workUnitState.getProp(key));
    }
    // Get the original schema for merging.
    Path originalSchemaPath = null;
    if (workUnitState.contains(ConfigurationKeys.CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH)) {
      originalSchemaPath =
          new Path(workUnitState.getProp(ConfigurationKeys.CONVERTER_AVRO_NULLIFY_FIELDS_ORIGINAL_SCHEMA_PATH));
    } else {
      // If the path to get the original schema is not specified in the configuration,
      // adopt the best-try policy to search adjacent output folders.
      originalSchemaPath =
          WriterUtils.getDataPublisherFinalDir(workUnitState,
              workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1),
              workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY, 0)).getParent();
    }
    try {
      Schema prevSchema = AvroUtils.getDirectorySchema(originalSchemaPath, conf, false);
      Schema mergedSchema = AvroUtils.nullifyFiledsForSchemaMerge(prevSchema, currentAvroSchema);
      return mergedSchema;
    } catch (Exception e) {
      throw new SchemaConversionException("Unable to nullify fields in the new avro schema.");
    }
  }
}
