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

package org.apache.gobblin.converter.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.initializer.AvroToJdbcEntryConverterInitializer;
import org.apache.gobblin.converter.initializer.ConverterInitializer;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.writer.commands.JdbcWriterCommandsFactory;


/**
 * Converts Avro Schema into JdbcEntrySchema
 * Converts Avro GenericRecord into JdbcEntryData
 * Converts Avro field name for JDBC counterpart.
 *
 * This converter is written based on Avro 1.7.7 specification https://avro.apache.org/docs/1.7.7/spec.html
 */
public class AvroToJdbcEntryConverter extends Converter<Schema, JdbcEntrySchema, GenericRecord, JdbcEntryData> {

  public static final String CONVERTER_AVRO_JDBC_DATE_FIELDS = "converter.avro.jdbc.date_fields";
  private static final String AVRO_NESTED_COLUMN_DELIMITER = ".";
  private static final String JDBC_FLATTENED_COLUMN_DELIMITER = "_";
  private static final String AVRO_NESTED_COLUMN_DELIMITER_REGEX_COMPATIBLE = "\\.";
  private static final Splitter AVRO_RECORD_LEVEL_SPLITTER = Splitter.on(AVRO_NESTED_COLUMN_DELIMITER).omitEmptyStrings();


  private static final Logger LOG = LoggerFactory.getLogger(AvroToJdbcEntryConverter.class);
  private static final Map<Type, JdbcType> AVRO_TYPE_JDBC_TYPE_MAPPING =
      ImmutableMap.<Type, JdbcType> builder()
        .put(Type.BOOLEAN, JdbcType.BOOLEAN)
        .put(Type.INT, JdbcType.INTEGER)
        .put(Type.LONG, JdbcType.BIGINT)
        .put(Type.FLOAT, JdbcType.FLOAT)
        .put(Type.DOUBLE, JdbcType.DOUBLE)
        .put(Type.STRING, JdbcType.VARCHAR)
        .put(Type.ENUM, JdbcType.VARCHAR).build();
  private static final Set<Type> AVRO_SUPPORTED_TYPES =
      ImmutableSet.<Type> builder()
        .addAll(AVRO_TYPE_JDBC_TYPE_MAPPING.keySet())
        .add(Type.UNION)
        .add(Type.RECORD)
        .build();
  private static final Set<JdbcType> JDBC_SUPPORTED_TYPES =
      ImmutableSet.<JdbcType> builder()
        .addAll(AVRO_TYPE_JDBC_TYPE_MAPPING.values())
        .add(JdbcType.DATE)
        .add(JdbcType.TIME)
        .add(JdbcType.TIMESTAMP)
        .build();

  private Optional<Map<String, String>> avroToJdbcColPairs = Optional.absent();
  private Map<String, String> jdbcToAvroColPairs = new HashMap<>();

  public AvroToJdbcEntryConverter() {
    super();
  }

  @VisibleForTesting
  public AvroToJdbcEntryConverter(WorkUnitState workUnit) {
    init(workUnit);
  }

  /**
   * Fetches JdbcWriterCommands.
   * Builds field name mapping between Avro and JDBC.
   * {@inheritDoc}
   * @see org.apache.gobblin.converter.Converter#init(org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Converter<Schema, JdbcEntrySchema, GenericRecord, JdbcEntryData> init(WorkUnitState workUnit) {
    String avroToJdbcFieldsPairJsonStr = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_JDBC_ENTRY_FIELDS_PAIRS);
    if (!StringUtils.isEmpty(avroToJdbcFieldsPairJsonStr)) {
      if (!this.avroToJdbcColPairs.isPresent()) {
        ImmutableMap.Builder<String, String> avroToJdbcBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> jdbcToAvroBuilder = ImmutableMap.builder();

        JsonObject json = new JsonParser().parse(avroToJdbcFieldsPairJsonStr).getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
          if (!entry.getValue().isJsonPrimitive()) {
            throw new IllegalArgumentException("Json value should be a primitive String. "
                + ConfigurationKeys.CONVERTER_AVRO_JDBC_ENTRY_FIELDS_PAIRS + " : " + avroToJdbcFieldsPairJsonStr);
          }
          avroToJdbcBuilder.put(entry.getKey(), entry.getValue().getAsString());
          jdbcToAvroBuilder.put(entry.getValue().getAsString(), entry.getKey());
        }
        this.avroToJdbcColPairs = Optional.of((Map<String, String>) avroToJdbcBuilder.build());
      }
    }
    return this;
  }

  /**
   * Converts Avro schema to JdbcEntrySchema.
   *
   * Few precondition to the Avro schema
   * 1. Avro schema should have one entry type record at first depth.
   * 2. Avro schema can recurse by having record inside record.
   * 3. Supported Avro primitive types and conversion
   *  boolean --> java.lang.Boolean
   *  int --> java.lang.Integer
   *  long --> java.lang.Long or java.sql.Date , java.sql.Time , java.sql.Timestamp
   *  float --> java.lang.Float
   *  double --> java.lang.Double
   *  bytes --> byte[]
   *  string --> java.lang.String
   *  null: only allowed if it's within union (see complex types for more details)
   * 4. Supported Avro complex types
   *  Records: Supports nested record type as well.
   *  Enum --> java.lang.String
   *  Unions --> Only allowed if it have one primitive type in it, along with Record type, or null type with one primitive type where null will be ignored.
   *  Once Union is narrowed down to one primitive type, it will follow conversion of primitive type above.
   * {@inheritDoc}
   *
   * 5. In order to make conversion from Avro long type to java.sql.Date or java.sql.Time or java.sql.Timestamp,
   * converter will get table metadata from JDBC.
   * 6. As it needs JDBC connection from condition 5, it also assumes that it will use JDBC publisher where it will get connection information from.
   * 7. Conversion assumes that both schema, Avro and JDBC, uses same column name where name space in Avro is ignored.
   *    For case sensitivity, Avro is case sensitive where it differs in JDBC based on underlying database. As Avro is case sensitive, column name equality also take case sensitive in to account.
   *
   * @see org.apache.gobblin.converter.Converter#convertSchema(java.lang.Object, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public JdbcEntrySchema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    LOG.info("Converting schema " + inputSchema);
    Preconditions.checkArgument(Type.RECORD.equals(inputSchema.getType()),
        "%s is expected for the first level element in Avro schema %s",
        Type.RECORD, inputSchema);

    Map<String, Type> avroColumnType = flatten(inputSchema);
    String jsonStr = Preconditions.checkNotNull(workUnit.getProp(CONVERTER_AVRO_JDBC_DATE_FIELDS));
    java.lang.reflect.Type typeOfMap = new TypeToken<Map<String, JdbcType>>() {}.getType();
    Map<String, JdbcType> dateColumnMapping = new Gson().fromJson(jsonStr, typeOfMap);
    LOG.info("Date column mapping: " + dateColumnMapping);

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = Lists.newArrayList();
    for (Map.Entry<String, Type> avroEntry : avroColumnType.entrySet()) {
      String colName = tryConvertAvroColNameToJdbcColName(avroEntry.getKey());

      JdbcType JdbcType = dateColumnMapping.get(colName);
      if (JdbcType == null) {
        JdbcType = AVRO_TYPE_JDBC_TYPE_MAPPING.get(avroEntry.getValue());
      }
      Preconditions.checkNotNull(JdbcType, "Failed to convert " + avroEntry + " AVRO_TYPE_JDBC_TYPE_MAPPING: "
          + AVRO_TYPE_JDBC_TYPE_MAPPING + " , dateColumnMapping: " + dateColumnMapping);
      jdbcEntryMetaData.add(new JdbcEntryMetaDatum(colName, JdbcType));
    }

    JdbcEntrySchema converted = new JdbcEntrySchema(jdbcEntryMetaData);
    LOG.info("Converted schema into " + converted);
    return converted;
  }

  /**
   * Convert Avro column name to JDBC column name. If name mapping is defined, follow it. Otherwise, just return avro column name,
   * while replacing nested column delimiter, dot, to underscore.
   * This method also updates, mapping from JDBC column name to Avro column name for reverse look up.
   * @param avroColName
   * @return
   */
  private String tryConvertAvroColNameToJdbcColName(String avroColName) {
    if (!avroToJdbcColPairs.isPresent()) {
      String converted = avroColName.replaceAll(AVRO_NESTED_COLUMN_DELIMITER_REGEX_COMPATIBLE, JDBC_FLATTENED_COLUMN_DELIMITER);
      jdbcToAvroColPairs.put(converted, avroColName);
      return converted;
    }

    String converted = avroToJdbcColPairs.get().get(avroColName);
    converted = converted != null ? converted : avroColName;
    jdbcToAvroColPairs.put(converted, avroColName);
    return converted;
  }

  /**
   * Provides JDBC column name based on Avro column name. It's a one liner method but contains knowledge on where the mapping is.
   * @param colName
   * @return
   */
  private String convertJdbcColNameToAvroColName(String colName) {
    return Preconditions.checkNotNull(jdbcToAvroColPairs.get(colName));
  }


  /**
   * Flattens Avro's (possibly recursive) structure and provides field name and type.
   * It assumes that the leaf level field name has unique name.
   * @param schema
   * @return
   * @throws SchemaConversionException if there's duplicate name in leaf level of Avro Schema
   */
  private static Map<String, Type> flatten(Schema schema) throws SchemaConversionException {
    Map<String, Type> flattened = new LinkedHashMap<>();
    Schema recordSchema = determineType(schema);

    Preconditions.checkArgument(Type.RECORD.equals(recordSchema.getType()), "%s is expected. Schema: %s",
        Type.RECORD, recordSchema);

    for (Field f : recordSchema.getFields()) {
      produceFlattenedHelper(f, flattened);
    }
    return flattened;
  }

  private static void produceFlattenedHelper(Field field, Map<String, Type> flattened)
      throws SchemaConversionException {
    Schema actualSchema = determineType(field.schema());
    if (Type.RECORD.equals(actualSchema.getType())) {
      Map<String, Type> map = flatten(actualSchema);
      for (Entry<String, Type> entry : map.entrySet()) {
        String key = String.format("%s" + AVRO_NESTED_COLUMN_DELIMITER + "%s", field.name(), entry.getKey());
        Type existing = flattened.put(key, entry.getValue());
        Preconditions.checkArgument(existing == null, "Duplicate name detected in Avro schema. Field: " + key);
      }
      return;
    }

    Type existing = flattened.put(field.name(), actualSchema.getType());
    if (existing != null) {
      //No duplicate name allowed when flattening (not considering name space we don't have any assumption between namespace and actual database field name)
      throw new SchemaConversionException("Duplicate name detected in Avro schema. " + field.name());
    }
  }

  private static Schema determineType(Schema schema) throws SchemaConversionException {
    if (!AVRO_SUPPORTED_TYPES.contains(schema.getType())) {
      throw new SchemaConversionException(schema.getType() + " is not supported");
    }

    if (!Type.UNION.equals(schema.getType())) {
      return schema;
    }

    //For UNION, only supported avro type with NULL is allowed.
    List<Schema> schemas = schema.getTypes();
    if (schemas.size() > 2) {
      throw new SchemaConversionException("More than two types are not supported " + schemas);
    }

    for (Schema s : schemas) {
      if (Type.NULL.equals(s.getType())) {
        continue;
      }
      return s;
    }

    throw new SchemaConversionException("Cannot determine type of " + schema);
  }

  @Override
  public Iterable<JdbcEntryData> convertRecord(JdbcEntrySchema outputSchema, GenericRecord record,
      WorkUnitState workUnit) throws DataConversionException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Converting " + record);
    }
    List<JdbcEntryDatum> jdbcEntryData = Lists.newArrayList();
    for (JdbcEntryMetaDatum entry : outputSchema) {
      final String jdbcColName = entry.getColumnName();
      final JdbcType jdbcType = entry.getJdbcType();

      String avroColName = convertJdbcColNameToAvroColName(jdbcColName);
      final Object val = avroRecordValueGet(record, AVRO_RECORD_LEVEL_SPLITTER.split(avroColName).iterator());

      if (val == null) {
        jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, null));
        continue;
      }

      if (!JDBC_SUPPORTED_TYPES.contains(jdbcType)) {
        throw new DataConversionException("Unsupported JDBC type detected " + jdbcType);
      }

      switch (jdbcType) {
        case VARCHAR:
          jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, val.toString()));
          continue;
        case INTEGER:
        case BOOLEAN:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
          jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, val));
          continue;
        case DATE:
          jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, new Date((long) val)));
          continue;
        case TIME:
          jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, new Time((long) val)));
          continue;
        case TIMESTAMP:
          jdbcEntryData.add(new JdbcEntryDatum(jdbcColName, new Timestamp((long) val)));
          continue;
        default:
          throw new DataConversionException(jdbcType + " is not supported");
      }
    }
    JdbcEntryData converted = new JdbcEntryData(jdbcEntryData);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted data into " + converted);
    }
    return new SingleRecordIterable<>(converted);
  }

  private Object avroRecordValueGet(GenericRecord record, Iterator<String> recordNameIterator) {
    String name = recordNameIterator.next();
    Object val = record.get(name);
    if (val == null) {
      //Either leaf value is null or nested Record (represented as UNION) is null
      return null;
    }
    if (!recordNameIterator.hasNext()) {
      //Leaf
      return val;
    }

    //Recurse
    return avroRecordValueGet((GenericRecord) val, recordNameIterator);
  }

  @Override
  public ConverterInitializer getInitializer(State state, WorkUnitStream workUnits, int branches, int branchId) {
    JdbcWriterCommandsFactory factory = new JdbcWriterCommandsFactory();
    if (workUnits.isSafeToMaterialize()) {
      return new AvroToJdbcEntryConverterInitializer(state, workUnits.getMaterializedWorkUnitCollection(),
          factory, branches, branchId);
    } else {
      throw new RuntimeException(AvroToJdbcEntryConverter.class.getName() + " does not support work unit streams.");
    }
  }
}
