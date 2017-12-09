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

package org.apache.gobblin.data.management.conversion.hive.query;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;


/***
 * Generate Hive queries
 */
@Slf4j
public class HiveAvroORCQueryGenerator {

  private static final String SERIALIZED_PUBLISH_TABLE_COMMANDS = "serialized.publish.table.commands";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  // Table properties keys
  public static final String ORC_COMPRESSION_KEY                 = "orc.compress";
  public static final String ORC_ROW_INDEX_STRIDE_KEY            = "orc.row.index.stride";

  // Default values for Hive DDL / DML query generation
  private static final String DEFAULT_DB_NAME                     = "default";
  private static final String DEFAULT_ROW_FORMAT_SERDE            = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
  private static final String DEFAULT_ORC_INPUT_FORMAT            = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
  private static final String DEFAULT_ORC_OUTPUT_FORMAT           = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
  private static final String DEFAULT_ORC_COMPRESSION             = "ZLIB";
  private static final String DEFAULT_ORC_ROW_INDEX_STRIDE        = "268435456";
  private static final Properties DEFAULT_TBL_PROPERTIES = new Properties();
  static {
        DEFAULT_TBL_PROPERTIES.setProperty(ORC_COMPRESSION_KEY, DEFAULT_ORC_COMPRESSION);
        DEFAULT_TBL_PROPERTIES.setProperty(ORC_ROW_INDEX_STRIDE_KEY, DEFAULT_ORC_ROW_INDEX_STRIDE);
      }

  // Avro to Hive schema mapping
  private static final Map<Schema.Type, String> AVRO_TO_HIVE_COLUMN_MAPPING_V_12 = ImmutableMap
      .<Schema.Type, String>builder()
      .put(Schema.Type.NULL,    "void")
      .put(Schema.Type.BOOLEAN, "boolean")
      .put(Schema.Type.INT,     "int")
      .put(Schema.Type.LONG,    "bigint")
      .put(Schema.Type.FLOAT,   "float")
      .put(Schema.Type.DOUBLE,  "double")
      .put(Schema.Type.BYTES,   "binary")
      .put(Schema.Type.STRING,  "string")
      .put(Schema.Type.RECORD,  "struct")
      .put(Schema.Type.MAP,     "map")
      .put(Schema.Type.ARRAY,   "array")
      .put(Schema.Type.UNION,   "uniontype")
      .put(Schema.Type.ENUM,    "string")
      .put(Schema.Type.FIXED,   "binary")
      .build();

  // Hive evolution types supported
  private static final Map<String, Set<String>> HIVE_COMPATIBLE_TYPES = ImmutableMap
      .<String, Set<String>>builder()
      .put("tinyint", ImmutableSet.<String>builder()
          .add("smallint", "int", "bigint", "float", "double", "decimal", "string", "varchar").build())
      .put("smallint",  ImmutableSet.<String>builder().add("int", "bigint", "float", "double", "decimal", "string",
          "varchar").build())
      .put("int",       ImmutableSet.<String>builder().add("bigint", "float", "double", "decimal", "string", "varchar")
          .build())
      .put("bigint",    ImmutableSet.<String>builder().add("float", "double", "decimal", "string", "varchar").build())
      .put("float",     ImmutableSet.<String>builder().add("double", "decimal", "string", "varchar").build())
      .put("double",    ImmutableSet.<String>builder().add("decimal", "string", "varchar").build())
      .put("decimal",   ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("string",    ImmutableSet.<String>builder().add("double", "decimal", "varchar").build())
      .put("varchar",   ImmutableSet.<String>builder().add("double", "string", "varchar").build())
      .put("timestamp", ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("date",      ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("binary",    Sets.<String>newHashSet())
      .put("boolean",    Sets.<String>newHashSet()).build();

  @ToString
  public static enum COLUMN_SORT_ORDER {
    ASC ("ASC"),
    DESC ("DESC");

    private final String order;

    COLUMN_SORT_ORDER(String s) {
      order = s;
    }
  }

  /***
   * Generate DDL query to create a different format (default: ORC) Hive table for a given Avro Schema
   * @param schema Avro schema to use to generate the DDL for new Hive table
   * @param tblName New Hive table name
   * @param tblLocation New hive table location
   * @param optionalDbName Optional DB name, if not specified it defaults to 'default'
   * @param optionalPartitionDDLInfo Optional partition info in form of map of partition key, partition type pair
   *                                 If not specified, the table is assumed to be un-partitioned ie of type snapshot
   * @param optionalClusterInfo Optional cluster info
   * @param optionalSortOrderInfo Optional sort order
   * @param optionalNumOfBuckets Optional number of buckets
   * @param optionalRowFormatSerde Optional row format serde, default is ORC
   * @param optionalInputFormat Optional input format serde, default is ORC
   * @param optionalOutputFormat Optional output format serde, default is ORC
   * @param tableProperties Optional table properties
   * @param isEvolutionEnabled If schema evolution is turned on
   * @param destinationTableMeta Optional destination table metadata  @return Generated DDL query to create new Hive table
   */
  public static String generateCreateTableDDL(Schema schema,
      String tblName,
      String tblLocation,
      Optional<String> optionalDbName,
      Optional<Map<String, String>> optionalPartitionDDLInfo,
      Optional<List<String>> optionalClusterInfo,
      Optional<Map<String, COLUMN_SORT_ORDER>> optionalSortOrderInfo,
      Optional<Integer> optionalNumOfBuckets,
      Optional<String> optionalRowFormatSerde,
      Optional<String> optionalInputFormat,
      Optional<String> optionalOutputFormat,
      Properties tableProperties,
      boolean isEvolutionEnabled,
      Optional<Table> destinationTableMeta,
      Map<String, String> hiveColumns) {

    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(StringUtils.isNotBlank(tblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(tblLocation));

    String dbName = optionalDbName.isPresent() ? optionalDbName.get() : DEFAULT_DB_NAME;
    String rowFormatSerde = optionalRowFormatSerde.isPresent() ? optionalRowFormatSerde.get() : DEFAULT_ROW_FORMAT_SERDE;
    String inputFormat = optionalInputFormat.isPresent() ? optionalInputFormat.get() : DEFAULT_ORC_INPUT_FORMAT;
    String outputFormat = optionalOutputFormat.isPresent() ? optionalOutputFormat.get() : DEFAULT_ORC_OUTPUT_FORMAT;
    tableProperties = getTableProperties(tableProperties);

    // Start building Hive DDL
    // Refer to Hive DDL manual for explanation of clauses:
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/TruncateTable
    StringBuilder ddl = new StringBuilder();

    // Create statement
    ddl.append(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`%s` ", dbName, tblName));
    // .. open bracket for CREATE
    ddl.append("( \n");

    // 1. If evolution is enabled, and destination table does not exists
    //    .. use columns from new schema
    //    (evolution does not matter if its new destination table)
    // 2. If evolution is enabled, and destination table does exists
    //    .. use columns from new schema
    //    (alter table will be used before moving data from staging to final table)
    // 3. If evolution is disabled, and destination table does not exists
    //    .. use columns from new schema
    //    (evolution does not matter if its new destination table)
    // 4. If evolution is disabled, and destination table does exists
    //    .. use columns from destination schema
    if (isEvolutionEnabled || !destinationTableMeta.isPresent()) {
      log.info("Generating DDL using source schema");
      ddl.append(generateAvroToHiveColumnMapping(schema, Optional.of(hiveColumns), true));
    } else {
      log.info("Generating DDL using destination schema");
      ddl.append(generateDestinationToHiveColumnMapping(Optional.of(hiveColumns), destinationTableMeta.get()));
    }

    // .. close bracket for CREATE
    ddl.append(") \n");

    // Partition info
    if (optionalPartitionDDLInfo.isPresent() && optionalPartitionDDLInfo.get().size() > 0) {
      ddl.append("PARTITIONED BY ( ");
      boolean isFirst = true;
      Map<String, String> partitionInfoMap = optionalPartitionDDLInfo.get();
      for (Map.Entry<String, String> partitionInfo : partitionInfoMap.entrySet()) {
        if (isFirst) {
          isFirst = false;
        } else {
          ddl.append(", ");
        }
        ddl.append(String.format("`%s` %s", partitionInfo.getKey(), partitionInfo.getValue()));
      }
      ddl.append(" ) \n");
    }

    if (optionalClusterInfo.isPresent()) {
      if (!optionalNumOfBuckets.isPresent()) {
        throw new IllegalArgumentException(("CLUSTERED BY requested, but no NUM_BUCKETS specified"));
      }
      ddl.append("CLUSTERED BY ( ");
      boolean isFirst = true;
      for (String clusterByCol : optionalClusterInfo.get()) {
        if (!hiveColumns.containsKey(clusterByCol)) {
          throw new IllegalArgumentException(String.format("Requested CLUSTERED BY column: %s "
              + "is not present in schema", clusterByCol));
        }
        if (isFirst) {
          isFirst = false;
        } else {
          ddl.append(", ");
        }
        ddl.append(String.format("`%s`", clusterByCol));
      }
      ddl.append(" ) ");

      if (optionalSortOrderInfo.isPresent() && optionalSortOrderInfo.get().size() > 0) {
        Map<String, COLUMN_SORT_ORDER> sortOrderInfoMap = optionalSortOrderInfo.get();
        ddl.append("SORTED BY ( ");
        isFirst = true;
        for (Map.Entry<String, COLUMN_SORT_ORDER> sortOrderInfo : sortOrderInfoMap.entrySet()){
          if (!hiveColumns.containsKey(sortOrderInfo.getKey())) {
            throw new IllegalArgumentException(String.format(
                "Requested SORTED BY column: %s " + "is not present in schema", sortOrderInfo.getKey()));
          }
          if (isFirst) {
            isFirst = false;
          } else {
            ddl.append(", ");
          }
          ddl.append(String.format("`%s` %s", sortOrderInfo.getKey(), sortOrderInfo.getValue()));
        }
        ddl.append(" ) ");
      }
      ddl.append(String.format(" INTO %s BUCKETS %n", optionalNumOfBuckets.get()));
    } else {
      if (optionalSortOrderInfo.isPresent()) {
        throw new IllegalArgumentException("SORTED BY requested, but no CLUSTERED BY specified");
      }
    }

    // Field Terminal
    ddl.append("ROW FORMAT SERDE \n");
    ddl.append(String.format("  '%s' %n", rowFormatSerde));

    // Stored as ORC
    ddl.append("STORED AS INPUTFORMAT \n");
    ddl.append(String.format("  '%s' %n", inputFormat));
    ddl.append("OUTPUTFORMAT \n");
    ddl.append(String.format("  '%s' %n", outputFormat));

    // Location
    ddl.append("LOCATION \n");
    ddl.append(String.format("  '%s' %n", tblLocation));

    // Table properties
    if (null != tableProperties && tableProperties.size() > 0) {
      ddl.append("TBLPROPERTIES ( \n");
      boolean isFirst = true;
      for (String property : tableProperties.stringPropertyNames()) {
        if (isFirst) {
          isFirst = false;
        } else {
          ddl.append(", \n");
        }
        ddl.append(String.format("  '%s'='%s'", property, tableProperties.getProperty(property)));
      }
      ddl.append(") \n");
    }

    return ddl.toString();
  }

  private static Properties getTableProperties(Properties tableProperties) {
    if (null == tableProperties || tableProperties.size() == 0) {
      return DEFAULT_TBL_PROPERTIES;
    }

    for (String property : DEFAULT_TBL_PROPERTIES.stringPropertyNames()) {
      if (!tableProperties.containsKey(property)) {
        tableProperties.put(property, DEFAULT_TBL_PROPERTIES.get(property));
      }
    }

    return tableProperties;
  }

  /***
   * Generate DDL query to create a Hive partition pointing at specific location.
   * @param dbName Hive database name.
   * @param tableName Hive table name.
   * @param partitionLocation Physical location of partition.
   * @param partitionsDMLInfo Partitions DML info - a map of partition name and partition value.
   * @param format Hive partition file format
   * @return Commands to create a partition.
   */
  public static List<String> generateCreatePartitionDDL(String dbName, String tableName, String partitionLocation,
      Map<String, String> partitionsDMLInfo, Optional<String> format) {

    if (null == partitionsDMLInfo || partitionsDMLInfo.size() == 0) {
      return Collections.emptyList();
    }

    // Partition details
    StringBuilder partitionSpecs = new StringBuilder();
    partitionSpecs.append("PARTITION (");
    boolean isFirstPartitionSpec = true;
    for (Map.Entry<String, String> partition : partitionsDMLInfo.entrySet()) {
      if (isFirstPartitionSpec) {
        isFirstPartitionSpec = false;
      } else {
        partitionSpecs.append(", ");
      }
      partitionSpecs.append(String.format("`%s`='%s'", partition.getKey(), partition.getValue()));
    }
    partitionSpecs.append(") \n");

    // Create statement
    List<String> ddls = Lists.newArrayList();
    // Note: Hive does not support fully qualified Hive table names such as db.table for ALTER TABLE in v0.13
    // .. hence specifying 'use dbName' as a precursor to rename
    // Refer: HIVE-2496
    ddls.add(String.format("USE %s%n", dbName));
    if (format.isPresent()) {
      ddls.add(String
          .format("ALTER TABLE `%s` ADD IF NOT EXISTS %s FILEFORMAT %s LOCATION '%s' ", tableName, partitionSpecs,
              format.get(), partitionLocation));
    } else {
      ddls.add(String.format("ALTER TABLE `%s` ADD IF NOT EXISTS %s LOCATION '%s' ", tableName, partitionSpecs,
          partitionLocation));
    }

    return ddls;
  }

  public static List<String> generateCreatePartitionDDL(String dbName, String tableName, String partitionLocation,
      Map<String, String> partitionsDMLInfo) {
    return generateCreatePartitionDDL(dbName, tableName, partitionLocation, partitionsDMLInfo,
        Optional.<String>absent());
  }

  /***
   * Generate DDL query to drop a Hive table.
   * @param dbName Hive database name.
   * @param tableName Hive table name.
   * @return Command to drop the table.
   */
  public static String generateDropTableDDL(String dbName, String tableName) {
    return String.format("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tableName);
  }

  /***
   * Adapt Avro schema / types to Hive column types
   * @param schema Schema to adapt and generate Hive columns with corresponding types
   * @param hiveColumns Optional Map to populate with the generated hive columns for reference of caller
   * @param topLevel If this is first level
   * @return Generate Hive columns with types for given Avro schema
   */
  private static String generateAvroToHiveColumnMapping(Schema schema,
      Optional<Map<String, String>> hiveColumns,
      boolean topLevel) {
    if (topLevel && !schema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException(String.format("Schema for table must be of type RECORD. Received type: %s",
          schema.getType()));
    }

    StringBuilder columns = new StringBuilder();
    boolean isFirst;
    switch (schema.getType()) {
      case RECORD:
        isFirst = true;
        if (topLevel) {
          for (Schema.Field field : schema.getFields()) {
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(", \n");
            }
            String type = generateAvroToHiveColumnMapping(field.schema(), hiveColumns, false);
            if (hiveColumns.isPresent()) {
              hiveColumns.get().put(field.name(), type);
            }
            String flattenSource = field.getProp("flatten_source");
            if (StringUtils.isBlank(flattenSource)) {
              flattenSource = field.name();
            }
            columns.append(String.format("  `%s` %s COMMENT 'from flatten_source %s'", field.name(), type,flattenSource));
          }
        } else {
          columns.append(AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
          for (Schema.Field field : schema.getFields()) {
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(",");
            }
            String type = generateAvroToHiveColumnMapping(field.schema(), hiveColumns, false);
            columns.append("`").append(field.name()).append("`").append(":").append(type);
          }
          columns.append(">");
        }
        break;
      case UNION:
        Optional<Schema> optionalType = isOfOptionType(schema);
        if (optionalType.isPresent()) {
          Schema optionalTypeSchema = optionalType.get();
          columns.append(generateAvroToHiveColumnMapping(optionalTypeSchema, hiveColumns, false));
        } else {
          columns.append(AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
          isFirst = true;
          for (Schema unionMember : schema.getTypes()) {
            if (Schema.Type.NULL.equals(unionMember.getType())) {
              continue;
            }
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(",");
            }
            columns.append(generateAvroToHiveColumnMapping(unionMember, hiveColumns, false));
          }
          columns.append(">");
        }
        break;
      case MAP:
        columns.append(AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
        columns.append("string,").append(generateAvroToHiveColumnMapping(schema.getValueType(), hiveColumns, false));
        columns.append(">");
        break;
      case ARRAY:
        columns.append(AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
        columns.append(generateAvroToHiveColumnMapping(schema.getElementType(), hiveColumns, false));
        columns.append(">");
        break;
      case NULL:
        break;
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
      case BOOLEAN:
        columns.append(AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType()));
        break;
      default:
        String exceptionMessage = String.format("DDL query generation failed for \"%s\" ", schema);
        log.error(exceptionMessage);
        throw new AvroRuntimeException(exceptionMessage);
    }

    return columns.toString();
  }

  /***
   * Use destination table schema to generate column mapping
   * @param hiveColumns Optional Map to populate with the generated hive columns for reference of caller
   * @param destinationTableMeta destination table metadata
   * @return Generate Hive columns with types for given Avro schema
   */
  private static String generateDestinationToHiveColumnMapping(
      Optional<Map<String, String>> hiveColumns,
      Table destinationTableMeta) {
    StringBuilder columns = new StringBuilder();
    boolean isFirst = true;
    List<FieldSchema> fieldList = destinationTableMeta.getSd().getCols();
    for (FieldSchema field : fieldList) {
      if (isFirst) {
        isFirst = false;
      } else {
        columns.append(", \n");
      }
      String name = field.getName();
      String type = escapeHiveType(field.getType());
      String comment = field.getComment();
      if (hiveColumns.isPresent()) {
        hiveColumns.get().put(name, type);
      }
      columns.append(String.format("  `%s` %s COMMENT '%s'", name, type, escapeStringForHive(comment)));
    }

    return columns.toString();
  }

  /***
   * Escape the Hive nested field names.
   * @param type Primitive or nested Hive type.
   * @return Escaped Hive nested field.
   */
  public static String escapeHiveType(String type) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);

    // Primitve
    if (ObjectInspector.Category.PRIMITIVE.equals(typeInfo.getCategory())) {
      return type;
    }
    // List
    else if (ObjectInspector.Category.LIST.equals(typeInfo.getCategory())) {
      ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
      return org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "<"
          + escapeHiveType(listTypeInfo.getListElementTypeInfo().getTypeName()) + ">";
    }
    // Map
    else if (ObjectInspector.Category.MAP.equals(typeInfo.getCategory())) {
      MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
      return org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME + "<"
          + escapeHiveType(mapTypeInfo.getMapKeyTypeInfo().getTypeName()) + ","
          + escapeHiveType(mapTypeInfo.getMapValueTypeInfo().getTypeName()) + ">";
    }
    // Struct
    else if (ObjectInspector.Category.STRUCT.equals(typeInfo.getCategory())) {
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<String> allStructFieldNames = structTypeInfo.getAllStructFieldNames();
      List<TypeInfo> allStructFieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      StringBuilder sb = new StringBuilder();
      sb.append(serdeConstants.STRUCT_TYPE_NAME + "<");
      for (int i = 0; i < allStructFieldNames.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append("`");
        sb.append(allStructFieldNames.get(i));
        sb.append("`");
        sb.append(":");
        sb.append(escapeHiveType(allStructFieldTypeInfos.get(i).getTypeName()));
      }
      sb.append(">");
      return sb.toString();
    }
    // Union
    else if (ObjectInspector.Category.UNION.equals(typeInfo.getCategory())) {
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      List<TypeInfo> allUnionObjectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();

      StringBuilder sb = new StringBuilder();
      sb.append(serdeConstants.UNION_TYPE_NAME + "<");
      for (int i = 0; i < allUnionObjectTypeInfos.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(escapeHiveType(allUnionObjectTypeInfos.get(i).getTypeName()));
      }
      sb.append(">");
      return sb.toString();
    } else {
      throw new RuntimeException("Unknown type encountered: " + type);
    }
  }

  /***
   * Check if the Avro Schema is of type OPTION
   * ie. [null, TYPE] or [TYPE, null]
   * @param schema Avro Schema to check
   * @return Optional Avro Typed data if schema is of type OPTION
   */
  private static Optional<Schema> isOfOptionType(Schema schema) {
    Preconditions.checkNotNull(schema);

    // If not of type UNION, cant be an OPTION
    if (!Schema.Type.UNION.equals(schema.getType())) {
      return Optional.<Schema>absent();
    }

    // If has more than two members, can't be an OPTION
    List<Schema> types = schema.getTypes();
    if (null != types && types.size() == 2) {
      Schema first = types.get(0);
      Schema second = types.get(1);

      // One member should be of type NULL and other of non NULL type
      if (Schema.Type.NULL.equals(first.getType()) && !Schema.Type.NULL.equals(second.getType())) {
        return Optional.of(second);
      } else if (!Schema.Type.NULL.equals(first.getType()) && Schema.Type.NULL.equals(second.getType())) {
        return Optional.of(first);
      }
    }

    return Optional.<Schema>absent();
  }

  /***
   * Generate DML mapping query to populate output schema table by selecting from input schema table
   * This method assumes that each output schema field has a corresponding source input table's field reference
   * .. in form of 'flatten_source' property
   * @param inputAvroSchema Input schema that was used to obtain output schema (next argument)
   * @param outputOrcSchema Output schema (flattened or nested) that was generated using input schema
   *                        .. and has lineage information compatible with input schema
   * @param inputTblName Input table name
   * @param outputTblName Output table name
   * @param optionalInputDbName Optional input DB name, if not specified it will default to 'default'
   * @param optionalOutputDbName Optional output DB name, if not specified it will default to 'default'
   * @param optionalPartitionDMLInfo Optional partition info in form of map of partition key, partition value pairs
   * @param optionalOverwriteTable Optional overwrite table, if not specified it is set to true
   * @param optionalCreateIfNotExists Optional create if not exists, if not specified it is set to false
   * @param isEvolutionEnabled If schema evolution is turned on
   * @param destinationTableMeta Optional destination table metadata
   * @param rowLimit Optional row limit
   * @return DML query
   */
  public static String generateTableMappingDML(Schema inputAvroSchema,
      Schema outputOrcSchema,
      String inputTblName,
      String outputTblName,
      Optional<String> optionalInputDbName,
      Optional<String> optionalOutputDbName,
      Optional<Map<String, String>> optionalPartitionDMLInfo,
      Optional<Boolean> optionalOverwriteTable,
      Optional<Boolean> optionalCreateIfNotExists,
      boolean isEvolutionEnabled,
      Optional<Table> destinationTableMeta,
      Optional<Integer> rowLimit) {
    Preconditions.checkNotNull(inputAvroSchema);
    Preconditions.checkNotNull(outputOrcSchema);
    Preconditions.checkArgument(StringUtils.isNotBlank(inputTblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(outputTblName));

    String inputDbName = optionalInputDbName.isPresent() ? optionalInputDbName.get() : DEFAULT_DB_NAME;
    String outputDbName = optionalOutputDbName.isPresent() ? optionalOutputDbName.get() : DEFAULT_DB_NAME;
    boolean shouldOverwriteTable = optionalOverwriteTable.isPresent() ? optionalOverwriteTable.get() : true;
    boolean shouldCreateIfNotExists = optionalCreateIfNotExists.isPresent() ? optionalCreateIfNotExists.get() : false;

    log.debug("Input Schema: " + inputAvroSchema.toString());
    log.debug("Output Schema: " + outputOrcSchema.toString());

    // Start building Hive DML
    // Refer to Hive DDL manual for explanation of clauses:
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
    StringBuilder dmlQuery = new StringBuilder();

    // Insert query
    if (shouldOverwriteTable) {
      dmlQuery.append(String.format("INSERT OVERWRITE TABLE `%s`.`%s` %n", outputDbName, outputTblName));
    } else {
      dmlQuery.append(String.format("INSERT INTO TABLE `%s`.`%s` %n", outputDbName, outputTblName));
    }

    // Partition details
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size()  > 0) {
        dmlQuery.append("PARTITION (");
        boolean isFirstPartitionSpec = true;
        for (Map.Entry<String, String> partition : optionalPartitionDMLInfo.get().entrySet()) {
          if (isFirstPartitionSpec) {
            isFirstPartitionSpec = false;
          } else {
            dmlQuery.append(", ");
          }
          dmlQuery.append(String.format("`%s`='%s'", partition.getKey(), partition.getValue()));
        }
        dmlQuery.append(") \n");
      }
    }

    // If not exists
    if (shouldCreateIfNotExists) {
      dmlQuery.append(" IF NOT EXISTS \n");
    }

    // Select query
    dmlQuery.append("SELECT \n");

    // 1. If evolution is enabled, and destination table does not exists
    //    .. use columns from new schema
    //    (evolution does not matter if its new destination table)
    // 2. If evolution is enabled, and destination table does exists
    //    .. use columns from new schema
    //    (alter table will be used before moving data from staging to final table)
    // 3. If evolution is disabled, and destination table does not exists
    //    .. use columns from new schema
    //    (evolution does not matter if its new destination table)
    // 4. If evolution is disabled, and destination table does exists
    //    .. use columns from destination schema
    if (isEvolutionEnabled || !destinationTableMeta.isPresent()) {
      log.info("Generating DML using source schema");
      boolean isFirst = true;
      List<Schema.Field> fieldList = outputOrcSchema.getFields();
      for (Schema.Field field : fieldList) {
        String flattenSource = field.getProp("flatten_source");
        String colName;
        if (StringUtils.isNotBlank(flattenSource)) {
          colName = flattenSource;
        } else {
          colName = field.name();
        }
        // Escape the column name
        colName = colName.replaceAll("\\.", "`.`");

        if (isFirst) {
          isFirst = false;
        } else {
          dmlQuery.append(", \n");
        }
        dmlQuery.append(String.format("  `%s`", colName));
      }
    } else {
      log.info("Generating DML using destination schema");
      boolean isFirst = true;
      List<FieldSchema> fieldList = destinationTableMeta.get().getSd().getCols();
      for (FieldSchema field : fieldList) {
        String colName = StringUtils.EMPTY;
        if (field.isSetComment() && field.getComment().startsWith("from flatten_source ")) {
          // Retrieve from column (flatten_source) from comment
          colName = field.getComment().replaceAll("from flatten_source ", "").trim();
        } else {
          // Or else find field in flattened schema
          List<Schema.Field> evolvedFieldList = outputOrcSchema.getFields();
          for (Schema.Field evolvedField : evolvedFieldList) {
            if (evolvedField.name().equalsIgnoreCase(field.getName())) {
              String flattenSource = evolvedField.getProp("flatten_source");
              if (StringUtils.isNotBlank(flattenSource)) {
                colName = flattenSource;
              } else {
                colName = evolvedField.name();
              }
              break;
            }
          }
        }
        // Escape the column name
        colName = colName.replaceAll("\\.", "`.`");

        // colName can be blank if it is deleted in new evolved schema, so we shouldn't try to fetch it
        if (StringUtils.isNotBlank(colName)) {
          if (isFirst) {
            isFirst = false;
          } else {
            dmlQuery.append(", \n");
          }
          dmlQuery.append(String.format("  `%s`", colName));
        }
      }
    }

    dmlQuery.append(String.format(" %n FROM `%s`.`%s` ", inputDbName, inputTblName));

    // Partition details
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size() > 0) {
        dmlQuery.append("WHERE ");
        boolean isFirstPartitionSpec = true;
        for (Map.Entry<String, String> partition : optionalPartitionDMLInfo.get().entrySet()) {
          if (isFirstPartitionSpec) {
            isFirstPartitionSpec = false;
          } else {
            dmlQuery.append(" AND ");
          }
          dmlQuery.append(String.format("`%s`='%s'",
              partition.getKey(), partition.getValue()));
        }
        dmlQuery.append(" \n");
      }
    }

    // Limit clause
    if (rowLimit.isPresent()) {
      dmlQuery.append(String.format("LIMIT %s", rowLimit.get()));
    }

    return dmlQuery.toString();
  }

  public static Schema readSchemaFromString(String schemaStr)
      throws IOException {
    return new Schema.Parser().parse(schemaStr);
  }

  /***
   * Generate DDLs to evolve final destination table.
   * @param stagingTableName Staging table.
   * @param finalTableName Un-evolved final destination table.
   * @param optionalStagingDbName Optional staging database name, defaults to default.
   * @param optionalFinalDbName Optional final database name, defaults to default.
   * @param evolvedSchema Evolved Avro Schema.
   * @param isEvolutionEnabled Is schema evolution enabled.
   * @param evolvedColumns Evolved columns in Hive format.
   * @param destinationTableMeta Destination table metadata.
   * @return DDLs to evolve final destination table.
   */
  public static List<String> generateEvolutionDDL(String stagingTableName,
      String finalTableName,
      Optional<String> optionalStagingDbName,
      Optional<String> optionalFinalDbName,
      Schema evolvedSchema,
      boolean isEvolutionEnabled,
      Map<String, String> evolvedColumns,
      Optional<Table> destinationTableMeta) {
    // If schema evolution is disabled, then do nothing OR
    // If destination table does not exists, then do nothing
    if (!isEvolutionEnabled || !destinationTableMeta.isPresent()) {
      return Collections.emptyList();
    }

    String stagingDbName = optionalStagingDbName.isPresent() ? optionalStagingDbName.get() : DEFAULT_DB_NAME;
    String finalDbName = optionalFinalDbName.isPresent() ? optionalFinalDbName.get() : DEFAULT_DB_NAME;

    List<String> ddl = Lists.newArrayList();

    // Evolve schema
    Table destinationTable = destinationTableMeta.get();
    if (destinationTable.getSd().getCols().size() == 0) {
      log.warn("Desination Table: " + destinationTable + " does not has column details in StorageDescriptor. "
          + "It is probably of Avro type. Cannot evolve via traditional HQL, so skipping evolution checks.");
      return ddl;
    }
    for (Map.Entry<String, String> evolvedColumn : evolvedColumns.entrySet()) {
      // Find evolved column in destination table
      boolean found = false;
      for (FieldSchema destinationField : destinationTable.getSd().getCols()) {
        if (destinationField.getName().equalsIgnoreCase(evolvedColumn.getKey())) {
          // If evolved column is found, but type is evolved - evolve it
          // .. if incompatible, isTypeEvolved will throw an exception
          if (isTypeEvolved(evolvedColumn.getValue(), destinationField.getType())) {
            ddl.add(String.format("USE %s%n", finalDbName));
            ddl.add(String.format("ALTER TABLE `%s` CHANGE COLUMN %s %s %s COMMENT '%s'",
                finalTableName, evolvedColumn.getKey(), evolvedColumn.getKey(), evolvedColumn.getValue(),
                escapeStringForHive(destinationField.getComment())));
          }
          found = true;
          break;
        }
      }
      if (!found) {
        // If evolved column is not found ie. its new, add this column
        String flattenSource = evolvedSchema.getField(evolvedColumn.getKey()).getProp("flatten_source");
        if (StringUtils.isBlank(flattenSource)) {
          flattenSource = evolvedSchema.getField(evolvedColumn.getKey()).name();
        }
        // Note: Hive does not support fully qualified Hive table names such as db.table for ALTER TABLE in v0.13
        // .. hence specifying 'use dbName' as a precursor to rename
        // Refer: HIVE-2496
        ddl.add(String.format("USE %s%n", finalDbName));
        ddl.add(String.format("ALTER TABLE `%s` ADD COLUMNS (%s %s COMMENT 'from flatten_source %s')",
            finalTableName, evolvedColumn.getKey(), evolvedColumn.getValue(), flattenSource));
      }
    }

    return ddl;
  }

  /**
   * Generate DDL for dropping partitions of a table.
   * <p>
   * ALTER TABLE finalTableName DROP IF EXISTS PARTITION partition_spec, PARTITION partition_spec, ...;
   * </p>
   * @param finalTableName Table name where partitions are dropped
   * @param partitionsDMLInfo Partitions to be dropped
   * @return DDL to drop partitions in <code>finalTableName</code>
   */
  public static List<String> generateDropPartitionsDDL(final String dbName, final String finalTableName,
      final Map<String, String> partitionsDMLInfo) {

    if (null == partitionsDMLInfo || partitionsDMLInfo.isEmpty()) {
      return Collections.emptyList();
    }

    // Partition details
    StringBuilder partitionSpecs = new StringBuilder();
    partitionSpecs.append("PARTITION (");
    boolean isFirstPartitionSpec = true;
    for (Map.Entry<String, String> partition : partitionsDMLInfo.entrySet()) {
      if (isFirstPartitionSpec) {
        isFirstPartitionSpec = false;
      } else {
        partitionSpecs.append(", ");
      }
      partitionSpecs.append(String.format("`%s`='%s'", partition.getKey(), partition.getValue()));
    }
    partitionSpecs.append(") ");

    List<String> ddls = Lists.newArrayList();
    // Note: Hive does not support fully qualified Hive table names such as db.table for ALTER TABLE in v0.13
    // .. hence specifying 'use dbName' as a precursor to rename
    // Refer: HIVE-2496
    ddls.add(String.format("USE %s%n", dbName));
    ddls.add(String.format("ALTER TABLE %s DROP IF EXISTS %s", finalTableName, partitionSpecs));

    return ddls;
  }

  /**
   * Generate DDL for dropping partitions of a table.
   * <p>
   * ALTER TABLE finalTableName DROP IF EXISTS PARTITION partition_spec, PARTITION partition_spec, ...;
   * </p>
   * @param finalTableName Table name where partitions are dropped
   * @param partitionDMLInfos list of Partition to be dropped
   * @return DDL to drop partitions in <code>finalTableName</code>
   */
  public static List<String> generateDropPartitionsDDL(final String dbName, final String finalTableName,
      final List<Map<String, String>> partitionDMLInfos) {

    if (partitionDMLInfos.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> ddls = Lists.newArrayList();
    ddls.add(String.format("USE %s %n", dbName));
    // Join the partition specs
    ddls.add(String.format("ALTER TABLE %s DROP IF EXISTS %s", finalTableName,
        Joiner.on(",").join(Iterables.transform(partitionDMLInfos, PARTITION_SPEC_GENERATOR))));

    return ddls;
  }

  /***
   * Generate DDL for creating and updating view over a table.
   *
   * Create view:
   * <p>
   *   CREATE VIEW IF NOT EXISTS db.viewName AS SELECT * FROM db.tableName
   * </p>
   *
   * Update view:
   * <p>
   *   ALTER VIEW db.viewName AS SELECT * FROM db.tableName
   * </p>
   *
   * @param tableDbName       Database for the table over which view has to be created.
   * @param tableName         Table over which view has to be created.
   * @param viewDbName        Database for the view to be created.
   * @param viewName          View to be created.
   * @param shouldUpdateView  If view should be forced re-built.
   * @return DDLs to create and / or update view over a table
   */
  public static List<String> generateCreateOrUpdateViewDDL(final String tableDbName, final String tableName,
      final String viewDbName, final String viewName, final boolean shouldUpdateView) {

    Preconditions.checkArgument(StringUtils.isNotBlank(tableName), "Table name should not be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(viewName), "View name should not be empty");

    // Resolve defaults
    String resolvedTableDbName = (StringUtils.isBlank(tableDbName)) ? DEFAULT_DB_NAME : tableDbName;
    String resolvedViewDbName = (StringUtils.isBlank(viewDbName)) ? DEFAULT_DB_NAME : viewDbName;

    List<String> ddls = Lists.newArrayList();

    // No-op if view already exists
    ddls.add(String.format("CREATE VIEW IF NOT EXISTS `%s`.`%s` AS SELECT * FROM `%s`.`%s`",
        resolvedViewDbName, viewName,
        resolvedTableDbName, tableName));

    // This will force re-build the view
    if (shouldUpdateView) {
      ddls.add(String.format("ALTER VIEW `%s`.`%s` AS SELECT * FROM `%s`.`%s`",
          resolvedViewDbName, viewName,
          resolvedTableDbName, tableName));
    }

    return ddls;
  }

  /***
   * Generate DDL for updating file format of table or partition.
   * If partition spec is absent, DDL query to change storage format of Table is generated.
   *
   * Query syntax:
   * <p>
   *   ALTER TABLE tableName [PARTITION partition_spec] SET FILEFORMAT fileFormat
   * </p>
   *
   * @param dbName            Database for the table for which storage format needs to be changed.
   * @param tableName         Table for which storage format needs to be changed.
   * @param partitionsDMLInfo Optional partition spec for which storage format needs to be changed.
   * @param format            Storage format.
   * @return DDL to change storage format for Table or Partition.
   */
  public static List<String> generateAlterTableOrPartitionStorageFormatDDL(final String dbName,
      final String tableName,
      final Optional<Map<String, String>> partitionsDMLInfo,
      String format) {
    Preconditions.checkArgument(StringUtils.isNotBlank(tableName), "Table name should not be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(format), "Format should not be empty");

    // Resolve defaults
    String resolvedDbName = (StringUtils.isBlank(dbName)) ? DEFAULT_DB_NAME : dbName;

    // Partition details
    StringBuilder partitionSpecs = new StringBuilder();

    if (partitionsDMLInfo.isPresent()) {
      partitionSpecs.append("PARTITION (");
      boolean isFirstPartitionSpec = true;
      for (Map.Entry<String, String> partition : partitionsDMLInfo.get().entrySet()) {
        if (isFirstPartitionSpec) {
          isFirstPartitionSpec = false;
        } else {
          partitionSpecs.append(", ");
        }
        partitionSpecs.append(String.format("`%s`='%s'", partition.getKey(), partition.getValue()));
      }
      partitionSpecs.append(") ");
    }

    List<String> ddls = Lists.newArrayList();


    // Note: Hive does not support fully qualified Hive table names such as db.table for ALTER TABLE in v0.13
    // .. hence specifying 'use dbName' as a precursor to rename
    // Refer: HIVE-2496
    ddls.add(String.format("USE %s%n", resolvedDbName));
    ddls.add(String.format("ALTER TABLE %s %s SET FILEFORMAT %s", tableName, partitionSpecs, format));

    return ddls;
  }

  /***
   * Serialize a {@link QueryBasedHivePublishEntity} into a {@link State} at {@link #SERIALIZED_PUBLISH_TABLE_COMMANDS}.
   * @param state {@link State} to serialize entity into.
   * @param queryBasedHivePublishEntity to carry to publisher.
   */
  public static void serializePublishCommands(State state, QueryBasedHivePublishEntity queryBasedHivePublishEntity) {
    state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_TABLE_COMMANDS,
        GSON.toJson(queryBasedHivePublishEntity));
  }

  /***
   * Deserialize the publish entity from a {@link State} at {@link #SERIALIZED_PUBLISH_TABLE_COMMANDS}.
   * @param state {@link State} to look into for serialized entity.
   * @return Publish table entity.
   */
  public static QueryBasedHivePublishEntity deserializePublishCommands(State state) {
    QueryBasedHivePublishEntity queryBasedHivePublishEntity =
        GSON.fromJson(state.getProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_TABLE_COMMANDS),
            QueryBasedHivePublishEntity.class);
    return queryBasedHivePublishEntity == null ? new QueryBasedHivePublishEntity() : queryBasedHivePublishEntity;
  }

  public static boolean isTypeEvolved(String evolvedType, String destinationType) {
    if (evolvedType.equalsIgnoreCase(destinationType)) {
      // Same type, not evolved
      return false;
    }
    // Look for compatibility in evolved type
    if (HIVE_COMPATIBLE_TYPES.containsKey(destinationType)) {
      if (HIVE_COMPATIBLE_TYPES.get(destinationType).contains(evolvedType)) {
        return true;
      } else {
        throw new RuntimeException(String.format("Incompatible type evolution from: %s to: %s",
            destinationType, evolvedType));
      }
    } else {
      // We assume all complex types are compatible
      // TODO: Add compatibility check when ORC evolution supports complex types
      return true;
    }
  }

  /**
   * Generate partition spec in Hive standard syntax. (partition_column=partition_col_value, partition_column=partition_col_value, ...)
   */
  private static final Function<Map<String, String>, String> PARTITION_SPEC_GENERATOR = new Function<Map<String, String>, String>() {
    @Override
    public String apply(Map<String, String> partitionDMLInfo) {

      if (partitionDMLInfo == null) {
        return StringUtils.EMPTY;
      }
      return String.format(" PARTITION (%s)", Joiner.on(",").withKeyValueSeparator("=").join(Maps.transformValues(partitionDMLInfo, QUOTE_PARTITION_VALUES)));
    }
  };

  private static final Function<String, String> QUOTE_PARTITION_VALUES = new Function<String, String>() {

    @Override
    public String apply(String value) {
      return String.format("'%s'", value);
    }
  };

  private static String escapeStringForHive(String st) {
    char backslash = '\\';
    char singleQuote = '\'';
    char semicolon = ';';
    String escapedSingleQuote = String.valueOf(backslash) + String.valueOf(singleQuote);
    String escapedSemicolon = String.valueOf(backslash) + String.valueOf(semicolon);

    st = st.replace(String.valueOf(singleQuote), escapedSingleQuote)
        .replace(String.valueOf(semicolon), escapedSemicolon);
    return st;
  }
}
