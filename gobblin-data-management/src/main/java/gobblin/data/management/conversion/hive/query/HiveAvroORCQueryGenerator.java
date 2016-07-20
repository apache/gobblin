/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.conversion.hive.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import gobblin.configuration.State;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;


/***
 * Generate Hive queries
 */
@Slf4j
public class HiveAvroORCQueryGenerator {

  private static final String SERIALIZED_PUBLISH_TABLE_COMMANDS = "serialized.publish.table.commands";
  private static final String SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH = "serialized.publish.partition.preCleanup";
  private static final String SERIALIZED_PUBLISH_PARTITION_COMMANDS = "serialized.publish.partition.commands";
  private static final String SERIALIZED_CLEANUP_COMMANDS = "serialized.cleanup.commands";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  // Table properties keys
  private static final String ORC_COMPRESSION_KEY                 = "orc.compress";
  private static final String ORC_ROW_INDEX_STRIDE_KEY            = "orc.row.index.stride";

  // Default values for Hive DDL / DML query generation
  private static final String DEFAULT_DB_NAME                     = "default";
  private static final String DEFAULT_ROW_FORMAT_SERDE            = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
  private static final String DEFAULT_ORC_INPUT_FORMAT            = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
  private static final String DEFAULT_ORC_OUTPUT_FORMAT           = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
  private static final String DEFAULT_ORC_COMPRESSION             = "SNAPPY";
  private static final String DEFAULT_ORC_ROW_INDEX_STRIDE        = "268435456";
  private static final Map<String, String> DEFAULT_TBL_PROPERTIES = ImmutableMap
      .<String, String>builder().
      put(ORC_COMPRESSION_KEY, DEFAULT_ORC_COMPRESSION).
      put(ORC_ROW_INDEX_STRIDE_KEY, DEFAULT_ORC_ROW_INDEX_STRIDE).
      build();

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
   * @param optionalTblProperties Optional table properties
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
      Optional<Map<String, String>> optionalTblProperties,
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
    Map<String, String> tblProperties = optionalTblProperties.isPresent() ? optionalTblProperties.get() :
                                                                            DEFAULT_TBL_PROPERTIES;

    // Start building Hive DDL
    // Refer to Hive DDL manual for explanation of clauses:
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/TruncateTable
    StringBuilder ddl = new StringBuilder();

    // Create statement
    ddl.append(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS `%s.%s` ", dbName, tblName));
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
    if (null != tblProperties && tblProperties.size() > 0) {
      ddl.append("TBLPROPERTIES ( \n");
      boolean isFirst = true;
      for (Map.Entry<String, String> entry : tblProperties.entrySet()) {
        if (isFirst) {
          isFirst = false;
        } else {
          ddl.append(", \n");
        }
        ddl.append(String.format("  '%s'='%s'", entry.getKey(), entry.getValue()));
      }
      ddl.append(") \n");
    }

    return ddl.toString();
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
      String type = field.getType();
      String comment = field.getComment();
      if (hiveColumns.isPresent()) {
        hiveColumns.get().put(name, type);
      }
      columns.append(String.format("  `%s` %s COMMENT '%s'", name, type, comment));
    }

    return columns.toString();
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
      dmlQuery.append(String.format("INSERT OVERWRITE TABLE `%s.%s` %n", outputDbName, outputTblName));
    } else {
      dmlQuery.append(String.format("INSERT INTO TABLE `%s.%s` %n", outputDbName, outputTblName));
    }

    // Partition details
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size()  > 0) {
        dmlQuery.append("PARTITION (");
        for (Map.Entry<String, String> partition : optionalPartitionDMLInfo.get().entrySet()) {
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

    dmlQuery.append(String.format(" %n FROM `%s.%s` ", inputDbName, inputTblName));

    // Partition details
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size()  > 0) {
        dmlQuery.append("WHERE ");
        for (Map.Entry<String, String> partition : optionalPartitionDMLInfo.get().entrySet()) {
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
  public static String generateEvolutionDDL(String stagingTableName,
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
      return StringUtils.EMPTY;
    }

    String stagingDbName = optionalStagingDbName.isPresent() ? optionalStagingDbName.get() : DEFAULT_DB_NAME;
    String finalDbName = optionalFinalDbName.isPresent() ? optionalFinalDbName.get() : DEFAULT_DB_NAME;

    StringBuilder ddl = new StringBuilder();

    // Evolve schema
    Table destinationTable = destinationTableMeta.get();
    for (Map.Entry<String, String> evolvedColumn : evolvedColumns.entrySet()) {
      // Find evolved column in destination table
      boolean found = false;
      for (FieldSchema destinationField : destinationTable.getSd().getCols()) {
        if (destinationField.getName().equalsIgnoreCase(evolvedColumn.getKey())) {
          // If evolved column is found, but type is evolved - evolve it
          // .. if incompatible, isTypeEvolved will throw an exception
          if (isTypeEvolved(evolvedColumn.getValue(), destinationField.getType())) {
            ddl.append(String.format("ALTER TABLE `%s`.`%s` CHANGE COLUMN %s %s %s COMMENT '%s'",
                finalDbName, finalTableName, evolvedColumn.getKey(), evolvedColumn.getKey(), evolvedColumn.getValue(),
                destinationField.getComment())).append("\n");
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
        ddl.append(String.format("ALTER TABLE `%s`.`%s` ADD COLUMNS (%s %s COMMENT 'from flatten_source %s')",
            finalDbName, finalTableName, evolvedColumn.getKey(), evolvedColumn.getValue(), flattenSource)).append("\n");
      }
    }

    return ddl.toString();
  }

  /***
   * Generate DDLs to publish staging table to final destination table.
   * @param stagingTableName Staging table name to publish from.
   * @param finalTableName Final table name to publish to.
   * @param optionalStagingDbName Optional staging database name, defaults to default.
   * @param optionalFinalDbName Optional final database name, defaults to default.
   * @param destinationTableMeta Existing final table metadata if any.
   * @return DDL to publish to final destination table from staging table.
   */
  public static String generatePublishTableDDL(
      String stagingTableName,
      String finalTableName,
      Optional<String> optionalStagingDbName,
      Optional<String> optionalFinalDbName,
      Optional<Table> destinationTableMeta) {
    StringBuilder ddl = new StringBuilder();

    String stagingDbName = optionalStagingDbName.isPresent() ? optionalStagingDbName.get() : DEFAULT_DB_NAME;
    String finalDbName = optionalFinalDbName.isPresent() ? optionalFinalDbName.get() : DEFAULT_DB_NAME;
    Preconditions.checkArgument(stagingDbName.equalsIgnoreCase(finalDbName), "We do not support staging and final "
        + "tables in different Hive databases because of limitations of Hive v0.13");

    // If new table, then create table
    if (!destinationTableMeta.isPresent()) {
      ddl.append(String.format("DROP TABLE IF EXISTS `%s`.`%s`", finalDbName, finalTableName)).append("\n");
      // Note: Hive does not support fully qualified Hive table names such as db.table for 'RENAME' in v0.13
      // .. hence specifying 'use dbName' as a precursor to rename
      // Refer: HIVE-2496
      ddl.append(String.format("USE `%s`", finalDbName)).append("\n");
      ddl.append(String.format("ALTER TABLE `%s` RENAME TO `%s`", stagingTableName, finalTableName)).append("\n");

      return ddl.toString();
    }

    return StringUtils.EMPTY;
  }

  /***
   * Generate DDLs to publish staging table partitions to final destination table.
   * @param stagingTableName Staging table name to publish from.
   * @param finalTableName Final table name to publish to.
   * @param optionalStagingDbName Optional staging database name, defaults to default.
   * @param optionalFinalDbName Optional final database name, defaults to default.
   * @param partitionsDMLInfo Partitions to be moved from staging to final table.
   * @param destinationTableMeta Existing final table metadata if any.
   * @param hiveVersion Hive version for compatibility.
   * @return DDL to publish to final destination table from staging table.
   */
  public static String generatePublishPartitionDDL(String stagingTableName,
      String finalTableName,
      Optional<String> optionalStagingDbName,
      Optional<String> optionalFinalDbName,
      Map<String, String> partitionsDMLInfo,
      Optional<Table> destinationTableMeta,
      Optional<String> hiveVersion) {
    if (!destinationTableMeta.isPresent() || partitionsDMLInfo.size() == 0) {
      return StringUtils.EMPTY;
    }

    // Format: alter table t4 exchange partition (ds='3') with table t3
    StringBuilder ddl = new StringBuilder();

    String stagingDbName = optionalStagingDbName.isPresent() ? optionalStagingDbName.get() : DEFAULT_DB_NAME;
    String finalDbName = optionalFinalDbName.isPresent() ? optionalFinalDbName.get() : DEFAULT_DB_NAME;

    // Schema is already evolved or if evolution is turned off then staging and final table have same schema
    // .. now we have to move partitions from staging to final table
    // Note: In Hive v0.13 exchange partition behaves inversely: HIVE-6129 and was fixed later
    //       More context: HIVE-4095
    for (Map.Entry<String, String> partition : partitionsDMLInfo.entrySet()) {
      // Note: Hive does not support fully qualified Hive table names such as db.table for ALTER TABLE in v0.13
      // .. hence specifying 'use dbName' as a precursor to rename
      // Refer: HIVE-2496
      ddl.append(String.format("USE `%s`", finalDbName)).append("\n");
      ddl.append(String.format("ALTER TABLE `%s` DROP IF EXISTS PARTITION (%s='%s')", finalTableName,
          partition.getKey(), partition.getValue())).append("\n");

      if (hiveVersion.isPresent()
          && !"0.13".equalsIgnoreCase(hiveVersion.get())
          && !"0.12".equalsIgnoreCase(hiveVersion.get())) {
        // Newer versions have the bug fixed
        ddl.append(String.format("ALTER TABLE `%s`.`%s` EXCHANGE PARTITION (%s='%s') WITH TABLE `%s`.`%s`", stagingDbName,
            stagingTableName, partition.getKey(), partition.getValue(), finalDbName, finalTableName)).append("\n");
      } else {
        // By default assume it is 0.13 or 0.12 with the bug (pre 0.12 versions did not support exchange partitions)
        ddl.append(String.format("ALTER TABLE `%s`.`%s` EXCHANGE PARTITION (%s='%s') WITH TABLE `%s`.`%s`", finalDbName,
            finalTableName, partition.getKey(), partition.getValue(), stagingDbName, stagingTableName)).append("\n");
      }
    }

    return ddl.toString();
  }

  /***
   * Find directory to delete before partition is published. This is required when a pre-existing destination
   * partition is being overwritten and it has a data directory.
   * Exchange partition has a bug because of which it is not able to cleanly overwrite data, and instead moves
   * staging table partition as a sub-directory of existing destination partition directory, rather than
   * completely replacing it.
   *
   * Refer: HIVE-11194
   *
   * @param sourcePartition Source table partition.
   * @param destinationPartitionsMeta Metadata about destination partition.
   * @return Directory to be deleted recursively before publish.
   */
  public static Optional<String> findDirToDeleteBeforePartitionPublish(
      Optional<SchemaAwareHivePartition> sourcePartition, Optional<List<Partition>> destinationPartitionsMeta) {
    if (!sourcePartition.isPresent() || !destinationPartitionsMeta.isPresent()) {
      return Optional.<String>absent();
    }

    for (Partition partitionMeta : destinationPartitionsMeta.get()) {
      if (sourcePartition.get().getName().equalsIgnoreCase(partitionMeta.getName())) {
        return Optional.of(partitionMeta.getLocation());
      }
    }

    return Optional.<String>absent();
  }

  /***
   * Generate DDL statement for cleaning up temporary staging table.
   * @param stagingTableName Staging table to be cleaned.
   * @param optionalStagingDbName Optional staging database name, defaults to default.
   * @return DDL to clean up temporary staging table.
   */
  public static String generateCleanupDDL(String stagingTableName, Optional<String> optionalStagingDbName) {
    String stagingDbName = optionalStagingDbName.isPresent() ? optionalStagingDbName.get() : DEFAULT_DB_NAME;
    return String.format("DROP TABLE IF EXISTS `%s`.`%s`", stagingDbName, stagingTableName) + "\n";
  }

  /***
   * Serialize a {@link String} of publish table commands into a {@link State} at
   * {@link #SERIALIZED_PUBLISH_TABLE_COMMANDS}.
   * @param state {@link State} to serialize commands into.
   * @param commands Publish table commands to serialize.
   */
  public static void serializePublishTableCommands(State state, String commands) {
    state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_TABLE_COMMANDS,
        GSON.toJson(commands));
  }

  /***
   * Serialize a {@link String} of dir to delete before partition publish into a {@link State} at
   * {@link #SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH}.
   * @param state {@link State} to serialize dir into.
   * @param dirName Dir to delete before partition publish.
   */
  public static void serializeDirToDeleteBeforePartitionPublish(State state, Optional<String> dirName) {
    if (dirName.isPresent()) {
      state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH,
          GSON.toJson(dirName.get()));
    } else {
      state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH,
          GSON.toJson(StringUtils.EMPTY));
    }
  }

  /***
   * Serialize a {@link String} of publish partition commands into a {@link State} at
   * {@link #SERIALIZED_PUBLISH_PARTITION_COMMANDS}.
   * @param state {@link State} to serialize commands into.
   * @param commands Publish partition commands to serialize.
   */
  public static void serializePublishPartitionCommands(State state, String commands) {
    state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_PARTITION_COMMANDS,
        GSON.toJson(commands));
  }

  /***
   * Serialize a {@link String} of cleanup commands into a {@link State} at {@link #SERIALIZED_CLEANUP_COMMANDS}.
   * @param state {@link State} to serialize commands into.
   * @param commands Cleanup commands to serialize.
   */
  public static void serializedCleanupCommands(State state, String commands) {
    state.setProp(HiveAvroORCQueryGenerator.SERIALIZED_CLEANUP_COMMANDS,
        GSON.toJson(commands));
  }

  /***
   * Deserialize the publish table commands from a {@link State} at {@link #SERIALIZED_PUBLISH_TABLE_COMMANDS}.
   * @param state {@link State} to look into for serialized commands.
   * @return Publish table commands.
   */
  public static String deserializePublishTableCommands(State state) {
    return GSON.fromJson(state.getProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_TABLE_COMMANDS), String.class);
  }

  /***
   * Deserialize the dir to delete before partition publish from a {@link State} at
   * {@link #SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH}
   * @param state {@link State} to look into for dir name.
   * @return Dir to delete before partition publish.
   */
  public static String deserializeDirToDeleteBeforePartitionPublish(State state) {
    return GSON.fromJson(state.getProp(HiveAvroORCQueryGenerator.SERIALIZED_DIR_TO_DELETE_BEFORE_PARTITION_PUBLISH),
        String.class);
  }

  /***
   * Deserialize the publish partition commands from a {@link State} at {@link #SERIALIZED_PUBLISH_PARTITION_COMMANDS}.
   * @param state {@link State} to look into for serialized commands.
   * @return Publish partition commands.
   */
  public static String deserializePublishPartitionCommands(State state) {
    return GSON.fromJson(state.getProp(HiveAvroORCQueryGenerator.SERIALIZED_PUBLISH_PARTITION_COMMANDS), String.class);
  }

  /***
   * Deserialize the cleanup commands from a {@link State} at {@link #SERIALIZED_CLEANUP_COMMANDS}.
   * @param state {@link State} to look into for serialized commands.
   * @return Cleanup commands.
   */
  public static String deserializeCleanupCommands(State state) {
    return GSON.fromJson(state.getProp(HiveAvroORCQueryGenerator.SERIALIZED_CLEANUP_COMMANDS), String.class);
  }

  private static boolean isTypeEvolved(String evolvedType, String destinationType) {
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
}
