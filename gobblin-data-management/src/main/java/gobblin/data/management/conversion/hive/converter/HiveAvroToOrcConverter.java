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
package gobblin.data.management.conversion.hive.converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.util.HiveAvroORCQueryUtils;
import gobblin.util.AvroFlattener;

/**
 * Builds the Hive avro to Orc conversion query. The record type for this converter is {@link QueryBasedHiveConversionEntity}. A {@link QueryBasedHiveConversionEntity}
 * can be a hive table or a hive partition.
 */
@Slf4j
public class HiveAvroToOrcConverter
    extends Converter<Schema, Schema, QueryBasedHiveConversionEntity, QueryBasedHiveConversionEntity> {

  // TODO: Remove when topology is enabled
  private static final String ORC_TABLE_ALTERNATE_LOCATION = "orc.table.alternate.location";

  private static AvroFlattener avroFlattener = new AvroFlattener();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Populate the avro to orc conversion queries. The Queries will be added to {@link QueryBasedHiveConversionEntity#getQueries()}
   */
  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema outputSchema,
      QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit) throws DataConversionException {
    Preconditions.checkNotNull(outputSchema);
    Preconditions.checkNotNull(conversionEntity);
    Preconditions.checkNotNull(workUnit);
    Preconditions.checkNotNull(conversionEntity.getHiveTable());

    Schema flattenedSchema = avroFlattener.flatten(outputSchema, true);

    // Create flattened table if not exists
    // ORC Hive tables are named as   : {avro_table_name}_orc
    // ORC Hive tables use location as: {avro_table_location}_orc
    // TODO: Add cluster by info (from config)
    // TODO: Add sort by info (from config)
    // TODO: Add num of buckets (from config)

    // Avro table name and location
    String avroTableName = conversionEntity.getHiveTable().getTableName();
    String avroTableLocation = conversionEntity.getHiveTable().getSd().getLocation();

    // ORC table name and location
    String orcTableName = avroTableName + "_orc";
    String orcTableLocation;

    // By default ORC table creates a new directory where Avro data resides with _orc postfix, but this can be
    // .. overridden by specifying this property
    String orcTableAlternateLocation = workUnit.getJobState().getProp(ORC_TABLE_ALTERNATE_LOCATION);
    if (StringUtils.isNotBlank(orcTableAlternateLocation)) {
      orcTableLocation = orcTableAlternateLocation.endsWith("/") ?
           orcTableAlternateLocation + orcTableName : orcTableAlternateLocation + "/" + orcTableName;
    } else {
      orcTableLocation = (avroTableLocation.endsWith("/") ?
          avroTableLocation.substring(0, avroTableLocation.length() - 1) : avroTableLocation) + "_orc";
    }

    // Each job execution further writes to a sub-directory within ORC data directory to support stagin use-case
    // .. ie for atomic swap
    orcTableLocation += "/" + workUnit.getJobState().getId();

    // Populate optional partition info
    Optional<Map<String, String>> optionalPartitionsDDLInfo = Optional.<Map<String, String>>absent();
    Optional<Map<String, String>> optionalPartitionsDMLInfo = Optional.<Map<String, String>>absent();

    String partitionsInfoString = null;
    String partitionsTypeString = null;

    if (conversionEntity.getHivePartition().isPresent()) {
      partitionsInfoString = conversionEntity.getHivePartition().get().getName();
      partitionsTypeString = conversionEntity.getHivePartition().get().getSchema().getProperty("partition_columns.types");
    }

    if (StringUtils.isNotBlank(partitionsInfoString) || StringUtils.isNotBlank(partitionsTypeString)) {
      if (StringUtils.isBlank(partitionsInfoString) || StringUtils.isBlank(partitionsTypeString)) {
        throw new IllegalArgumentException("Both partitions info and partions must be present, if one is specified");
      }
      Map<String, String> partitionDDLInfo = new HashMap<>();
      Map<String, String> partitionDMLInfo = new HashMap<>();
      List<String> pInfo = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsInfoString);
      List<String> pType = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsTypeString);
      if (pInfo.size() != pType.size()) {
        throw new IllegalArgumentException("partitions info and partitions type list should of same size");
      }
      for (int i=0; i<pInfo.size(); i++) {
        List<String> partitionInfoParts = Splitter.on("=").omitEmptyStrings().trimResults().splitToList(pInfo.get(i));
        String partitionType = pType.get(i);
        if (partitionInfoParts.size() != 2) {
          throw new IllegalArgumentException(
              String.format("Partition details should be of the format partitionName=partitionValue. Recieved: %s",
                  pInfo.get(i)));
        }
        partitionDDLInfo.put(partitionInfoParts.get(0), partitionType);
        partitionDMLInfo.put(partitionInfoParts.get(0), partitionInfoParts.get(1));
      }
      if (partitionDDLInfo.size() > 0) {
        optionalPartitionsDDLInfo = Optional.of(partitionDDLInfo);
      }
      if (partitionDMLInfo.size() > 0) {
        optionalPartitionsDMLInfo = Optional.of(partitionDMLInfo);
      }
    }

    // Create DDL statement
    String createFlattenedTableDDL = HiveAvroORCQueryUtils
        .generateCreateTableDDL(flattenedSchema,
            orcTableName,
            orcTableLocation,
            Optional.of(conversionEntity.getHiveTable().getDbName()),
            optionalPartitionsDDLInfo,
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent());
    conversionEntity.getQueries().add(createFlattenedTableDDL);
    log.info("Create DDL: " + createFlattenedTableDDL);

    // Create DML statement
    String insertInORCTableDML = HiveAvroORCQueryUtils
        .generateTableMappingDML(outputSchema, flattenedSchema, avroTableName, orcTableName,
            Optional.of(conversionEntity.getHiveTable().getDbName()),
            Optional.of(conversionEntity.getHiveTable().getDbName()),
            optionalPartitionsDMLInfo,
            Optional.<Boolean>absent(), Optional.<Boolean>absent());
    conversionEntity.getQueries().add(insertInORCTableDML);
    log.info("Conversion DML: " + insertInORCTableDML);

    log.info("Conversion Query " + conversionEntity.getQueries());
    return new SingleRecordIterable<>(conversionEntity);
  }

}
