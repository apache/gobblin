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

import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

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
  private static final String ORC_TABLE_ALTERNATE_DATABASE = "orc.table.alternate.database";
  private static final String ORC_TABLE_FLATTEN_SCHEMA = "orc.table.flatten.schema";

  private static AvroFlattener AVRO_FLATTENER = new AvroFlattener();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Populate the avro to orc conversion queries. The Queries will be added to {@link QueryBasedHiveConversionEntity#getQueries()}
   */
  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema inputAvroSchema,
      QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit) throws DataConversionException {
    Preconditions.checkNotNull(inputAvroSchema, "Avro schema must not be null");
    Preconditions.checkNotNull(conversionEntity, "Conversion entity must not be null");
    Preconditions.checkNotNull(workUnit, "Workunit state must not be null");
    Preconditions.checkNotNull(conversionEntity.getHiveTable(), "Hive table within conversion entity must not be null");

    // Convert schema if required
    Schema convertedOrcSchema;
    boolean isOrcTableFlattened = shouldFlattenSchema(workUnit);
    if (isOrcTableFlattened) {
      convertedOrcSchema = AVRO_FLATTENER.flatten(inputAvroSchema, false);
    } else {
      convertedOrcSchema = inputAvroSchema;
    }

    // Create output table if not exists
    // ORC Hive tables are named as   : {avro_table_name}_orc
    // ORC Hive tables use location as: {avro_table_location}_orc
    // TODO: Add cluster by info (from config)
    // TODO: Add sort by info (from config)
    // TODO: Add num of buckets (from config)

    // Avro table name and location
    String avroTableName = conversionEntity.getHiveTable().getTableName();
    String avroDataLocation =
        conversionEntity.getHivePartition().isPresent() ? conversionEntity.getHivePartition().get().getLocation()
            : conversionEntity.getHiveTable().getTTable().getSd().getLocation();

    // ORC table name and location
    // TODO: Define naming convention and pull it from config / topology
    String orcTableName = avroTableName + "_orc";
    // TODO: Define naming convention and pull it from config / topology
    Optional<String> orcDataLocationPostfix = Optional.absent();
    if (!isOrcTableFlattened) {
      orcTableName = orcTableName + "_nested";
      orcDataLocationPostfix = Optional.of("_nested");
    }
    String orcTableDatabase = getOrcTableDatabase(workUnit, conversionEntity);
    String orcDataLocation = getOrcDataLocation(workUnit, avroDataLocation, orcTableName, orcDataLocationPostfix);

    // Populate optional partition info
    Map<String, String> partitionsDDLInfo = Maps.newHashMap();
    Map<String, String> partitionsDMLInfo = Maps.newHashMap();
    populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);

    // Create DDL statement
    String createTargetTableDDL = HiveAvroORCQueryUtils
        .generateCreateTableDDL(convertedOrcSchema,
            orcTableName,
            orcDataLocation,
            Optional.of(orcTableDatabase),
            Optional.of(partitionsDDLInfo),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent());
    conversionEntity.getQueries().add(createTargetTableDDL);
    log.info("Create DDL: " + createTargetTableDDL);

    // Create DML statement
    String insertInORCTableDML = HiveAvroORCQueryUtils
        .generateTableMappingDML(inputAvroSchema, convertedOrcSchema, avroTableName, orcTableName,
            Optional.of(conversionEntity.getHiveTable().getDbName()),
            Optional.of(orcTableDatabase),
            Optional.of(partitionsDMLInfo),
            Optional.<Boolean>absent(), Optional.<Boolean>absent());
    conversionEntity.getQueries().add(insertInORCTableDML);
    log.info("Conversion DML: " + insertInORCTableDML);

    log.info("Conversion Query " + conversionEntity.getQueries());
    return new SingleRecordIterable<>(conversionEntity);
  }

  private boolean shouldFlattenSchema(WorkUnitState workUnit) {
    return workUnit.getJobState().getPropAsBoolean(ORC_TABLE_FLATTEN_SCHEMA, true);
  }

  private String getOrcTableDatabase(WorkUnitState workUnit, QueryBasedHiveConversionEntity conversionEntity) {
    String orcTableAlternateDB = workUnit.getJobState().getProp(ORC_TABLE_ALTERNATE_DATABASE);
    return StringUtils.isNotBlank(orcTableAlternateDB) ? orcTableAlternateDB :
        conversionEntity.getHiveTable().getDbName();
  }

  private String getOrcDataLocation(WorkUnitState workUnit, String avroDataLocation, String orcTableName,
      Optional<String> postfix) {
    String orcDataLocation;

    // By default ORC table creates a new directory where Avro data resides with _orc postfix, but this can be
    // .. overridden by specifying this property
    String orcTableAlternateLocation = workUnit.getJobState().getProp(ORC_TABLE_ALTERNATE_LOCATION);
    if (StringUtils.isNotBlank(orcTableAlternateLocation)) {
      orcDataLocation = new Path(orcTableAlternateLocation, orcTableName).toString();
    } else {
      orcDataLocation = StringUtils.removeEnd(avroDataLocation, Path.SEPARATOR) + "_orc";
    }

    if (postfix.isPresent()) {
      orcDataLocation += postfix.get();
    }

    // Each job execution further writes to a sub-directory within ORC data directory to support stagin use-case
    // .. ie for atomic swap
    orcDataLocation += Path.SEPARATOR + workUnit.getJobState().getId();

    return orcDataLocation;
  }

  private void populatePartitionInfo(QueryBasedHiveConversionEntity conversionEntity,
      Map<String, String> partitionsDDLInfo,
      Map<String, String> partitionsDMLInfo) {
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
        partitionsDDLInfo.put(partitionInfoParts.get(0), partitionType);
        partitionsDMLInfo.put(partitionInfoParts.get(0), partitionInfoParts.get(1));
      }
    }
  }
}
