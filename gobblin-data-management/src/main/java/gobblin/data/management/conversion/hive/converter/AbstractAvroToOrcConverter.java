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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.AutoReturnableObject;


/**
 * Builds the Hive avro to ORC conversion query. The record type for this converter is {@link QueryBasedHiveConversionEntity}. A {@link QueryBasedHiveConversionEntity}
 * can be a hive table or a hive partition.
 * <p>
 * Concrete subclasses define the semantics of Avro to ORC conversion for a specific ORC format by providing {@link ConversionConfig}s.
 * </p>
 */
@Slf4j
public abstract class AbstractAvroToOrcConverter extends Converter<Schema, Schema, QueryBasedHiveConversionEntity, QueryBasedHiveConversionEntity> {

  /**
   * Supported destination ORC formats
   */
  protected enum OrcFormats {
    FLATTENED_ORC("flattenedOrc"),
    NESTED_ORC("nestedOrc");
    private final String configPrefix;

    OrcFormats(String configPrefix) {
      this.configPrefix = configPrefix;
    }

    public String getConfigPrefix() {
      return this.configPrefix;
    }
  }

  /**
   * The dataset being converted.
   */
  protected ConvertibleHiveDataset hiveDataset;

  /**
   * Subclasses can convert the {@link Schema} if required.
   *
   * {@inheritDoc}
   * @see gobblin.converter.Converter#convertSchema(java.lang.Object, gobblin.configuration.WorkUnitState)
   */
  @Override
  public abstract Schema convertSchema(Schema inputSchema, WorkUnitState workUnit);

  /**
   * <p>
   * This method is called by {@link AbstractAvroToOrcConverter#convertRecord(Schema, QueryBasedHiveConversionEntity, WorkUnitState)} before building the
   * conversion query. Subclasses can find out if conversion is enabled for their format by calling
   * {@link ConvertibleHiveDataset#getConversionConfigForFormat(String)} on the <code>hiveDataset</code>.<br>
   * Available ORC formats are defined by the enum {@link OrcFormats}
   * </p>
   * <p>
   * If this method returns false, no Avro to to ORC conversion queries will be built for the ORC format.
   * </p>
   * @return true if conversion is required. false otherwise
   */
  protected abstract boolean hasConversionConfig();

  /**
   * Get the {@link ConversionConfig} required for building the Avro to ORC conversion query
   * @return
   */
  protected abstract ConversionConfig getConversionConfig();

  /**
   * Populate the avro to orc conversion queries. The Queries will be added to {@link QueryBasedHiveConversionEntity#getQueries()}
   */
  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema outputAvroSchema, QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit)
      throws DataConversionException {

    Preconditions.checkNotNull(outputAvroSchema, "Avro schema must not be null");
    Preconditions.checkNotNull(conversionEntity, "Conversion entity must not be null");
    Preconditions.checkNotNull(workUnit, "Workunit state must not be null");
    Preconditions.checkNotNull(conversionEntity.getHiveTable(), "Hive table within conversion entity must not be null");

    this.hiveDataset = conversionEntity.getConvertibleHiveDataset();

    if (!hasConversionConfig()) {
      return new SingleRecordIterable<>(conversionEntity);
    }

    // Avro table name and location
    String avroTableName = conversionEntity.getHiveTable().getTableName();

    // ORC table name and location
    String orcTableName = getConversionConfig().getDestinationTableName();
    String orcStagingTableName = getConversionConfig().getDestinationStagingTableName();
    String orcTableDatabase = getConversionConfig().getDestinationDbName();
    String orcDataLocation = getOrcDataLocation(workUnit);
    boolean isEvolutionEnabled = getConversionConfig().isEvolutionEnabled();
    Pair<Optional<Table>, Optional<List<Partition>>> destinationMeta = getDestinationTableMeta(orcTableDatabase,
        orcTableName, workUnit);
    Optional<Table> destinationTableMeta = destinationMeta.getLeft();
    Optional<List<Partition>> destinationPartitionsMeta = destinationMeta.getRight();

    // Optional
    Optional<List<String>> clusterBy =
        getConversionConfig().getClusterBy().isEmpty()
            ? Optional.<List<String>> absent()
            : Optional.of(getConversionConfig().getClusterBy());
    Optional<Integer> numBuckets = getConversionConfig().getNumBuckets();
    Optional<Integer> rowLimit = getConversionConfig().getRowLimit();
    Optional<String> hiveVersion = getConversionConfig().getHiveVersion();

    // Populate optional partition info
    Map<String, String> partitionsDDLInfo = Maps.newHashMap();
    Map<String, String> partitionsDMLInfo = Maps.newHashMap();
    populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);

    // Set hive runtime properties
    for (Map.Entry<Object, Object> entry : getConversionConfig().getHiveRuntimeProperties().entrySet()) {
      conversionEntity.getQueries().add(String.format("SET %s=%s;", entry.getKey(), entry.getValue()));
    }

    // Create DDL statement
    Map<String, String> hiveColumns = new HashMap<String, String>();
    String createTargetTableDDL =
        HiveAvroORCQueryGenerator.generateCreateTableDDL(outputAvroSchema,
            orcStagingTableName,
            orcDataLocation,
            Optional.of(orcTableDatabase),
            Optional.of(partitionsDDLInfo),
            clusterBy,
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(),
            numBuckets,
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta,
            hiveColumns);
    conversionEntity.getQueries().add(createTargetTableDDL);
    log.debug("Create DDL: " + createTargetTableDDL);

    // Create DML statement
    String insertInORCTableDML =
        HiveAvroORCQueryGenerator
            .generateTableMappingDML(conversionEntity.getHiveTable().getAvroSchema(),
                outputAvroSchema,
                avroTableName,
                orcStagingTableName,
                Optional.of(conversionEntity.getHiveTable().getDbName()),
                Optional.of(orcTableDatabase),
                Optional.of(partitionsDMLInfo),
                Optional.<Boolean>absent(),
                Optional.<Boolean>absent(),
                isEvolutionEnabled,
                destinationTableMeta,
                rowLimit);
    conversionEntity.getQueries().add(insertInORCTableDML);
    log.debug("Conversion DML: " + insertInORCTableDML);

    // Addition to WUS for Staging publish:
    // A. Evolution turned on:
    //    1. If table does not exists: simply create it
    //    2. If table exists:
    //       2.1 Evolve table (alter table)
    //       2.2 Move partitions from staging to final table
    //       2.3 Drop staging table
    // B. Evolution turned off:
    //    1. If table does not exists: simply create it
    //    2. If table exists:
    //       2.1 Move partitions from staging to final table (staging is already based on un-evolved schema)
    //       2.2 Drop staging table
    // Note: The queries below also serve as compatibility check module before conversion, an incompatible
    //      .. schema throws a Runtime exeption, hence preventing further execution
    StringBuilder publishTableQueries = new StringBuilder();
    publishTableQueries.append(HiveAvroORCQueryGenerator
        .generateEvolutionDDL(orcStagingTableName,
            orcTableName,
            Optional.of(orcTableDatabase),
            Optional.of(orcTableDatabase),
            outputAvroSchema,
            isEvolutionEnabled,
            hiveColumns,
            destinationTableMeta)).append("\n");
    publishTableQueries.append(
        HiveAvroORCQueryGenerator.generatePublishTableDDL(orcStagingTableName,
            orcTableName,
            Optional.of(orcTableDatabase),
            Optional.of(orcTableDatabase),
            destinationTableMeta)).append("\n");
    HiveAvroORCQueryGenerator.serializePublishTableCommands(workUnit, publishTableQueries.toString());
    log.debug("Publish table queries: " + publishTableQueries);

    Optional<String> dirToDeleteBeforePartitionPublish = HiveAvroORCQueryGenerator
            .findDirToDeleteBeforePartitionPublish(conversionEntity.getHivePartition(), destinationPartitionsMeta);
    HiveAvroORCQueryGenerator.serializeDirToDeleteBeforePartitionPublish(workUnit, dirToDeleteBeforePartitionPublish);
    if (dirToDeleteBeforePartitionPublish.isPresent()) {
      log.info("Directory to delete before publish partition: " + dirToDeleteBeforePartitionPublish.get());
    } else {
      log.info("No pre-existing partition directory to delete before publish partition.");
    }

    StringBuilder publishPartitionQueries = new StringBuilder();
    publishPartitionQueries.append(
        HiveAvroORCQueryGenerator
            .generatePublishPartitionDDL(orcStagingTableName,
                orcTableName,
                Optional.of(orcTableDatabase),
                Optional.of(orcTableDatabase),
                partitionsDMLInfo,
                destinationTableMeta,
                hiveVersion)).append("\n");
    HiveAvroORCQueryGenerator.serializePublishPartitionCommands(workUnit, publishPartitionQueries.toString());
    log.debug("Publish partition queries: " + publishPartitionQueries);

    StringBuilder cleanupQueries = new StringBuilder();
    cleanupQueries.append(HiveAvroORCQueryGenerator.generateCleanupDDL(orcStagingTableName,
        Optional.of(orcTableDatabase))).append("\n");
    HiveAvroORCQueryGenerator.serializedCleanupCommands(workUnit, cleanupQueries.toString());
    log.debug("Cleanup queries: " + cleanupQueries);

    log.debug("Conversion Query " + conversionEntity.getQueries());
    return new SingleRecordIterable<>(conversionEntity);
  }


  private String getOrcDataLocation(WorkUnitState workUnit) {
    String orcDataLocation = getConversionConfig().getDestinationDataPath();

    // Each job execution further writes to a sub-directory within ORC data directory to support stagin use-case
    // .. ie for atomic swap
    if (StringUtils.isNotBlank(workUnit.getJobState().getId())) {
      orcDataLocation += Path.SEPARATOR + workUnit.getJobState().getId();
    }
    return orcDataLocation;
  }

  private void populatePartitionInfo(QueryBasedHiveConversionEntity conversionEntity, Map<String, String> partitionsDDLInfo,
      Map<String, String> partitionsDMLInfo) {
    String partitionsInfoString = null;
    String partitionsTypeString = null;

    if (conversionEntity.getHivePartition().isPresent()) {
      partitionsInfoString = conversionEntity.getHivePartition().get().getName();
      partitionsTypeString = conversionEntity.getHivePartition().get().getSchema().getProperty("partition_columns.types");
    }

    if (StringUtils.isNotBlank(partitionsInfoString) || StringUtils.isNotBlank(partitionsTypeString)) {
      if (StringUtils.isBlank(partitionsInfoString) || StringUtils.isBlank(partitionsTypeString)) {
        throw new IllegalArgumentException("Both partitions info and partitions must be present, if one is specified");
      }
      List<String> pInfo = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsInfoString);
      List<String> pType = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsTypeString);
      if (pInfo.size() != pType.size()) {
        throw new IllegalArgumentException("partitions info and partitions type list should of same size");
      }
      for (int i = 0; i < pInfo.size(); i++) {
        List<String> partitionInfoParts = Splitter.on("=").omitEmptyStrings().trimResults().splitToList(pInfo.get(i));
        String partitionType = pType.get(i);
        if (partitionInfoParts.size() != 2) {
          throw new IllegalArgumentException(
              String.format("Partition details should be of the format partitionName=partitionValue. Recieved: %s", pInfo.get(i)));
        }
        partitionsDDLInfo.put(partitionInfoParts.get(0), partitionType);
        partitionsDMLInfo.put(partitionInfoParts.get(0), partitionInfoParts.get(1));
      }
    }
  }

  private Pair<Optional<Table>, Optional<List<Partition>>> getDestinationTableMeta(String dbName,
      String tableName, WorkUnitState state)
      throws DataConversionException {

    Optional<Table> table = Optional.<Table>absent();
    Optional<List<Partition>> partitions = Optional.<List<Partition>>absent();

    try {
      HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(state.getJobState().getProperties(),
          Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        table = Optional.of(client.get().getTable(dbName, tableName));
        if (table.isPresent()) {
          org.apache.hadoop.hive.ql.metadata.Table qlTable = new org.apache.hadoop.hive.ql.metadata.Table(table.get());
          if (HiveUtils.isPartitioned(qlTable)) {
            partitions = Optional.of(HiveUtils.getPartitions(client.get(), qlTable, Optional.<String>absent()));
          }
        }
      }
    } catch (NoSuchObjectException e) {
      return ImmutablePair.of(table, partitions);
    } catch (IOException | TException e) {
      throw new DataConversionException("Could not fetch destination table metadata", e);
    }

    return ImmutablePair.of(table, partitions);
  }
}
