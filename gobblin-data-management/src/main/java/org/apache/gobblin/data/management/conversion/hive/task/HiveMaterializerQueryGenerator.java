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

package org.apache.gobblin.data.management.conversion.hive.task;

import java.util.Map;
import java.util.List;

import org.apache.gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.avro.AvroSchemaManager;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;
import org.apache.gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.AutoReturnableObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * A simple query generator for {@link HiveMaterializer}.
 */
public class HiveMaterializerQueryGenerator implements QueryGenerator {
  private final FileSystem fs;
  private final ConvertibleHiveDataset.ConversionConfig conversionConfig;
  private final ConvertibleHiveDataset hiveDataset;
  private final String inputDbName;
  private final String inputTableName;
  private final String outputDatabaseName;
  private final String outputTableName;
  private final String outputDataLocation;
  private final String stagingTableName;
  private final String stagingDataLocation;
  private final List<String> sourceDataPathIdentifier;
  private final String stagingDataPartitionDirName;
  private final String stagingDataPartitionLocation;
  private final Map<String, String> partitionsDDLInfo;
  private final Map<String, String> partitionsDMLInfo;
  private final Optional<Table> destinationTableMeta;
  private final HiveWorkUnit workUnit;
  private final HiveMetastoreClientPool pool;
  private final QueryBasedHiveConversionEntity conversionEntity;
  private final WorkUnitState workUnitState;

  public HiveMaterializerQueryGenerator(WorkUnitState workUnitState) throws Exception {
    this.workUnitState = workUnitState;
    this.workUnit = new HiveWorkUnit(workUnitState.getWorkunit());
    this.hiveDataset = (ConvertibleHiveDataset) workUnit.getHiveDataset();
    this.inputDbName = hiveDataset.getDbAndTable().getDb();
    this.inputTableName = hiveDataset.getDbAndTable().getTable();
    this.fs = HiveSource.getSourceFs(workUnitState);
    this.conversionConfig = hiveDataset.getConversionConfigForFormat("sameAsSource").get();
    this.outputDatabaseName = conversionConfig.getDestinationDbName();
    this.outputTableName = conversionConfig.getDestinationTableName();
    this.outputDataLocation = HiveConverterUtils.getOutputDataLocation(conversionConfig.getDestinationDataPath());
    this.stagingTableName = HiveConverterUtils.getStagingTableName(conversionConfig.getDestinationStagingTableName());
    this.stagingDataLocation = HiveConverterUtils.getStagingDataLocation(conversionConfig.getDestinationDataPath(), stagingTableName);
    this.sourceDataPathIdentifier = conversionConfig.getSourceDataPathIdentifier();
    this.pool = HiveMetastoreClientPool.get(workUnitState.getJobState().getProperties(),
        Optional.fromNullable(workUnitState.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
    this.conversionEntity = getConversionEntity();
    this.stagingDataPartitionDirName = HiveConverterUtils.getStagingDataPartitionDirName(conversionEntity, sourceDataPathIdentifier);
    this.stagingDataPartitionLocation = stagingDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
    this.partitionsDDLInfo = Maps.newHashMap();
    this.partitionsDMLInfo = Maps.newHashMap();
    HiveConverterUtils.populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);
    this.destinationTableMeta = HiveConverterUtils.getDestinationTableMeta(outputDatabaseName,
        outputTableName, workUnitState.getProperties()).getLeft();
  }

  /**
   * Returns hive queries to be run as a part of a hive task.
   * This does not include publish queries.
   * @return
   */
  @Override
  public List<String> generateQueries() {

    List<String> hiveQueries = Lists.newArrayList();

    Preconditions.checkNotNull(this.workUnit, "Workunit must not be null");
    EventWorkunitUtils.setBeginDDLBuildTimeMetadata(this.workUnit, System.currentTimeMillis());

    HiveConverterUtils.createStagingDirectory(fs, conversionConfig.getDestinationDataPath(),
        conversionEntity, this.workUnitState);

    // Create DDL statement for table
    String createStagingTableDDL =
        HiveConverterUtils.generateCreateDuplicateTableDDL(
            inputDbName,
            inputTableName,
            stagingTableName,
            stagingDataLocation,
            Optional.of(outputDatabaseName));
    hiveQueries.add(createStagingTableDDL);
    log.debug("Create staging table DDL:\n" + createStagingTableDDL);

    /*
     * Setting partition mode to 'nonstrict' is needed to improve readability of the code.
     * If we do not set dynamic partition mode to nonstrict, we will have to write partition values also,
     * and because hive considers partition as a virtual column, we also have to write each of the column
     * name in the query (in place of *) to match source and target columns.
     */
    hiveQueries.add("SET hive.exec.dynamic.partition.mode=nonstrict");

    String insertInStagingTableDML =
        HiveConverterUtils
            .generateTableCopy(
                inputTableName,
                stagingTableName,
                conversionEntity.getHiveTable().getDbName(),
                outputDatabaseName,
                Optional.of(partitionsDMLInfo));
    hiveQueries.add(insertInStagingTableDML);
    log.debug("Conversion staging DML: " + insertInStagingTableDML);

    log.info("Conversion Queries {}\n",  hiveQueries);

    EventWorkunitUtils.setEndDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());
    return hiveQueries;
  }

  /**
   * Retuens a QueryBasedHivePublishEntity which includes publish level queries and cleanup commands.
   * @return QueryBasedHivePublishEntity
   * @throws DataConversionException
   */
  public QueryBasedHivePublishEntity generatePublishQueries() throws DataConversionException {

    QueryBasedHivePublishEntity publishEntity = new QueryBasedHivePublishEntity();
    List<String> publishQueries = publishEntity.getPublishQueries();
    Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
    List<String> cleanupQueries = publishEntity.getCleanupQueries();
    List<String> cleanupDirectories = publishEntity.getCleanupDirectories();

    String createFinalTableDDL =
        HiveConverterUtils.generateCreateDuplicateTableDDL(inputDbName, inputTableName, outputTableName,
            outputDataLocation, Optional.of(outputDatabaseName));
    publishQueries.add(createFinalTableDDL);
    log.debug("Create final table DDL:\n" + createFinalTableDDL);

    if (partitionsDDLInfo.size() == 0) {
      log.debug("Snapshot directory to move: " + stagingDataLocation + " to: " + outputDataLocation);
      publishDirectories.put(stagingDataLocation, outputDataLocation);

      String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(outputDatabaseName, stagingTableName);

      log.debug("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      log.debug("Staging table directory to delete: " + stagingDataLocation);
      cleanupDirectories.add(stagingDataLocation);
    } else {
      String finalDataPartitionLocation = outputDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
      Optional<Path> destPartitionLocation =
            HiveConverterUtils.getDestinationPartitionLocation(destinationTableMeta, this.workUnitState,
                conversionEntity.getHivePartition().get().getName());
        finalDataPartitionLocation = HiveConverterUtils.updatePartitionLocation(finalDataPartitionLocation, this.workUnitState,
            destPartitionLocation);

      log.debug("Partition directory to move: " + stagingDataPartitionLocation + " to: " + finalDataPartitionLocation);
      publishDirectories.put(stagingDataPartitionLocation, finalDataPartitionLocation);
      List<String> dropPartitionsDDL =
          HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName, outputTableName, partitionsDMLInfo);
      log.debug("Drop partitions if exist in final table: " + dropPartitionsDDL);
      publishQueries.addAll(dropPartitionsDDL);
      List<String> createFinalPartitionDDL =
          HiveAvroORCQueryGenerator.generateCreatePartitionDDL(outputDatabaseName, outputTableName,
              finalDataPartitionLocation, partitionsDMLInfo, Optional.<String>absent());

      log.debug("Create final partition DDL: " + createFinalPartitionDDL);
      publishQueries.addAll(createFinalPartitionDDL);

      String dropStagingTableDDL =
          HiveAvroORCQueryGenerator.generateDropTableDDL(outputDatabaseName, stagingTableName);

      log.debug("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      log.debug("Staging table directory to delete: " + stagingDataLocation);
      cleanupDirectories.add(stagingDataLocation);
    }

    publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName, outputTableName,
        AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(conversionEntity)));

    log.info("Publish partition entity: " + publishEntity);
    return publishEntity;
  }

  private QueryBasedHiveConversionEntity getConversionEntity() throws Exception {


    try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {

      Table table = client.get().getTable(this.inputDbName, this.inputTableName);

      SchemaAwareHiveTable schemaAwareHiveTable = new SchemaAwareHiveTable(table, AvroSchemaManager.getSchemaFromUrl(workUnit.getTableSchemaUrl(), fs));

      SchemaAwareHivePartition schemaAwareHivePartition = null;

      if (workUnit.getPartitionName().isPresent() && workUnit.getPartitionSchemaUrl().isPresent()) {
        org.apache.hadoop.hive.metastore.api.Partition
            partition = client.get().getPartition(this.inputDbName, this.inputTableName, workUnit.getPartitionName().get());
        schemaAwareHivePartition =
            new SchemaAwareHivePartition(table, partition, AvroSchemaManager.getSchemaFromUrl(workUnit.getPartitionSchemaUrl().get(), fs));
      }
      return new QueryBasedHiveConversionEntity(this.hiveDataset, schemaAwareHiveTable, Optional.fromNullable(schemaAwareHivePartition));
    }
  }
}