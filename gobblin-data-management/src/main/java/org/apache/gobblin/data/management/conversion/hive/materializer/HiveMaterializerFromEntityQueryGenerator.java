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

package org.apache.gobblin.data.management.conversion.hive.materializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import org.apache.gobblin.data.management.conversion.hive.entities.HiveProcessingEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;


/**
 * An abstract {@link org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator} containing common methods
 * for materializing existing tables / partitions / views.
 */
@Slf4j
public abstract class HiveMaterializerFromEntityQueryGenerator extends HiveMaterializerQueryGenerator {

  protected final String inputDbName;
  protected final String inputTableName;

  protected final List<String> sourceDataPathIdentifier;
  protected final String stagingDataPartitionDirName;
  protected final String stagingDataPartitionLocation;
  protected final Map<String, String> partitionsDDLInfo;
  protected final Map<String, String> partitionsDMLInfo;
  protected final HiveProcessingEntity conversionEntity;
  protected final Table sourceTable;
  protected final boolean supportTargetPartitioning;

  public HiveMaterializerFromEntityQueryGenerator(WorkUnitState workUnitState, boolean supportTargetPartitioning)
      throws IOException {
    super(workUnitState);


    try {
      this.conversionEntity = getConversionEntity(this.workUnit);
    } catch (TException | HiveException ex) {
      throw new IOException(ex);
    }
    this.sourceTable = this.conversionEntity.getTable();
    this.inputDbName = this.sourceTable.getDbName();
    this.inputTableName = this.sourceTable.getTableName();

    this.sourceDataPathIdentifier = this.outputTableMetadata.getSourceDataPathIdentifier();
    this.stagingDataPartitionDirName = HiveConverterUtils.getStagingDataPartitionDirName(conversionEntity, sourceDataPathIdentifier);
    this.stagingDataPartitionLocation = stagingDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
    this.partitionsDDLInfo = Maps.newHashMap();
    this.partitionsDMLInfo = Maps.newHashMap();
    HiveConverterUtils.populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);
    this.supportTargetPartitioning = supportTargetPartitioning;
  }

  private HiveProcessingEntity getConversionEntity(HiveWorkUnit hiveWorkUnit) throws IOException, TException,
                                                                                     HiveException {

    try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {

      HiveDataset dataset = hiveWorkUnit.getHiveDataset();
      HiveDatasetFinder.DbAndTable dbAndTable = dataset.getDbAndTable();

      Table table = new Table(client.get().getTable(dbAndTable.getDb(), dbAndTable.getTable()));

      Partition partition = null;
      if (hiveWorkUnit.getPartitionName().isPresent()) {
        partition = new Partition(table, client.get()
            .getPartition(dbAndTable.getDb(), dbAndTable.getTable(), hiveWorkUnit.getPartitionName().get()));
      }
      return new HiveProcessingEntity(dataset, table, Optional.fromNullable(partition));
    }
  }

  /**
   * Returns a QueryBasedHivePublishEntity which includes publish level queries and cleanup commands.
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
        HiveConverterUtils.generateCreateDuplicateTableDDL(outputDatabaseName, stagingTableName, outputTableName,
            outputDataLocation, Optional.of(outputDatabaseName));
    publishQueries.add(createFinalTableDDL);
    log.debug("Create final table DDL:\n" + createFinalTableDDL);

    if (!this.supportTargetPartitioning || partitionsDDLInfo.size() == 0) {
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
              conversionEntity.getPartition().get().getName());
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

      publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName, outputTableName,
          AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(conversionEntity)));
    }

    log.info("Publish partition entity: " + publishEntity);
    return publishEntity;
  }
}
