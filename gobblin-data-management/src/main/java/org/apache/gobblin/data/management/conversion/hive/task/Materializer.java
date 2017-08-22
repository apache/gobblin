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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.avro.AvroSchemaManager;
import org.apache.gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;
import org.apache.gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import org.apache.gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarkerFactory;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.HiveJdbcConnector;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;


@Slf4j
public class Materializer extends HiveTask {
  ConversionConfig conversionConfig;
  ConvertibleHiveDataset hiveDataset;

  // TODO some of the stuff should go to the super class
  protected HiveMetastoreClientPool pool;
  QueryBasedHiveConversionEntity conversionEntity;
  FileSystem fs;
  String inputDbName;
  String inputTableName;
  String outputDatabaseName;
  String outputTableName;
  String outputDataLocation;
  String outputStagingTableName;
  String outputStagingDataLocation;
  List<String> sourceDataPathIdentifier;
  String stagingDataPartitionDirName;
  String stagingDataPartitionLocation;
  Map<String, String> partitionsDDLInfo;
  Map<String, String> partitionsDMLInfo;
  Optional<Table> destinationTableMeta;

  public Materializer(TaskContext taskContext) throws Exception {
    super(taskContext);
    this.queryBasedHivePublishEntity = new QueryBasedHivePublishEntity();
    if (Boolean.valueOf(this.workUnitState.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      log.info("In Materializer, Ignoring Watermark workunit for {}", this.workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY));
      return;
    }
    if (!(workUnit.getHiveDataset() instanceof ConvertibleHiveDataset)) {
      throw new IllegalStateException("HiveConvertExtractor is only compatible with ConvertibleHiveDataset");
    }
    this.hiveDataset = (ConvertibleHiveDataset) workUnit.getHiveDataset();
    this.inputDbName = hiveDataset.getDbAndTable().getDb();
    this.inputTableName = hiveDataset.getDbAndTable().getTable();
    this.conversionConfig = hiveDataset.getConversionConfigForFormat("sameAsSource").get();
    this.outputDatabaseName = conversionConfig.getDestinationDbName();
    this.outputTableName = conversionConfig.getDestinationTableName();
    this.outputDataLocation = HiveConverterUtils.getOutputDataLocation(conversionConfig.getDestinationDataPath());
    this.outputStagingTableName = HiveConverterUtils.getStagingTableName(conversionConfig.getDestinationStagingTableName());
    this.outputStagingDataLocation = HiveConverterUtils.getStagingDataLocation(conversionConfig.getDestinationDataPath(), outputStagingTableName);
    this.sourceDataPathIdentifier = conversionConfig.getSourceDataPathIdentifier();
    this.conversionEntity = getConversionEntity();
    this.stagingDataPartitionDirName = HiveConverterUtils.getStagingDataPartitionDirName(conversionEntity, sourceDataPathIdentifier);
    this.stagingDataPartitionLocation = outputStagingDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
    this.partitionsDDLInfo = Maps.newHashMap();
    this.partitionsDMLInfo = Maps.newHashMap();
    HiveConverterUtils.populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);
    this.hiveJdbcConnector = HiveJdbcConnector.newConnectorWithProps(this.workUnitState.getProperties());
    this.destinationTableMeta = HiveConverterUtils.getDestinationTableMeta(outputDatabaseName,
        outputTableName, this.workUnitState).getLeft();
  }

  private static FileSystem getSourceFs(State state) throws IOException {
    if (state.contains(HiveSource.HIVE_SOURCE_FS_URI)) {
      return FileSystem.get(URI.create(state.getProp(HiveSource.HIVE_SOURCE_FS_URI)), HadoopUtils.getConfFromState(state));
    }
    return FileSystem.get(HadoopUtils.getConfFromState(state));
  }

  private QueryBasedHiveConversionEntity getConversionEntity() throws Exception {
      fs = getSourceFs(this.workUnitState);
      this.pool = HiveMetastoreClientPool.get(this.workUnitState.getJobState().getProperties(),
          Optional.fromNullable(this.workUnitState.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));

      try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {
        Table table = client.get().getTable(this.inputDbName, this.inputTableName);

        SchemaAwareHiveTable schemaAwareHiveTable = new SchemaAwareHiveTable(table, AvroSchemaManager.getSchemaFromUrl(this.workUnit.getTableSchemaUrl(), fs));

        SchemaAwareHivePartition schemaAwareHivePartition = null;

        if (this.workUnit.getPartitionName().isPresent() && this.workUnit.getPartitionSchemaUrl().isPresent()) {

          org.apache.hadoop.hive.metastore.api.Partition
              partition = client.get().getPartition(this.inputDbName, this.inputTableName, this.workUnit.getPartitionName().get());
          schemaAwareHivePartition =
              new SchemaAwareHivePartition(table, partition, AvroSchemaManager.getSchemaFromUrl(this.workUnit.getPartitionSchemaUrl().get(), fs));
        }

        return new QueryBasedHiveConversionEntity(this.hiveDataset, schemaAwareHiveTable, Optional.fromNullable(schemaAwareHivePartition));
      }
  }

  @Override
  public void generateHiveQueries() throws Exception {
    // real work of generating queries
    Preconditions.checkNotNull(this.workUnitState.getWorkunit(), "Workunit state must not be null");
    EventWorkunitUtils.setBeginDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());

    HiveConverterUtils.createStagingDirectory(this.fs, conversionConfig.getDestinationDataPath(),
        conversionEntity, this.workUnitState);

    // Create DDL statement for table
    String createStagingTableDDL =
        HiveConverterUtils.generateCreateDuplicateTableDDL(
            inputDbName,
            inputTableName,
            outputStagingTableName,
            outputStagingDataLocation,
            Optional.of(outputDatabaseName));
    conversionEntity.getQueries().add(createStagingTableDDL);
    log.debug("Create staging table DDL:\n" + createStagingTableDDL);

    // Create DDL statement for partition
    if (partitionsDMLInfo.size() > 0) {
      List<String> createStagingPartitionDDL =
          HiveAvroORCQueryGenerator.generateCreatePartitionDDL(outputDatabaseName,
              outputStagingTableName,
              stagingDataPartitionLocation,
              partitionsDMLInfo);

      conversionEntity.getQueries().addAll(createStagingPartitionDDL);
      log.debug("Create staging partition DDL: " + createStagingPartitionDDL);
    }

    conversionEntity.getQueries().add("SET hive.exec.dynamic.partition.mode=nonstrict");

    String insertInStagingTableDML =
        HiveConverterUtils
            .generateTableCopy(conversionEntity.getHiveTable().getAvroSchema(),
                inputTableName,
                outputStagingTableName,
                Optional.of(conversionEntity.getHiveTable().getDbName()),
                Optional.of(outputDatabaseName),
                Optional.of(partitionsDMLInfo),
                Optional.<Boolean>absent(),
                Optional.<Boolean>absent());
    conversionEntity.getQueries().add(insertInStagingTableDML);
    log.debug("Conversion staging DML: " + insertInStagingTableDML);

    log.info("Conversion Query " + conversionEntity.getQueries());

    EventWorkunitUtils.setEndDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());
  }

  @Override
  public void generatePublishQueries() throws Exception {
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
      HiveConverterUtils.cleanUpNonPartitionedTable(publishDirectories, cleanupQueries, outputStagingDataLocation,
          cleanupDirectories, outputDataLocation, outputDatabaseName, outputStagingTableName);
    } else {
      String finalDataPartitionLocation = outputDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
      Optional<Path> destPartitionLocation = HiveConverterUtils.getDestinationPartitionLocation(destinationTableMeta, this.workUnitState,
          conversionEntity.getHivePartition().get().getName());
      finalDataPartitionLocation =
          HiveConverterUtils.updatePartitionLocation(finalDataPartitionLocation, this.workUnitState,
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
          HiveAvroORCQueryGenerator.generateDropTableDDL(outputDatabaseName, outputStagingTableName);

      log.info("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      log.info("Staging table directory to delete: " + outputStagingDataLocation);
      cleanupDirectories.add(outputStagingDataLocation);
    }

    publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName, outputTableName,
        AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(conversionEntity)));

    log.info("Publish partition entity: " + publishEntity);
  }

  @Override
  public void run() {
    try {
      generateHiveQueries();
      executeQueries();
      //commit();
    } catch (Exception e) {
      log.error("Exception in Materializer generate/execute HiveQueries ", e);
    }

  }

  @Override
  public void commit() {
    try {
      generatePublishQueries();
      executePublishQueries();
    } catch (Exception e) {
      log.error("Exception in Materializer generate/execute publish queries ", e);
    }
    super.commit();
  }

  private void executePublishQueries() {
    Set<String> cleanUpQueries = Sets.newLinkedHashSet();
    Set<String> publishQueries = Sets.newLinkedHashSet();
    List<String> directoriesToDelete = Lists.newArrayList();

    try {
      // Add cleanup commands - to be executed later
      if (publishEntity.getCleanupQueries() != null) {
        cleanUpQueries.addAll(publishEntity.getCleanupQueries());
      }

      if (publishEntity.getCleanupDirectories() != null) {
        directoriesToDelete.addAll(publishEntity.getCleanupDirectories());
      }

      if (publishEntity.getPublishDirectories() != null) {
        // Publish snapshot / partition directories
        Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
        try {
          for (Map.Entry<String, String> publishDir : publishDirectories.entrySet()) {
            HiveConverterUtils.moveDirectory(this.fs, publishDir.getKey(), publishDir.getValue());
          }
        } catch (Exception e) {
          log.error("error in move dir");
        }
      }

      if (publishEntity.getPublishQueries() != null) {
        publishQueries.addAll(publishEntity.getPublishQueries());
      }

      WorkUnitState wus = this.workUnitState;

      // Actual publish: Register snapshot / partition
      executeQueries(Lists.newArrayList(publishQueries));

      wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);

      HiveSourceWatermarker watermarker = GobblinConstructorUtils.invokeConstructor(
          HiveSourceWatermarkerFactory.class, wus.getProp(HiveSource.HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY,
              HiveSource.DEFAULT_HIVE_SOURCE_WATERMARKER_FACTORY_CLASS)).createFromState(wus);

       watermarker.setActualHighWatermark(wus);
    } finally {

      try {
        executeQueries(Lists.newArrayList(cleanUpQueries));
      } catch (Exception e) {
        log.error("Failed to cleanup staging entities in Hive metastore.", e);
      }
      try {
        HiveConverterUtils.deleteDirectories(this.fs, directoriesToDelete);
      } catch (Exception e) {
        log.error("Failed to cleanup staging directories.", e);
      }
    }
  }

  private void executeQueries(List<String> queries) {
    if (null == queries || queries.size() == 0) {
      return;
    }
    try {
      this.hiveJdbcConnector.executeStatements(queries.toArray(new String[queries.size()]));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void executeQueries() throws IOException {
    //Destination destination = Destination.of(this.taskContext.getDestinationType(0, 0), this.workUnitState);
    List<String> conversionQueries = conversionEntity.getQueries();
    try {
      EventWorkunitUtils.setBeginConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
      this.hiveJdbcConnector.executeStatements(conversionQueries.toArray(new String[conversionQueries.size()]));
      // Adding properties for preserving partitionParams:
      addPropsForPublisher(conversionEntity);
      EventWorkunitUtils.setEndConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("Failed to execute queries: ");
      for (String conversionQuery : conversionQueries) {
        log.warn("Conversion query attempted by Hive Query writer: " + conversionQuery);
      }
      throw new IOException(e);
    }

  }
  private void addPropsForPublisher(QueryBasedHiveConversionEntity hiveConversionEntity) {
    if (!hiveConversionEntity.getHivePartition().isPresent()) {
      return;
    }
    String AT_CHAR = "@";
    ConvertibleHiveDataset convertibleHiveDataset = hiveConversionEntity.getConvertibleHiveDataset();
    for (String format : convertibleHiveDataset.getDestFormats()) {
      Optional<ConversionConfig> conversionConfigForFormat =
          convertibleHiveDataset.getConversionConfigForFormat(format);
      if (!conversionConfigForFormat.isPresent()) {
        continue;
      }
      SchemaAwareHivePartition sourcePartition = hiveConversionEntity.getHivePartition().get();

      // Get complete source partition name dbName@tableName@partitionName
      String completeSourcePartitionName = StringUtils.join(Arrays
          .asList(sourcePartition.getTable().getDbName(), sourcePartition.getTable().getTableName(),
              sourcePartition.getName()), AT_CHAR);
      ConvertibleHiveDataset.ConversionConfig config = conversionConfigForFormat.get();

      // Get complete destination partition name dbName@tableName@partitionName
      String completeDestPartitionName = StringUtils.join(
          Arrays.asList(config.getDestinationDbName(), config.getDestinationTableName(), sourcePartition.getName()),
          AT_CHAR);

      workUnit.setProp(HiveConvertPublisher.COMPLETE_SOURCE_PARTITION_NAME, completeSourcePartitionName);
      workUnit.setProp(HiveConvertPublisher.COMPLETE_DEST_PARTITION_NAME, completeDestPartitionName);
    }
  }

}
