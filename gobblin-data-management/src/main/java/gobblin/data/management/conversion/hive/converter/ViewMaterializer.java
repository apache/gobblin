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
package gobblin.data.management.conversion.hive.converter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import gobblin.converter.DataConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
@Slf4j

/**
 * An Avro to ORC converter for avro to flattened ORC. {@link OrcFormats#FLATTENED_ORC}
 */
public class ViewMaterializer extends AbstractAvroToOrcConverter {

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) {
    return inputSchema;
  }

  @Override
  protected boolean hasConversionConfig() {
    return super.hiveDataset.getConversionConfigForFormat(OrcFormats.SAME_AS_SOURCE.getConfigPrefix()).isPresent();
  }

  @Override
  protected ConversionConfig getConversionConfig() {
    return super.hiveDataset.getConversionConfigForFormat(OrcFormats.SAME_AS_SOURCE.getConfigPrefix()).get();
  }

  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema outputSchema, QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit)
      throws DataConversionException {

    Preconditions.checkNotNull(outputSchema, "Schema must not be null");
    Preconditions.checkNotNull(conversionEntity, "Conversion entity must not be null");
    Preconditions.checkNotNull(workUnit, "Workunit state must not be null");
    Preconditions.checkNotNull(conversionEntity.getHiveTable(), "Hive table within conversion entity must not be null");

    EventWorkunitUtils.setBeginDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());

    this.hiveDataset = conversionEntity.getConvertibleHiveDataset();

    if (!hasConversionConfig()) {
      return new SingleRecordIterable<>(conversionEntity);
    }

    // Input db, table name and location
    String inputDbName = conversionEntity.getHiveTable().getDbName();
    String inputTableName = conversionEntity.getHiveTable().getTableName();

    // Output table name and location
    String outputTableName = getConversionConfig().getDestinationTableName();
    String outputStagingTableName = getOrcStagingTableName(getConversionConfig().getDestinationStagingTableName());
    String outputDatabaseName = getConversionConfig().getDestinationDbName();
    String outputDataLocation = getOrcDataLocation();
    String outputStagingDataLocation = getOrcStagingDataLocation(outputStagingTableName);

    // todo evolution not needed in case of materialization

    Pair<Optional<Table>, Optional<List<Partition>>> destinationMeta = getDestinationTableMeta(outputDatabaseName,
        outputTableName, workUnit);
    Optional<Table> destinationTableMeta = destinationMeta.getLeft();

    Optional<Integer> rowLimit = getConversionConfig().getRowLimit();

    List<String> sourceDataPathIdentifier = getConversionConfig().getSourceDataPathIdentifier();

    // Populate optional partition info
    Map<String, String> partitionsDDLInfo = Maps.newHashMap();
    Map<String, String> partitionsDMLInfo = Maps.newHashMap();
    populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);

    createStagingDirectory(conversionEntity, workUnit);

    // Create DDL statement for table
    String createStagingTableDDL =
        HiveAvroORCQueryGenerator.generateCreateDuplicateTableDDL(outputSchema,
            inputDbName,
            inputTableName,
            outputStagingTableName,
            outputStagingDataLocation,
            Optional.of(outputDatabaseName));
    conversionEntity.getQueries().add(createStagingTableDDL);
    log.info("Create staging table DDL:\n" + createStagingTableDDL);

    // Create DDL statement for partition
    String stagingDataPartitionDirName = getOrcStagingDataPartitionDirName(conversionEntity, sourceDataPathIdentifier);
    String stagingDataPartitionLocation = outputStagingDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
    if (partitionsDMLInfo.size() > 0) {
      List<String> createStagingPartitionDDL =
          HiveAvroORCQueryGenerator.generateCreatePartitionDDL(outputDatabaseName,
              outputStagingTableName,
              stagingDataPartitionLocation,
              partitionsDMLInfo);

      conversionEntity.getQueries().addAll(createStagingPartitionDDL);
      log.info("Create staging partition DDL: " + createStagingPartitionDDL);
    }

    // todo only in case of partitioned table, better to make it static insert, because its anyway one by one partition
    conversionEntity.getQueries().add("SET hive.exec.dynamic.partition.mode=nonstrict");

    String insertInStagingTableDML =
        HiveAvroORCQueryGenerator
            .generateTableCopy(conversionEntity.getHiveTable().getAvroSchema(),
                outputSchema,
                inputTableName,
                outputStagingTableName,
                Optional.of(conversionEntity.getHiveTable().getDbName()),
                Optional.of(outputDatabaseName),
                Optional.of(partitionsDMLInfo),
                Optional.<Boolean>absent(),
                Optional.<Boolean>absent());
    conversionEntity.getQueries().add(insertInStagingTableDML);
    log.info("Conversion staging DML: " + insertInStagingTableDML);

    QueryBasedHivePublishEntity publishEntity = new QueryBasedHivePublishEntity();
    List<String> publishQueries = publishEntity.getPublishQueries();
    Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
    List<String> cleanupQueries = publishEntity.getCleanupQueries();
    List<String> cleanupDirectories = publishEntity.getCleanupDirectories();

    if (partitionsDDLInfo.size() == 0) {
      // Step:
      // A.2.2, B.2.2: Snapshot table

      // Step:
      // A.2.2.1, B.2.2.1: Delete data in final table directory
      // A.2.2.2, B.2.2.2: Move data from staging to final table directory
      HiveAvroORCQueryGenerator.cleanUpNonPartitionedTable(publishDirectories, cleanupQueries, outputStagingDataLocation,
          cleanupDirectories, outputDataLocation, outputDatabaseName, outputStagingTableName);

    } else {
      // Step:
      // A.2.3, B.2.3: If partitioned table, move partitions from staging to final table; for all partitions:

      // Step:
// A.2.3.2, B.2.3.2: Move partition directory
      // Move: orcStagingDataPartitionLocation to: orcFinalDataPartitionLocation
      String finalDataPartitionLocation = outputDataLocation + Path.SEPARATOR + stagingDataPartitionDirName;
      Optional<Path> destPartitionLocation = getDestinationPartitionLocation(destinationTableMeta, workUnit,
          conversionEntity.getHivePartition().get().getName());
      finalDataPartitionLocation =
          updatePartitionLocation(finalDataPartitionLocation, workUnit, destPartitionLocation);
      log.info(
          "Partition directory to move: " + stagingDataPartitionLocation + " to: " + finalDataPartitionLocation);
      publishDirectories.put(stagingDataPartitionLocation, finalDataPartitionLocation);
      // Step:
      // A.2.3.1, B.2.3.1: Drop if exists partition in final table

      // Step:
      // If destination partition already exists, alter the partition location
      // A.2.3.3, B.2.3.3: Create partition with location (and update storage format if not in ORC already)
      List<String> dropPartitionsDDL =
          HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName,
              outputTableName,
              partitionsDMLInfo);
      log.info("Drop partitions if exist in final table: " + dropPartitionsDDL);
      publishQueries.addAll(dropPartitionsDDL);
      List<String> createFinalPartitionDDL =
          HiveAvroORCQueryGenerator.generateCreatePartitionDDL(outputDatabaseName,
              outputTableName,
              finalDataPartitionLocation,
              partitionsDMLInfo,
              Optional.<String>absent());

      log.info("Create final partition DDL: " + createFinalPartitionDDL);
      publishQueries.addAll(createFinalPartitionDDL);

      // Step:
      // A.2.3.4, B.2.3.4: Drop this staging table and delete directories
      String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(outputDatabaseName, outputStagingTableName);

      log.info("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      // Delete: outputStagingDataLocation
      log.info("Staging table directory to delete: " + outputStagingDataLocation);
      cleanupDirectories.add(outputStagingDataLocation);
    }

    /*
     * Drop the replaced partitions if any. This is required in case the partition being converted is derived from
     * several other partitions. E.g. Daily partition is a replacement of hourly partitions of the same day. When daily
     * partition is converted to ORC all it's hourly ORC partitions need to be dropped.
     */
    publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName,
        outputTableName,
        getDropPartitionsDDLInfo(conversionEntity)));

    // todo removed wrapper view code
    HiveAvroORCQueryGenerator.serializePublishCommands(workUnit, publishEntity);
    log.info("Publish partition entity: " + publishEntity);

    log.info("Conversion Query " + conversionEntity.getQueries());

    EventWorkunitUtils.setEndDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());

    return new SingleRecordIterable<>(conversionEntity);
  }
}
