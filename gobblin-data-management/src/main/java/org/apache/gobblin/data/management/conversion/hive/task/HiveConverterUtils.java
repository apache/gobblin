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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveUtils;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.thrift.TException;


@Slf4j
public class HiveConverterUtils {

  private static final String DEFAULT_DB_NAME = "default";

  /***
   * Subdirectory within destination table directory to publish data
   */
  private static final String PUBLISHED_TABLE_SUBDIRECTORY = "final";

  /***
   * Separators used by Hive
   */
  private static final String HIVE_PARTITIONS_INFO = "/";
  private static final String HIVE_PARTITIONS_TYPE = ":";

  /**
   * If the property is set to true then partition dir is overwritten,
   * else a new time-stamped partition dir is created to avoid breaking in-flight queries
   * Check org.apache.gobblin.data.management.retention.Avro2OrcStaleDatasetCleaner to clean stale directories
   */
  public static final String HIVE_DATASET_PARTITION_OVERWRITE = "hive.dataset.partition.overwrite";
  public static final boolean DEFAULT_HIVE_DATASET_PARTITION_OVERWRITE = true;

  /**
   * If the property is set to true then in the destination dir permissions, group won't be explicitly set.
   */
  public static final String HIVE_DATASET_DESTINATION_SKIP_SETGROUP = "hive.dataset.destination.skip.setGroup";
  public static final boolean DEFAULT_HIVE_DATASET_DESTINATION_SKIP_SETGROUP = false;

  public static String getStagingTableName(String stagingTableNamePrefix) {
    int randomNumber = new Random().nextInt(10);
    String uniqueStagingTableQualifier = String.format("%s%s", System.currentTimeMillis(), randomNumber);

    return stagingTableNamePrefix + "_" + uniqueStagingTableQualifier;
  }

  /***
   * Get the final table location of format: <final table location>/final
   * @return final table location.
   */
  public static String getOutputDataLocation(String outputDataLocation) {
    return outputDataLocation + Path.SEPARATOR + PUBLISHED_TABLE_SUBDIRECTORY;
  }

  /***
   * Get the staging table location of format: <final table location>/<staging table name>
   * @param outputDataLocation output table data lcoation.
   * @return staging table location.
   */
  public static String getStagingDataLocation(String outputDataLocation, String stagingTableName) {
    return outputDataLocation + Path.SEPARATOR + stagingTableName;
  }

  /***
   * Generate DDL query to create a duplicate Hive table
   * @param inputDbName source DB name
   * @param inputTblName source table name
   * @param tblName New Hive table name
   * @param tblLocation New hive table location
   * @param optionalDbName Optional DB name, if not specified it defaults to 'default'
   */
  public static String generateCreateDuplicateTableDDL(
      String inputDbName,
      String inputTblName,
      String tblName,
      String tblLocation,
      Optional<String> optionalDbName) {

    Preconditions.checkArgument(StringUtils.isNotBlank(tblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(tblLocation));

    String dbName = optionalDbName.isPresent() ? optionalDbName.get() : "default";

    return String.format("CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`%s` LIKE `%s`.`%s` LOCATION %n  '%s' %n",
        dbName, tblName, inputDbName, inputTblName, tblLocation);
  }

  public static String generateTableCopy(
      String inputTblName,
      String outputTblName,
      Optional<String> optionalInputDbName,
      Optional<String> optionalOutputDbName,
      Optional<Map<String, String>> optionalPartitionDMLInfo,
      Optional<Boolean> optionalOverwriteTable,
      Optional<Boolean> optionalCreateIfNotExists) {
    Preconditions.checkArgument(StringUtils.isNotBlank(inputTblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(outputTblName));

    String inputDbName = optionalInputDbName.isPresent() ? optionalInputDbName.get() : DEFAULT_DB_NAME;
    String outputDbName = optionalOutputDbName.isPresent() ? optionalOutputDbName.get() : DEFAULT_DB_NAME;
    boolean shouldOverwriteTable = optionalOverwriteTable.isPresent() ? optionalOverwriteTable.get() : true;
    boolean shouldCreateIfNotExists = optionalCreateIfNotExists.isPresent() ? optionalCreateIfNotExists.get() : false;

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
    dmlQuery.append(partitionKeyValues(optionalPartitionDMLInfo));

    // If not exists
    if (shouldCreateIfNotExists) {
      dmlQuery.append(" IF NOT EXISTS \n");
    }
    dmlQuery.append(String.format("SELECT * FROM `%s`.`%s`", inputDbName, inputTblName));
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size() > 0) {
        dmlQuery.append(" WHERE ");
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
    return dmlQuery.toString();
    //dmlQuery.append(String.format(" %n FROM `%s`.`%s` ", inputDbName, inputTblName));
  }

  protected static StringBuilder partitionKeyValues(Optional<Map<String, String>> optionalPartitionDMLInfo) {
    StringBuilder partitionKV = new StringBuilder();
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size()  > 0) {
        partitionKV.append("PARTITION (");
        boolean isFirstPartitionSpec = true;
        for (Map.Entry<String, String> partition : optionalPartitionDMLInfo.get().entrySet()) {
          if (isFirstPartitionSpec) {
            isFirstPartitionSpec = false;
          } else {
            partitionKV.append(", ");
          }
          partitionKV.append(String.format("`%s`", partition.getKey()));
        }
        partitionKV.append(") \n");
      }
    }
    return partitionKV;
  }

  public static void cleanUpNonPartitionedTable(Map<String, String> publishDirectories, List<String> cleanupQueries,
      String stagingDataLocation, List<String> cleanupDirectories, String outputDataLocation,
      String outputTableDatabase, String stagingTableName) {
    log.debug("Snapshot directory to move: " + stagingDataLocation + " to: " + outputDataLocation);
    publishDirectories.put(stagingDataLocation, outputDataLocation);

    String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(outputTableDatabase, stagingTableName);

    log.debug("Drop staging table DDL: " + dropStagingTableDDL);
    cleanupQueries.add(dropStagingTableDDL);

    log.debug("Staging table directory to delete: " + stagingDataLocation);
    cleanupDirectories.add(stagingDataLocation);
  }

  public static void moveDirectory(FileSystem fs, String sourceDir, String targetDir) throws IOException {
    // If targetDir exists, delete it
    if (fs.exists(new Path(targetDir))) {
      deleteDirectory(fs, targetDir);
    }

    // Create parent directories of targetDir
    WriterUtils.mkdirsWithRecursivePermission(fs, new Path(targetDir).getParent(),
        FsPermission.getCachePoolDefault());

    // Move directory
    log.info("Moving directory: " + sourceDir + " to: " + targetDir);
    if (!fs.rename(new Path(sourceDir), new Path(targetDir))) {
      throw new IOException(String.format("Unable to move %s to %s", sourceDir, targetDir));
    }
  }

  public static void deleteDirectory(FileSystem fs, String dirToDelete) throws IOException {
    if (org.apache.commons.lang.StringUtils.isBlank(dirToDelete)) {
      return;
    }

    log.info("Going to delete existing partition data: " + dirToDelete);
    fs.delete(new Path(dirToDelete), true);
  }

  public static void deleteDirectories(FileSystem fs, List<String> directoriesToDelete) throws IOException {
    for (String directory : directoriesToDelete) {
      deleteDirectory(fs, directory);
    }
  }

  public static void populatePartitionInfo(QueryBasedHiveConversionEntity conversionEntity, Map<String, String> partitionsDDLInfo,
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
      List<String> pInfo = Splitter.on(HIVE_PARTITIONS_INFO).omitEmptyStrings().trimResults().splitToList(partitionsInfoString);
      List<String> pType = Splitter.on(HIVE_PARTITIONS_TYPE).omitEmptyStrings().trimResults().splitToList(partitionsTypeString);
      log.debug("PartitionsInfoString: " + partitionsInfoString);
      log.debug("PartitionsTypeString: " + partitionsTypeString);

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

  public static void createStagingDirectory(FileSystem fs, String destination, QueryBasedHiveConversionEntity conversionEntity,
      WorkUnitState workUnit) {
    /*
     * Create staging data location with the same permissions as source data location
     *
     * Note that hive can also automatically create the non-existing directories but it does not
     * seem to create it with the desired permissions.
     * According to hive docs permissions for newly created directories/files can be controlled using uMask like,
     *
     * SET hive.warehouse.subdir.inherit.perms=false;
     * SET fs.permissions.umask-mode=022;
     * Upon testing, this did not work
     */
    Path destinationPath = new Path(destination);
    try {
      FileStatus sourceDataFileStatus = fs.getFileStatus(conversionEntity.getHiveTable().getDataLocation());
      FsPermission sourceDataPermission = sourceDataFileStatus.getPermission();
      if (!fs.mkdirs(destinationPath, sourceDataPermission)) {
        throw new RuntimeException(String.format("Failed to create path %s with permissions %s",
            destinationPath, sourceDataPermission));
      } else {
        fs.setPermission(destinationPath, sourceDataPermission);
        // Set the same group as source
        if (!workUnit.getPropAsBoolean(HIVE_DATASET_DESTINATION_SKIP_SETGROUP, DEFAULT_HIVE_DATASET_DESTINATION_SKIP_SETGROUP)) {
          fs.setOwner(destinationPath, null, sourceDataFileStatus.getGroup());
        }
        log.info(String.format("Created %s with permissions %s and group %s", destinationPath, sourceDataPermission, sourceDataFileStatus.getGroup()));
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /***
   * Get the partition directory name of the format: [hourly_][daily_]<partitionSpec1>[partitionSpec ..]
   * @param conversionEntity Conversion entity.
   * @param sourceDataPathIdentifier Hints to look in source partition location to prefix the partition dir name
   *                               such as hourly or daily.
   * @return Partition directory name.
   */
  public static String getStagingDataPartitionDirName(QueryBasedHiveConversionEntity conversionEntity,
      List<String> sourceDataPathIdentifier) {

    if (conversionEntity.getHivePartition().isPresent()) {
      StringBuilder dirNamePrefix = new StringBuilder();
      String sourceHivePartitionLocation = conversionEntity.getHivePartition().get().getDataLocation().toString();
      if (null != sourceDataPathIdentifier && null != sourceHivePartitionLocation) {
        for (String hint : sourceDataPathIdentifier) {
          if (sourceHivePartitionLocation.toLowerCase().contains(hint.toLowerCase())) {
            dirNamePrefix.append(hint.toLowerCase()).append("_");
          }
        }
      }

      return dirNamePrefix + conversionEntity.getHivePartition().get().getName();
    } else {
      return StringUtils.EMPTY;
    }
  }

  public static Optional<Path> getDestinationPartitionLocation(Optional<Table> table, WorkUnitState state,
      String partitionName)
      throws DataConversionException {
    Optional<org.apache.hadoop.hive.metastore.api.Partition> partitionOptional;
    if (!table.isPresent()) {
      return Optional.absent();
    }
    try {
      HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(state.getJobState().getProperties(),
          Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        partitionOptional =
            Optional.of(client.get().getPartition(table.get().getDbName(), table.get().getTableName(), partitionName));
      } catch (NoSuchObjectException e) {
        return Optional.absent();
      }
      if (partitionOptional.isPresent()) {
        org.apache.hadoop.hive.ql.metadata.Table qlTable = new org.apache.hadoop.hive.ql.metadata.Table(table.get());
        Partition qlPartition =
            new Partition(qlTable, partitionOptional.get());
        return Optional.of(qlPartition.getDataLocation());
      }
    } catch (IOException | TException | HiveException e) {
      throw new DataConversionException("Could not fetch destination table metadata", e);
    }
    return Optional.absent();
  }

  /**
   * If partition already exists then new partition location will be a separate time stamp dir
   * If partition location is /a/b/c/<oldTimeStamp> then new partition location is /a/b/c/<currentTimeStamp>
   * If partition location is /a/b/c/ then new partition location is /a/b/c/<currentTimeStamp>
   **/
  public static String updatePartitionLocation(String outputDataPartitionLocation, WorkUnitState workUnitState,
      Optional<Path> destPartitionLocation)
      throws DataConversionException {

    if (workUnitState.getPropAsBoolean(HIVE_DATASET_PARTITION_OVERWRITE, DEFAULT_HIVE_DATASET_PARTITION_OVERWRITE)) {
      return outputDataPartitionLocation;
    }
    if (!destPartitionLocation.isPresent()) {
      return outputDataPartitionLocation;
    }
    long timeStamp = System.currentTimeMillis();
    return StringUtils.join(Arrays.asList(outputDataPartitionLocation, timeStamp), '/');
  }

  public static Pair<Optional<Table>, Optional<List<Partition>>> getDestinationTableMeta(String dbName,
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

  public static FileSystem getSourceFs(State state) throws IOException {
    if (state.contains(HiveSource.HIVE_SOURCE_FS_URI)) {
      return FileSystem.get(URI.create(state.getProp(HiveSource.HIVE_SOURCE_FS_URI)), HadoopUtils.getConfFromState(state));
    }
    return FileSystem.get(HadoopUtils.getConfFromState(state));
  }
}
