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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import static java.util.stream.Collectors.joining;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.data.management.conversion.hive.entities.HiveProcessingEntity;
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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveUtils;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.AutoReturnableObject;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j

/**
 * A utility class for converting hive data from one dataset to another.
 */
public class HiveConverterUtils {

  @AllArgsConstructor
  @Getter
  public static enum StorageFormat {
    TEXT_FILE("TEXTFILE"),
    SEQUENCE_FILE("SEQUENCEFILE"),
    ORC("ORC"),
    PARQUET("PARQUET"),
    AVRO("AVRO"),
    RC_FILE("RCFILE");

    private final String hiveName;
  }

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
    int randomNumber = new Random().nextInt(100);
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

  /**
   * Generates a CTAS statement to dump the contents of a table / partition into a new table.
   * @param outputDbAndTable output db and table where contents should be written.
   * @param sourceEntity source table / partition.
   * @param partitionDMLInfo map of partition values.
   * @param storageFormat format of output table.
   * @param outputTableLocation location where files of output table should be written.
   */
  public static String generateStagingCTASStatementFromSelectStar(HiveDatasetFinder.DbAndTable outputDbAndTable,
      HiveDatasetFinder.DbAndTable sourceEntity, Map<String, String> partitionDMLInfo,
      StorageFormat storageFormat, String outputTableLocation) {
    StringBuilder sourceQueryBuilder = new StringBuilder("SELECT * FROM `").append(sourceEntity.getDb())
        .append("`.`").append(sourceEntity.getTable()).append("`");
    if (partitionDMLInfo != null && !partitionDMLInfo.isEmpty()) {
      sourceQueryBuilder.append(" WHERE ");
      sourceQueryBuilder.append(partitionDMLInfo.entrySet().stream()
          .map(e -> "`" + e.getKey() + "`='" + e.getValue() + "'")
          .collect(joining(" AND ")));
    }
    return generateStagingCTASStatement(outputDbAndTable, sourceQueryBuilder.toString(), storageFormat, outputTableLocation);
  }

  /**
   * Generates a CTAS statement to dump the results of a query into a new table.
   * @param outputDbAndTable output db and table where contents should be written.
   * @param sourceQuery query to materialize.
   * @param storageFormat format of output table.
   * @param outputTableLocation location where files of output table should be written.
   */
  public static String generateStagingCTASStatement(HiveDatasetFinder.DbAndTable outputDbAndTable,
      String sourceQuery, StorageFormat storageFormat, String outputTableLocation) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(outputDbAndTable.getDb()) &&
        !Strings.isNullOrEmpty(outputDbAndTable.getTable()), "Invalid output db and table " + outputDbAndTable);

    return String.format("CREATE TEMPORARY TABLE `%s`.`%s` STORED AS %s LOCATION '%s' AS %s", outputDbAndTable.getDb(),
        outputDbAndTable.getTable(), storageFormat.getHiveName(), outputTableLocation, sourceQuery);
  }

  /**
   * Fills data from input table into output table.
   * @param inputTblName input hive table name
   * @param outputTblName output hive table name
   * @param inputDbName input hive database name
   * @param outputDbName output hive database name
   * @param optionalPartitionDMLInfo input hive table's partition's name and value
   * @return Hive query string
   */
  public static String generateTableCopy(
      String inputTblName,
      String outputTblName,
      String inputDbName,
      String outputDbName,
      Optional<Map<String, String>> optionalPartitionDMLInfo) {
    Preconditions.checkArgument(StringUtils.isNotBlank(inputTblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(outputTblName));
    Preconditions.checkArgument(StringUtils.isNotBlank(inputDbName));
    Preconditions.checkArgument(StringUtils.isNotBlank(outputDbName));

    StringBuilder dmlQuery = new StringBuilder();

    // Insert query
    dmlQuery.append(String.format("INSERT OVERWRITE TABLE `%s`.`%s` %n", outputDbName, outputTblName));

    if (optionalPartitionDMLInfo.isPresent() && optionalPartitionDMLInfo.get().size() > 0) {
      // Partition details
      dmlQuery.append(partitionKeyValues(optionalPartitionDMLInfo));
    }

    dmlQuery.append(String.format("SELECT * FROM `%s`.`%s`", inputDbName, inputTblName));
    if (optionalPartitionDMLInfo.isPresent()) {
      if (optionalPartitionDMLInfo.get().size() > 0) {
        dmlQuery.append(" WHERE ");
        String partitionsAndValues = optionalPartitionDMLInfo.get().entrySet().stream()
            .map(e -> "`" + e.getKey() + "`='" + e.getValue() + "'")
            .collect(joining(" AND "));
        dmlQuery.append(partitionsAndValues);
      }
    }

    return dmlQuery.toString();
  }

  protected static StringBuilder partitionKeyValues(Optional<Map<String, String>> optionalPartitionDMLInfo) {
    if (!optionalPartitionDMLInfo.isPresent()) {
      return new StringBuilder();
    } else {
      return new StringBuilder("PARTITION (").append(Joiner.on(", ")
          .join(optionalPartitionDMLInfo.get().entrySet().stream().map(e -> "`" + e.getKey() + "`").iterator())).append(") \n");
    }
  }

  /**
   * It fills partitionsDDLInfo and partitionsDMLInfo with the partition information
   * @param conversionEntity conversion entity to
   * @param partitionsDDLInfo partition type information, to be filled by this method
   * @param partitionsDMLInfo partition key-value pair, to be filled by this method
   */
  public static void populatePartitionInfo(HiveProcessingEntity conversionEntity, Map<String, String> partitionsDDLInfo,
      Map<String, String> partitionsDMLInfo) {

    String partitionsInfoString = null;
    String partitionsTypeString = null;

    if (conversionEntity.getPartition().isPresent()) {
      partitionsInfoString = conversionEntity.getPartition().get().getName();
      partitionsTypeString = conversionEntity.getPartition().get().getSchema().getProperty("partition_columns.types");
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

  /**
   * Creates a staging directory with the permission as in source directory.
   * @param fs filesystem object
   * @param destination staging directory location
   * @param conversionEntity conversion entity used to get source directory permissions
   * @param workUnit workunit
   */
  public static void createStagingDirectory(FileSystem fs, String destination, HiveProcessingEntity conversionEntity,
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
      FsPermission permission;
      String group = null;
      if (conversionEntity.getTable().getDataLocation() != null) {
        FileStatus sourceDataFileStatus = fs.getFileStatus(conversionEntity.getTable().getDataLocation());
        permission = sourceDataFileStatus.getPermission();
        group = sourceDataFileStatus.getGroup();
      } else {
        permission = FsPermission.getDefault();
      }

      if (!fs.mkdirs(destinationPath, permission)) {
        throw new RuntimeException(String.format("Failed to create path %s with permissions %s",
            destinationPath, permission));
      } else {
        fs.setPermission(destinationPath, permission);
        // Set the same group as source
        if (group != null && !workUnit.getPropAsBoolean(HIVE_DATASET_DESTINATION_SKIP_SETGROUP, DEFAULT_HIVE_DATASET_DESTINATION_SKIP_SETGROUP)) {
          fs.setOwner(destinationPath, null, group);
        }
        log.info(String.format("Created %s with permissions %s and group %s", destinationPath, permission, group));
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
  public static String getStagingDataPartitionDirName(HiveProcessingEntity conversionEntity,
      List<String> sourceDataPathIdentifier) {

    if (conversionEntity.getPartition().isPresent()) {
      StringBuilder dirNamePrefix = new StringBuilder();
      String sourceHivePartitionLocation = conversionEntity.getPartition().get().getDataLocation().toString();
      if (null != sourceDataPathIdentifier && null != sourceHivePartitionLocation) {
        for (String hint : sourceDataPathIdentifier) {
          if (sourceHivePartitionLocation.toLowerCase().contains(hint.toLowerCase())) {
            dirNamePrefix.append(hint.toLowerCase()).append("_");
          }
        }
      }

      return dirNamePrefix + conversionEntity.getPartition().get().getName();
    } else {
      return StringUtils.EMPTY;
    }
  }

  /**
   * Returns the partition data location of a given table and partition
   * @param table Hive table
   * @param state workunit state
   * @param partitionName partition name
   * @return partition data location
   * @throws DataConversionException
   */
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

  /**
   * Returns a pair of Hive table and its partitions
   * @param dbName db name
   * @param tableName table name
   * @param props properties
   * @return a pair of Hive table and its partitions
   * @throws DataConversionException
   */
  public static Pair<Optional<Table>, Optional<List<Partition>>> getDestinationTableMeta(String dbName,
      String tableName, Properties props) {

    Optional<Table> table = Optional.<Table>absent();
    Optional<List<Partition>> partitions = Optional.<List<Partition>>absent();

    try {
      HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(props,
          Optional.fromNullable(props.getProperty(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
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
      throw new RuntimeException("Could not fetch destination table metadata", e);
    }

    return ImmutablePair.of(table, partitions);
  }
}
