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
package org.apache.gobblin.data.management.conversion.hive.converter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import org.apache.gobblin.data.management.conversion.hive.entities.HiveProcessingEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.metrics.event.sla.SlaEventKeys;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.HadoopUtils;

import static org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils.getOutputDataLocation;


/**
 * Builds the Hive avro to ORC conversion query. The record type for this converter is {@link QueryBasedHiveConversionEntity}. A {@link QueryBasedHiveConversionEntity}
 * can be a hive table or a hive partition.
 * <p>
 * Concrete subclasses define the semantics of Avro to ORC conversion for a specific ORC format by providing {@link ConversionConfig}s.
 * </p>
 */
@Slf4j
public abstract class AbstractAvroToOrcConverter extends Converter<Schema, Schema, QueryBasedHiveConversionEntity, QueryBasedHiveConversionEntity> {

  /***
   * Subdirectory within destination ORC table directory to publish data
   */
  private static final String PUBLISHED_TABLE_SUBDIRECTORY = "final";
  public static final String OUTPUT_AVRO_SCHEMA_KEY = "output.avro.schema";

  private static final String ORC_FORMAT = "orc";

  /**
   * Hive runtime property key names for tracking
   */
  private static final String GOBBLIN_DATASET_URN_KEY = "gobblin.datasetUrn";
  private static final String GOBBLIN_PARTITION_NAME_KEY = "gobblin.partitionName";
  private static final String GOBBLIN_WORKUNIT_CREATE_TIME_KEY = "gobblin.workunitCreateTime";

  /***
   * Separators used by Hive
   */
  private static final String HIVE_PARTITIONS_INFO = "/";
  private static final String HIVE_PARTITIONS_TYPE = ":";

  protected final FileSystem fs;

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
   * list of partitions that a partition has replaced. E.g. list of hourly partitons for a daily partition
   */
  public static final String REPLACED_PARTITIONS_HIVE_METASTORE_KEY = "gobblin.replaced.partitions";

  /**
   * The dataset being converted.
   */
  protected ConvertibleHiveDataset hiveDataset;

  /**
   * If the property is set to true then in the destination dir permissions, group won't be explicitly set.
   */
  public static final String HIVE_DATASET_DESTINATION_SKIP_SETGROUP = "hive.dataset.destination.skip.setGroup";
  public static final boolean DEFAULT_HIVE_DATASET_DESTINATION_SKIP_SETGROUP = false;

  public static final String HIVE_DATASET_DESTINATION_GROUP_NAME = "hive.dataset.destination.groupName";
  public static final String HIVE_DATASET_STAGING_GROUP_NAME = "hive.dataset.staging.groupName";



  /**
   * If set to true, a set format DDL will be separate from add partition DDL
   */
  public static final String HIVE_CONVERSION_SETSERDETOAVROEXPLICITELY = "hive.conversion.setSerdeToAvroExplicitly";
  public static final boolean DEFAULT_HIVE_CONVERSION_SETSERDETOAVROEXPLICITELY = true;

  /***
   * Global Hive conversion view registration whitelist / blacklist key
   */
  public static final String HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST = "hive.conversion.view.registration.whitelist";
  public static final String HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST = "hive.conversion.view.registration.blacklist";

  /**
   * Subclasses can convert the {@link Schema} if required.
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.converter.Converter#convertSchema(java.lang.Object, org.apache.gobblin.configuration.WorkUnitState)
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
   * @return Conversion config
   */
  protected abstract ConversionConfig getConversionConfig();

  public AbstractAvroToOrcConverter() {
    try {
      this.fs = FileSystem.get(HadoopUtils.newConfiguration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Populate the avro to orc conversion queries. The Queries will be added to {@link QueryBasedHiveConversionEntity#getQueries()}
   */
  @Override
  public Iterable<QueryBasedHiveConversionEntity> convertRecord(Schema outputAvroSchema, QueryBasedHiveConversionEntity conversionEntity, WorkUnitState workUnit)
      throws DataConversionException {

    Preconditions.checkNotNull(outputAvroSchema, "Avro schema must not be null");
    Preconditions.checkNotNull(conversionEntity, "Conversion entity must not be null");
    Preconditions.checkNotNull(workUnit, "Workunit state must not be null");
    Preconditions.checkNotNull(conversionEntity.getTable(), "Hive table within conversion entity must not be null");

    EventWorkunitUtils.setBeginDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());

    this.hiveDataset = conversionEntity.getConvertibleHiveDataset();

    if (!hasConversionConfig()) {
      return new SingleRecordIterable<>(conversionEntity);
    }

    // Avro table name and location
    String avroTableName = conversionEntity.getTable().getTableName();

    // ORC table name and location
    String orcTableName = getConversionConfig().getDestinationTableName();
    String orcStagingTableName = getOrcStagingTableName(getConversionConfig().getDestinationStagingTableName());
    String orcTableDatabase = getConversionConfig().getDestinationDbName();
    String orcDataLocation = getOrcDataLocation();
    String orcStagingDataLocation = getOrcStagingDataLocation(orcStagingTableName);
    boolean isEvolutionEnabled = getConversionConfig().isEvolutionEnabled();
    boolean isCasePreserved = getConversionConfig().isCasePreserved();
    Pair<Optional<Table>, Optional<List<Partition>>> destinationMeta = HiveConverterUtils.getDestinationTableMeta(orcTableDatabase,
        orcTableName, workUnit.getProperties());
    Optional<Table> destinationTableMeta = destinationMeta.getLeft();

    // Optional
    // View registration blacklist / whitelist
    Optional<WhitelistBlacklist> optionalViewRegistrationWhiteBlacklist = getViewWhiteBackListFromWorkUnit(workUnit);

    // wrapperViewName          : If specified view with 'wrapperViewName' is created if not already exists
    //                            over destination table
    // isUpdateViewAlwaysEnabled: If false 'wrapperViewName' is only updated when schema evolves; if true
    //                            'wrapperViewName' is always updated (everytime publish happens)
    Optional<String> wrapperViewName = Optional.<String>absent();
    if (optionalViewRegistrationWhiteBlacklist.isPresent()) {
      wrapperViewName = optionalViewRegistrationWhiteBlacklist.get().acceptTable(orcTableDatabase, orcTableName)
          ? getConversionConfig().getDestinationViewName() : wrapperViewName;
    } else {
      wrapperViewName = getConversionConfig().getDestinationViewName();
    }
    boolean shouldUpdateView = getConversionConfig().isUpdateViewAlwaysEnabled();

    // Other properties
    Optional<List<String>> clusterBy =
        getConversionConfig().getClusterBy().isEmpty()
            ? Optional.<List<String>> absent()
            : Optional.of(getConversionConfig().getClusterBy());
    Optional<Integer> numBuckets = getConversionConfig().getNumBuckets();
    Optional<Integer> rowLimit = getConversionConfig().getRowLimit();
    Properties tableProperties = getConversionConfig().getDestinationTableProperties();

    // Partition dir hint helps create different directory for hourly and daily partition with same timestamp, such as:
    // .. daily_2016-01-01-00 and hourly_2016-01-01-00
    // This helps existing hourly data from not being deleted at the time of roll up, and so Hive queries in flight
    // .. do not fail
    List<String> sourceDataPathIdentifier = getConversionConfig().getSourceDataPathIdentifier();

    // Populate optional partition info
    Map<String, String> partitionsDDLInfo = Maps.newHashMap();
    Map<String, String> partitionsDMLInfo = Maps.newHashMap();
    HiveConverterUtils.populatePartitionInfo(conversionEntity, partitionsDDLInfo, partitionsDMLInfo);

    /*
     * Create ORC data location with the same permissions as Avro data
     *
     * Note that hive can also automatically create the non-existing directories but it does not
     * seem to create it with the desired permissions.
     * According to hive docs permissions for newly created directories/files can be controlled using uMask like,
     *
     * SET hive.warehouse.subdir.inherit.perms=false;
     * SET fs.permissions.umask-mode=022;
     * Upon testing, this did not work
     */
    try {
      FileStatus sourceDataFileStatus = this.fs.getFileStatus(conversionEntity.getTable().getDataLocation());
      FsPermission sourceDataPermission = sourceDataFileStatus.getPermission();
      if (!this.fs.mkdirs(new Path(getConversionConfig().getDestinationDataPath()), sourceDataPermission)) {
        throw new RuntimeException(String.format("Failed to create path %s with permissions %s", new Path(
            getConversionConfig().getDestinationDataPath()), sourceDataPermission));
      } else {
        this.fs.setPermission(new Path(getConversionConfig().getDestinationDataPath()), sourceDataPermission);

        // Explicitly set group name for destination location if specified otherwise preserve source group name
        String destinationGroupName;
        if (workUnit.contains(HIVE_DATASET_DESTINATION_GROUP_NAME)) {
          destinationGroupName = workUnit.getProp(HIVE_DATASET_DESTINATION_GROUP_NAME);
        } else {
          destinationGroupName = sourceDataFileStatus.getGroup();
        }
        if (!workUnit.getPropAsBoolean(HIVE_DATASET_DESTINATION_SKIP_SETGROUP,
            DEFAULT_HIVE_DATASET_DESTINATION_SKIP_SETGROUP)) {
          this.fs.setOwner(new Path(getConversionConfig().getDestinationDataPath()), null, destinationGroupName);
        }
        log.info(String.format("Created %s with permissions %s and group %s", new Path(getConversionConfig()
            .getDestinationDataPath()), sourceDataPermission, sourceDataFileStatus.getGroup()));

        // Explicitly set group name for staging directory if specified
        if (workUnit.contains(HIVE_DATASET_STAGING_GROUP_NAME)) {
          String stagingGroupName = workUnit.getProp(HIVE_DATASET_STAGING_GROUP_NAME);
          log.info("Setting staging directory group name as " + stagingGroupName);
          this.fs.mkdirs(new Path(getOrcStagingDataLocation(orcStagingTableName)));
          this.fs.setOwner(new Path(getOrcStagingDataLocation(orcStagingTableName)), null, stagingGroupName);

          // Staging directory will be renamed to getOrcDataLocation() and hence it's group name should match
          // with the group name of the staging directory
          this.fs.mkdirs(new Path(getOrcDataLocation()));
          this.fs.setOwner(new Path(getOrcDataLocation()), null, stagingGroupName);
        }
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }

    // Set hive runtime properties from conversion config
    for (Map.Entry<Object, Object> entry : getConversionConfig().getHiveRuntimeProperties().entrySet()) {
      conversionEntity.getQueries().add(String.format("SET %s=%s", entry.getKey(), entry.getValue()));
    }
    // Set hive runtime properties for tracking
    conversionEntity.getQueries().add(String.format("SET %s=%s", GOBBLIN_DATASET_URN_KEY,
        conversionEntity.getTable().getCompleteName()));
    if (conversionEntity.getPartition().isPresent()) {
      conversionEntity.getQueries().add(String.format("SET %s=%s", GOBBLIN_PARTITION_NAME_KEY,
          conversionEntity.getPartition().get().getCompleteName()));
    }
    conversionEntity.getQueries().add(String
        .format("SET %s=%s", GOBBLIN_WORKUNIT_CREATE_TIME_KEY,
            workUnit.getWorkunit().getProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY)));
    workUnit.setProp(OUTPUT_AVRO_SCHEMA_KEY, outputAvroSchema.toString());

    // Create DDL statement for table
    Map<String, String> hiveColumns = new LinkedHashMap<>();
    String createStagingTableDDL =
        HiveAvroORCQueryGenerator.generateCreateTableDDL(outputAvroSchema,
            orcStagingTableName,
            orcStagingDataLocation,
            Optional.of(orcTableDatabase),
            Optional.of(partitionsDDLInfo),
            clusterBy,
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(),
            numBuckets,
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            tableProperties,
            isEvolutionEnabled,
            isCasePreserved,
            destinationTableMeta,
            hiveColumns);
    conversionEntity.getQueries().add(createStagingTableDDL);
    log.debug("Create staging table DDL: " + createStagingTableDDL);

    // Create DDL statement for partition
    String orcStagingDataPartitionDirName = HiveConverterUtils.getStagingDataPartitionDirName(conversionEntity, sourceDataPathIdentifier);
    String orcStagingDataPartitionLocation = orcStagingDataLocation + Path.SEPARATOR + orcStagingDataPartitionDirName;
    if (partitionsDMLInfo.size() > 0) {
      List<String> createStagingPartitionDDL =
          HiveAvroORCQueryGenerator.generateCreatePartitionDDL(orcTableDatabase,
              orcStagingTableName,
              orcStagingDataPartitionLocation,
              partitionsDMLInfo);

      conversionEntity.getQueries().addAll(createStagingPartitionDDL);
      log.debug("Create staging partition DDL: " + createStagingPartitionDDL);
    }

    // Create DML statement
    String insertInORCStagingTableDML =
        HiveAvroORCQueryGenerator
            .generateTableMappingDML(conversionEntity.getHiveTable().getAvroSchema(),
                outputAvroSchema,
                avroTableName,
                orcStagingTableName,
                Optional.of(conversionEntity.getTable().getDbName()),
                Optional.of(orcTableDatabase),
                Optional.of(partitionsDMLInfo),
                Optional.<Boolean>absent(),
                Optional.<Boolean>absent(),
                isEvolutionEnabled,
                destinationTableMeta,
                rowLimit);
    conversionEntity.getQueries().add(insertInORCStagingTableDML);
    log.debug("Conversion staging DML: " + insertInORCStagingTableDML);

    // TODO: Split this method into two (conversion and publish)
    // Addition to WUS for Staging publish:
    // A. Evolution turned on:
    //    1. If table does not exists: simply create it (now it should exist)
    //    2. If table exists:
    //      2.1 Evolve table (alter table)
    //      2.2 If snapshot table:
    //          2.2.1 Delete data in final table directory
    //          2.2.2 Move data from staging to final table directory
    //          2.2.3 Drop this staging table and delete directories
    //      2.3 If partitioned table, move partitions from staging to final table; for all partitions:
    //          2.3.1 Drop if exists partition in final table
    //          2.3.2 Move partition directory
    //          2.3.3 Create partition with location
    //          2.3.4 Drop this staging table and delete directories
    // B. Evolution turned off:
    //    1. If table does not exists: simply create it (now it should exist)
    //    2. If table exists:
    //      2.1 Do not evolve table
    //      2.2 If snapshot table:
    //          2.2.1 Delete data in final table directory
    //          2.2.2 Move data from staging to final table directory
    //          2.2.3 Drop this staging table and delete directories
    //      2.3 If partitioned table, move partitions from staging to final table; for all partitions:
    //          2.3.1 Drop if exists partition in final table
    //          2.3.2 Move partition directory
    //          2.3.3 Create partition with location
    //          2.3.4 Drop this staging table and delete directories
    // Note: The queries below also serve as compatibility check module before conversion, an incompatible
    //      .. schema throws a Runtime exeption, hence preventing further execution
    QueryBasedHivePublishEntity publishEntity = new QueryBasedHivePublishEntity();
    List<String> publishQueries = publishEntity.getPublishQueries();
    Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
    List<String> cleanupQueries = publishEntity.getCleanupQueries();
    List<String> cleanupDirectories = publishEntity.getCleanupDirectories();

    // Step:
    // A.1, B.1: If table does not exists, simply create it
    if (!destinationTableMeta.isPresent()) {
      String createTargetTableDDL =
          HiveAvroORCQueryGenerator.generateCreateTableDDL(outputAvroSchema,
              orcTableName,
              orcDataLocation,
              Optional.of(orcTableDatabase),
              Optional.of(partitionsDDLInfo),
              clusterBy,
              Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(),
              numBuckets,
              Optional.<String>absent(),
              Optional.<String>absent(),
              Optional.<String>absent(),
              tableProperties,
              isCasePreserved,
              isEvolutionEnabled,
              destinationTableMeta,
              new HashMap<String, String>());
      publishQueries.add(createTargetTableDDL);
      log.debug("Create final table DDL: " + createTargetTableDDL);
    }

    // Step:
    // A.2.1: If table pre-exists (destinationTableMeta would be present), evolve table and update table properties
    // B.2.1: No-op
    List<String> evolutionDDLs = HiveAvroORCQueryGenerator.generateEvolutionDDL(orcStagingTableName,
        orcTableName,
        Optional.of(orcTableDatabase),
        Optional.of(orcTableDatabase),
        outputAvroSchema,
        isEvolutionEnabled,
        hiveColumns,
        destinationTableMeta,
        tableProperties);
    log.debug("Evolve final table DDLs: " + evolutionDDLs);
    EventWorkunitUtils.setEvolutionMetadata(workUnit, evolutionDDLs);

    // View (if present) must be updated if evolution happens
    shouldUpdateView |= evolutionDDLs.size() > 0;

    publishQueries.addAll(evolutionDDLs);


    if (partitionsDDLInfo.size() == 0) {
      // Step:
      // A.2.2, B.2.2: Snapshot table

      // Step:
      // A.2.2.1, B.2.2.1: Delete data in final table directory
      // A.2.2.2, B.2.2.2: Move data from staging to final table directory
      log.info("Snapshot directory to move: " + orcStagingDataLocation + " to: " + orcDataLocation);
      publishDirectories.put(orcStagingDataLocation, orcDataLocation);

      // Step:
      // A.2.2.3, B.2.2.3: Drop this staging table and delete directories
      String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(orcTableDatabase, orcStagingTableName);

      log.debug("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      // Delete: orcStagingDataLocation
      log.info("Staging table directory to delete: " + orcStagingDataLocation);
      cleanupDirectories.add(orcStagingDataLocation);

    } else {
      // Step:
      // A.2.3, B.2.3: If partitioned table, move partitions from staging to final table; for all partitions:

      // Step:
      // A.2.3.2, B.2.3.2: Move partition directory
      // Move: orcStagingDataPartitionLocation to: orcFinalDataPartitionLocation
      String orcFinalDataPartitionLocation = orcDataLocation + Path.SEPARATOR + orcStagingDataPartitionDirName;
      Optional<Path> destPartitionLocation = getDestinationPartitionLocation(destinationTableMeta, workUnit,
          conversionEntity.getPartition().get().getName());
      orcFinalDataPartitionLocation =
          HiveConverterUtils.updatePartitionLocation(orcFinalDataPartitionLocation, workUnit, destPartitionLocation);
      log.info(
          "Partition directory to move: " + orcStagingDataPartitionLocation + " to: " + orcFinalDataPartitionLocation);
      publishDirectories.put(orcStagingDataPartitionLocation, orcFinalDataPartitionLocation);
      // Step:
      // A.2.3.1, B.2.3.1: Drop if exists partition in final table

      // Step:
      // If destination partition already exists, alter the partition location
      // A.2.3.3, B.2.3.3: Create partition with location (and update storage format if not in ORC already)
      List<String> dropPartitionsDDL =
          HiveAvroORCQueryGenerator.generateDropPartitionsDDL(orcTableDatabase,
              orcTableName,
              partitionsDMLInfo);
      log.debug("Drop partitions if exist in final table: " + dropPartitionsDDL);
      publishQueries.addAll(dropPartitionsDDL);
      if (workUnit.getPropAsBoolean(HIVE_CONVERSION_SETSERDETOAVROEXPLICITELY,
          DEFAULT_HIVE_CONVERSION_SETSERDETOAVROEXPLICITELY)) {
        List<String> createFinalPartitionDDL =
            HiveAvroORCQueryGenerator.generateCreatePartitionDDL(orcTableDatabase,
                orcTableName,
                orcFinalDataPartitionLocation,
                partitionsDMLInfo,
                Optional.<String>absent());

        log.debug("Create final partition DDL: " + createFinalPartitionDDL);
        publishQueries.addAll(createFinalPartitionDDL);

        // Updating storage format non-transactionally is a stop gap measure until Hive supports transactionally update
        // .. storage format in ADD PARITTION command (today it only supports specifying location)
        List<String> updatePartitionStorageFormatDDL =
            HiveAvroORCQueryGenerator.generateAlterTableOrPartitionStorageFormatDDL(orcTableDatabase,
                orcTableName,
                Optional.of(partitionsDMLInfo),
                ORC_FORMAT);
        log.debug("Update final partition storage format to ORC (if not already in ORC)");
        publishQueries.addAll(updatePartitionStorageFormatDDL);
      } else {
        List<String> createFinalPartitionDDL =
            HiveAvroORCQueryGenerator.generateCreatePartitionDDL(orcTableDatabase,
                orcTableName,
                orcFinalDataPartitionLocation,
                partitionsDMLInfo,
                Optional.fromNullable(ORC_FORMAT));

        log.debug("Create final partition DDL: " + createFinalPartitionDDL);
        publishQueries.addAll(createFinalPartitionDDL);
      }

      // Step:
      // A.2.3.4, B.2.3.4: Drop this staging table and delete directories
      String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(orcTableDatabase, orcStagingTableName);

      log.debug("Drop staging table DDL: " + dropStagingTableDDL);
      cleanupQueries.add(dropStagingTableDDL);

      // Delete: orcStagingDataLocation
      log.info("Staging table directory to delete: " + orcStagingDataLocation);
      cleanupDirectories.add(orcStagingDataLocation);
    }

    /*
     * Drop the replaced partitions if any. This is required in case the partition being converted is derived from
     * several other partitions. E.g. Daily partition is a replacement of hourly partitions of the same day. When daily
     * partition is converted to ORC all it's hourly ORC partitions need to be dropped.
     */
    publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(orcTableDatabase,
        orcTableName,
        getDropPartitionsDDLInfo(conversionEntity)));

    /*
     * Create or update view over the ORC table if specified in the config (ie. wrapper view name is present in config)
     */
    if (wrapperViewName.isPresent()) {
      String viewName = wrapperViewName.get();
      List<String> createOrUpdateViewDDLs = HiveAvroORCQueryGenerator.generateCreateOrUpdateViewDDL(orcTableDatabase,
          orcTableName, orcTableDatabase, viewName, shouldUpdateView);
      log.debug("Create or update View DDLs: " + createOrUpdateViewDDLs);
      publishQueries.addAll(createOrUpdateViewDDLs);

    }

    HiveAvroORCQueryGenerator.serializePublishCommands(workUnit, publishEntity);
    log.debug("Publish partition entity: " + publishEntity);


    log.debug("Conversion Query " + conversionEntity.getQueries());

    EventWorkunitUtils.setEndDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());

    return new SingleRecordIterable<>(conversionEntity);
  }

  /***
   * Get Hive view registration whitelist blacklist from Workunit state
   * @param workUnit Workunit containing view whitelist blacklist property
   * @return Optional WhitelistBlacklist if Workunit contains it
   */
  @VisibleForTesting
  public static Optional<WhitelistBlacklist> getViewWhiteBackListFromWorkUnit(WorkUnitState workUnit) {
    Optional<WhitelistBlacklist> optionalViewWhiteBlacklist = Optional.absent();

    if (workUnit == null) {
      return optionalViewWhiteBlacklist;
    }
    if (workUnit.contains(HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST)
        || workUnit.contains(HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST)) {
      String viewWhiteList = workUnit.getProp(HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST, StringUtils.EMPTY);
      String viewBlackList = workUnit.getProp(HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST, StringUtils.EMPTY);
      try {
        optionalViewWhiteBlacklist = Optional.of(new WhitelistBlacklist(viewWhiteList, viewBlackList));
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }

    return optionalViewWhiteBlacklist;
  }

  /***
   * Get the staging table name for current converter. Each converter creates its own staging table.
   * @param stagingTableNamePrefix for the staging table for this converter.
   * @return Staging table name.
   */
  private String getOrcStagingTableName(String stagingTableNamePrefix) {
    int randomNumber = new Random().nextInt(10);
    String uniqueStagingTableQualifier = String.format("%s%s", System.currentTimeMillis(), randomNumber);

    return stagingTableNamePrefix + "_" + uniqueStagingTableQualifier;
  }



  /***
   * Get the ORC final table location of format: <ORC final table location>/final
   * @return ORC final table location.
   */
  private String getOrcDataLocation() {
    String orcDataLocation = getConversionConfig().getDestinationDataPath();
    return getConversionConfig().getDataDstPathUseSubdir() ? getOutputDataLocation(orcDataLocation)
        : orcDataLocation;
  }

  /***
   * Get the ORC staging table location of format: <ORC final table location>/<ORC staging table name>
   * @param orcStagingTableName ORC staging table name.
   * @return ORC staging table location.
   */
  private String getOrcStagingDataLocation(String orcStagingTableName) {
    String orcDataLocation = getConversionConfig().getDestinationDataPath();

    return orcDataLocation + Path.SEPARATOR + orcStagingTableName;
  }


  @VisibleForTesting
  public static List<Map<String, String>> getDropPartitionsDDLInfo(HiveProcessingEntity conversionEntity) {
    if (!conversionEntity.getPartition().isPresent()) {
      return Collections.emptyList();
    }

    return getDropPartitionsDDLInfo(conversionEntity.getPartition().get());

  }

  /**
   * Parse the {@link #REPLACED_PARTITIONS_HIVE_METASTORE_KEY} from partition parameters to returns DDLs for all the partitions to be
   * dropped.
   *
   * @return A {@link List} of partitions to be dropped. Each element of the list is a {@link Map} which maps a partition's
   * key and value.
   *
   */
  public static List<Map<String, String>> getDropPartitionsDDLInfo(Partition hivePartition) {
    List<Map<String, String>> replacedPartitionsDDLInfo = Lists.newArrayList();
    List<FieldSchema> partitionKeys = hivePartition.getTable().getPartitionKeys();

    if (StringUtils.isNotBlank(hivePartition.getParameters().get(REPLACED_PARTITIONS_HIVE_METASTORE_KEY))) {

      // Partitions are separated by "|"
      for (String partitionsInfoString : Splitter.on("|").omitEmptyStrings().split(hivePartition.getParameters().get(REPLACED_PARTITIONS_HIVE_METASTORE_KEY))) {

        // Values for a partition are separated by ","
        List<String> partitionValues = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitionsInfoString);

        // Do not drop the partition being processed. Sometimes a partition may have replaced another partition of the same values.
        if (!partitionValues.equals(hivePartition.getValues())) {
          ImmutableMap.Builder<String, String> partitionDDLInfoMap = ImmutableMap.builder();
          for (int i = 0; i < partitionKeys.size(); i++) {
            partitionDDLInfoMap.put(partitionKeys.get(i).getName(), partitionValues.get(i));
          }
          replacedPartitionsDDLInfo.add(partitionDDLInfoMap.build());
        }
      }
    }
    return replacedPartitionsDDLInfo;
  }

  private Optional<Path> getDestinationPartitionLocation(Optional<Table> table, WorkUnitState state,
      String partitionName)
      throws DataConversionException {
    Optional<org.apache.hadoop.hive.metastore.api.Partition> partitionOptional =
        Optional.<org.apache.hadoop.hive.metastore.api.Partition>absent();
    if (!table.isPresent()) {
      return Optional.<Path>absent();
    }
    try {
      HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(state.getJobState().getProperties(),
          Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        partitionOptional =
            Optional.of(client.get().getPartition(table.get().getDbName(), table.get().getTableName(), partitionName));
      } catch (NoSuchObjectException e) {
        return Optional.<Path>absent();
      }
      if (partitionOptional.isPresent()) {
        org.apache.hadoop.hive.ql.metadata.Table qlTable = new org.apache.hadoop.hive.ql.metadata.Table(table.get());
        org.apache.hadoop.hive.ql.metadata.Partition qlPartition =
            new org.apache.hadoop.hive.ql.metadata.Partition(qlTable, partitionOptional.get());
        return Optional.of(qlPartition.getDataLocation());
      }
    } catch (IOException | TException | HiveException e) {
      throw new DataConversionException(
          String.format("Could not fetch destination table %s.%s metadata", table.get().getDbName(),
              table.get().getTableName()), e);
    }
    return Optional.<Path>absent();
  }
}
