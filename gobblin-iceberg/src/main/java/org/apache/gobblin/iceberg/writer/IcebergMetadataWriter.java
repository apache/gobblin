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

package org.apache.gobblin.iceberg.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.joda.time.DateTime;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.AutoCloseableHiveLock;
import org.apache.gobblin.hive.HiveLock;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.writer.MetadataWriter;
import org.apache.gobblin.iceberg.Utils.IcebergUtils;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metrics.GobblinMetricsRegistry;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.WriterUtils;


/**
 * This writer is used to calculate iceberg metadata from GMCE and register to iceberg
 * iceberg metadata here includes:
 * 1. Data files that contained by the table and the metrics of the data files
 * 2. Properties of the table (origin table properties, data offset range, high watermark of the GMCE and schema created time)
 * 3. Latest schema of the table
 */
@Slf4j
public class IcebergMetadataWriter implements MetadataWriter {

  public static final String USE_DATA_PATH_AS_TABLE_LOCATION = "use.data.path.as.table.location";
  public static final String TABLE_LOCATION_SUFFIX = "/_iceberg_metadata/%s";
  public static final String GMCE_HIGH_WATERMARK_KEY = "gmce.high.watermark.%s";
  public static final String GMCE_LOW_WATERMARK_KEY = "gmce.low.watermark.%s";
  private final static String EXPIRE_SNAPSHOTS_LOOKBACK_TIME = "gobblin.iceberg.dataset.expire.snapshots.lookBackTime";
  private final static String DEFAULT_EXPIRE_SNAPSHOTS_LOOKBACK_TIME = "3d";
  public static final String ICEBERG_REGISTRATION_BLACKLIST = "iceberg.registration.blacklist";
  public static final String ICEBERG_REGISTRATION_WHITELIST = "iceberg.registration.whitelist";
  public static final String ICEBERG_METADATA_FILE_PERMISSION = "iceberg.metadata.file.permission";
  private static final String CLUSTER_IDENTIFIER_KEY_NAME = "clusterIdentifier";
  private static final String CREATE_TABLE_TIME = "iceberg.create.table.time";
  private static final String SCHEMA_CREATION_TIME_KEY = "schema.creation.time";
  private static final String ADDED_FILES_CACHE_EXPIRING_TIME = "added.files.cache.expiring.time";
  private static final int DEFAULT_ADDED_FILES_CACHE_EXPIRING_TIME = 1;
  private static final String OFFSET_RANGE_KEY_PREFIX = "offset.range.";
  private static final String OFFSET_RANGE_KEY_FORMAT = OFFSET_RANGE_KEY_PREFIX + "%s";
  private static final String ICEBERG_FILE_PATH_COLUMN = "file_path";
  private static final String DEFAULT_CREATION_TIME = "0";
  private static final String SNAPSHOT_EXPIRE_THREADS = "snapshot.expire.threads";
  private static final long DEFAULT_WATERMARK = -1L;
  protected final MetricContext metricContext;
  protected EventSubmitter eventSubmitter;
  private final WhitelistBlacklist whiteistBlacklist;
  private final Closer closer = Closer.create();
  private final Map<TableIdentifier, Long> tableCurrentWaterMarkMap;
  //Used to store the relationship between table and the gmce topicPartition
  private final Map<TableIdentifier, String> tableTopicpartitionMap;
  @Getter
  private final KafkaSchemaRegistry schemaRegistry;
  private final Map<TableIdentifier, TableMetadata> tableMetadataMap;
  @Setter
  protected HiveCatalog catalog;
  protected final Configuration conf;
  protected final ReadWriteLock readWriteLock;
  private final HiveLock locks;
  private final ParallelRunner parallelRunner;
  private final boolean useDataLoacationAsTableLocation;
  private FsPermission permission;

  public IcebergMetadataWriter(State state) throws IOException {
    this.schemaRegistry = KafkaSchemaRegistry.get(state.getProperties());
    conf = HadoopUtils.getConfFromState(state);
    initializeCatalog();
    tableTopicpartitionMap = new HashMap<>();
    tableMetadataMap = new HashMap<>();
    tableCurrentWaterMarkMap = new HashMap<>();
    List<Tag<?>> tags = Lists.newArrayList();
    String clusterIdentifier = ClustersNames.getInstance().getClusterName();
    tags.add(new Tag<>(CLUSTER_IDENTIFIER_KEY_NAME, clusterIdentifier));
    metricContext = closer.register(
        GobblinMetricsRegistry.getInstance().getMetricContext(state, IcebergMetadataWriter.class, tags));
    this.eventSubmitter =
        new EventSubmitter.Builder(this.metricContext, IcebergMCEMetadataKeys.METRICS_NAMESPACE_ICEBERG_WRITER).build();
    this.whiteistBlacklist = new WhitelistBlacklist(state.getProp(ICEBERG_REGISTRATION_WHITELIST, ""),
        state.getProp(ICEBERG_REGISTRATION_BLACKLIST, ""));
    // Use lock to make it safe when flush and write are called async
    readWriteLock = new ReentrantReadWriteLock();
    this.locks = new HiveLock(state.getProperties());
    parallelRunner = closer.register(new ParallelRunner(state.getPropAsInt(SNAPSHOT_EXPIRE_THREADS, 20),
        FileSystem.get(HadoopUtils.getConfFromState(state))));
    useDataLoacationAsTableLocation = state.getPropAsBoolean(USE_DATA_PATH_AS_TABLE_LOCATION, false);
    if (useDataLoacationAsTableLocation) {
      permission =
          HadoopUtils.deserializeFsPermission(state, ICEBERG_METADATA_FILE_PERMISSION,
              FsPermission.getDefault());
    }
  }

  protected void initializeCatalog() {
    catalog = HiveCatalogs.loadCatalog(conf);
  }

  private org.apache.iceberg.Table getIcebergTable(TableIdentifier tid) throws NoSuchTableException {
    TableMetadata tableMetadata = tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata());
    if (!tableMetadata.table.isPresent()) {
      tableMetadata.table = Optional.of(catalog.loadTable(tid));
    }
    return tableMetadata.table.get();
  }

  /**
   *  The method is used to get current watermark of the gmce topic partition for a table
   *  Make the watermark config name contains topicPartition in case we change the gmce topic name for some reason
   */
  private Long getCurrentWaterMark(TableIdentifier tid, String topicPartition) {
    if (tableCurrentWaterMarkMap.containsKey(tid)) {
      return tableCurrentWaterMarkMap.get(tid);
    }
    org.apache.iceberg.Table icebergTable;
    Long currentWatermark = DEFAULT_WATERMARK;
    try {
      icebergTable = getIcebergTable(tid);
    } catch (NoSuchTableException e) {
      return currentWatermark;
    }
    currentWatermark =
        icebergTable.properties().containsKey(String.format(GMCE_HIGH_WATERMARK_KEY, topicPartition)) ? Long.parseLong(
            icebergTable.properties().get(String.format(GMCE_HIGH_WATERMARK_KEY, topicPartition))) : DEFAULT_WATERMARK;
    if (currentWatermark != DEFAULT_WATERMARK) {
      // set the low watermark for current snapshot
      tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata()).lowWatermark = Optional.of(currentWatermark);
    }
    return currentWatermark;
  }

  /**
   *The write method will be responsible for process a given gmce and aggregate the metadata
   * The logic of this function will be:
   * 1. Check whether a table exists, if not then create the iceberg table
   * 2. Compute schema from the gmce and update the cache for candidate schemas
   * 3. Do the required operation of the gmce, i.e. addFile, rewriteFile or dropFile. change_property means only
   * update the table level property but no data modification.
   * Note: this method only aggregate the metadata in cache without committing. The actual commit will be done in flush method
   */
  public void write(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {
    TableIdentifier tid = TableIdentifier.of(tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName());
    TableMetadata tableMetadata = tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata());
    Table table;
    try {
      table = getIcebergTable(tid);
    } catch (NoSuchTableException e) {
      try {
        if (gmce.getOperationType() == OperationType.drop_files ||
            gmce.getOperationType() == OperationType.change_property) {
          log.warn("Table {} does not exist, skip processing this {} event", tid.toString(), gmce.getOperationType());
          return;
        }
        table = createTable(gmce, tableSpec);
        tableMetadata.table = Optional.of(table);
      } catch (Exception e1) {
        log.error("skip processing {} for table {}.{} due to error when creating table", gmce.toString(),
            tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName());
        log.debug(e1.toString());
        return;
      }
    }
    computeCandidateSchema(gmce, tid, tableSpec);
    tableMetadata.transaction = Optional.of(tableMetadata.transaction.or(table::newTransaction));
    tableMetadata.lowestGMCEEmittedTime = Long.min(tableMetadata.lowestGMCEEmittedTime, gmce.getGMCEmittedTime());
    switch (gmce.getOperationType()) {
      case add_files: {
        updateTableProperty(tableSpec, tid);
        addFiles(gmce, newSpecsMap, table, tableMetadata);
        if (gmce.getTopicPartitionOffsetsRange() != null) {
          mergeOffsets(gmce, tid);
        }
        break;
      }
      case rewrite_files: {
        updateTableProperty(tableSpec, tid);
        rewriteFiles(gmce, newSpecsMap, oldSpecsMap, table, tableMetadata);
        break;
      }
      case drop_files: {
        dropFiles(gmce, oldSpecsMap, table, tableMetadata, tid);
        break;
      }
      case change_property: {
        updateTableProperty(tableSpec, tid);
        if (gmce.getTopicPartitionOffsetsRange() != null) {
          mergeOffsets(gmce, tid);
        }
        log.info("No file operation need to be performed by Iceberg Metadata Writer at this point.");
        break;
      }
      default: {
        log.error("unsupported operation {}", gmce.getOperationType().toString());
        return;
      }
    }
  }

  private HashMap<String, List<Range>> getLastOffset(TableMetadata tableMetadata) {
    HashMap<String, List<Range>> offsets = new HashMap<>();
    if (tableMetadata.lastProperties.isPresent()) {
      for (Map.Entry<String, String> entry : tableMetadata.lastProperties.get().entrySet()) {
        if (entry.getKey().startsWith(OFFSET_RANGE_KEY_PREFIX)) {
          List<Range> ranges = Arrays.asList(entry.getValue().split(ConfigurationKeys.LIST_DELIMITER_KEY))
              .stream()
              .map(s -> {
                List<String> rangePair = Splitter.on(ConfigurationKeys.RANGE_DELIMITER_KEY).splitToList(s);
                return Range.openClosed(Long.parseLong(rangePair.get(0)), Long.parseLong(rangePair.get(1)));})
              .collect(Collectors.toList());
          offsets.put(entry.getKey().substring(OFFSET_RANGE_KEY_PREFIX.length()), ranges);
        }
      }
    }
    return offsets;
  }

  private void mergeOffsets(GobblinMetadataChangeEvent gmce, TableIdentifier tid) throws IOException {
    TableMetadata tableMetadata = tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata());
    tableMetadata.dataOffsetRange = Optional.of(tableMetadata.dataOffsetRange.or(() -> getLastOffset(tableMetadata)));
    Map<String, List<Range>> offsets = tableMetadata.dataOffsetRange.get();
    for (Map.Entry<String, String> entry : gmce.getTopicPartitionOffsetsRange().entrySet()) {
      List<String> rangePair = Splitter.on(ConfigurationKeys.RANGE_DELIMITER_KEY).splitToList(entry.getValue());
      Range range = Range.openClosed(Long.parseLong(rangePair.get(0)), Long.parseLong(rangePair.get(1)));
      List<Range> existRanges = offsets.getOrDefault(entry.getKey(), new ArrayList<>());
      List<Range> newRanges = new ArrayList<>();
      for (Range r : existRanges) {
        if (range.isConnected(r)) {
          range = range.span(r);
        } else {
          newRanges.add(r);
        }
      }
      newRanges.add(range);
      Collections.sort(newRanges, new Comparator<Range>() {
        @Override
        public int compare(Range o1, Range o2) {
          return o1.lowerEndpoint().compareTo(o2.lowerEndpoint());
        }
      });
      offsets.put(entry.getKey(), newRanges);
    }
  }

  private void updateTableProperty(HiveSpec tableSpec, TableIdentifier tid) {
    org.apache.hadoop.hive.metastore.api.Table table = HiveMetaStoreUtils.getTable(tableSpec.getTable());
    tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata()).newProperties =
        Optional.of(IcebergUtils.getTableProperties(table));
  }

  /**
   * Compute the candidate schema from the gmce.
   * If the schema source is schemaRegistry, we will use the schema creation time as the schema version to compute candidate schema and determine latest schema
   * If the schema does not contain creation time, we will treat it the same as when schema source is event
   * If the schema source is event, we will put it as default creation time, during flush, if we only have one candidate with default creation time,
   * we'll use that to update schema.
   * @param gmce
   * @param tid
   */
  private void computeCandidateSchema(GobblinMetadataChangeEvent gmce, TableIdentifier tid, HiveSpec spec)
      throws IOException {
    Table table = getIcebergTable(tid);
    TableMetadata tableMetadata = tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata());
    org.apache.hadoop.hive.metastore.api.Table hiveTable = HiveMetaStoreUtils.getTable(spec.getTable());
    tableMetadata.lastProperties = Optional.of(tableMetadata.lastProperties.or(() -> table.properties()));
    Map<String, String> props = tableMetadata.lastProperties.get();
    tableMetadata.lastSchemaVersion = Optional.of(
        tableMetadata.lastSchemaVersion.or(() -> props.getOrDefault(SCHEMA_CREATION_TIME_KEY, DEFAULT_CREATION_TIME)));
    String lastSchemaVersion = tableMetadata.lastSchemaVersion.get();
    tableMetadata.candidateSchemas = Optional.of(tableMetadata.candidateSchemas.or(() -> CacheBuilder.newBuilder()
        .expireAfterAccess(conf.getInt(MetadataWriter.CACHE_EXPIRING_TIME,
            MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
        .build()));
    Cache<String, Schema> candidate = tableMetadata.candidateSchemas.get();
    try {
      switch (gmce.getSchemaSource()) {
        case SCHEMAREGISTRY: {
          org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(gmce.getTableSchema());
          String createdOn = AvroUtils.getSchemaCreationTime(schema);
          if (createdOn == null) {
            candidate.put(DEFAULT_CREATION_TIME,
                IcebergUtils.getIcebergSchema(gmce.getTableSchema(), hiveTable).tableSchema);
          } else if (!createdOn.equals(lastSchemaVersion)) {
            candidate.put(createdOn, IcebergUtils.getIcebergSchema(gmce.getTableSchema(), hiveTable).tableSchema);
          }
          break;
        }
        case EVENT: {
          candidate.put(DEFAULT_CREATION_TIME,
              IcebergUtils.getIcebergSchema(gmce.getTableSchema(), hiveTable).tableSchema);
          break;
        }
        case NONE: {
          log.debug("Schema source set to be none, will ignore the schema");
          break;
        }
        default: {
          throw new IOException(String.format("unsupported schema source %s", gmce.getSchemaSource()));
        }
      }
    } catch (Exception e) {
      log.error("Cannot get candidate schema from event due to", e);
    }
  }

  protected Table createTable(GobblinMetadataChangeEvent gmce, HiveSpec spec) throws IOException {
    String schema = gmce.getTableSchema();
    org.apache.hadoop.hive.metastore.api.Table table = HiveMetaStoreUtils.getTable(spec.getTable());
    IcebergUtils.IcebergDataAndPartitionSchema schemas = IcebergUtils.getIcebergSchema(schema, table);
    TableIdentifier tid = TableIdentifier.of(table.getDbName(), table.getTableName());
    Schema tableSchema = schemas.tableSchema;
    Preconditions.checkState(tableSchema != null, "Table schema cannot be null when creating a table");
    PartitionSpec partitionSpec = IcebergUtils.getPartitionSpec(tableSchema, schemas.partitionSchema);
    Table icebergTable = null;
    String tableLocation = null;
    if (useDataLoacationAsTableLocation) {
      tableLocation = gmce.getDatasetIdentifier().getNativeName() + String.format(TABLE_LOCATION_SUFFIX, table.getDbName());
      //Set the path permission
      Path tablePath = new Path(tableLocation);
      WriterUtils.mkdirsWithRecursivePermission(tablePath.getFileSystem(conf), tablePath, permission);
    }
    try (Timer.Context context = metricContext.timer(CREATE_TABLE_TIME).time()) {
      icebergTable =
          catalog.createTable(tid, tableSchema, partitionSpec, tableLocation, IcebergUtils.getTableProperties(table));
      log.info("Created table {}, schema: {} partition spec: {}", tid, tableSchema, partitionSpec);
    } catch (AlreadyExistsException e) {
      log.warn("table {} already exist, there may be some other process try to create table concurrently", tid);
    }
    return icebergTable;
  }

  protected void rewriteFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, Table table, TableMetadata tableMetadata) throws IOException {
    PartitionSpec partitionSpec = table.spec();
    tableMetadata.transaction = Optional.of(tableMetadata.transaction.or(table::newTransaction));
    Transaction transaction = tableMetadata.transaction.get();
    Set<DataFile> newDataFiles = new HashSet<>();
    getIcebergDataFilesToBeAddedHelper(gmce, table, newSpecsMap, tableMetadata)
        .forEach(dataFile -> {
          newDataFiles.add(dataFile);
          tableMetadata.addedFiles.put(dataFile.path(), "");
        });
    Set<DataFile> oldDataFiles = getIcebergDataFilesToBeDeleted(gmce, table, newSpecsMap, oldSpecsMap, partitionSpec);
    if (oldDataFiles.isEmpty() && !newDataFiles.isEmpty()) {
      //We randomly check whether one of the new data files already exists in the db to avoid duplication of re-write events
      DataFile file = newDataFiles.iterator().next();
      Expression exp = Expressions.startsWith(ICEBERG_FILE_PATH_COLUMN, (String) file.path());
      if (FindFiles.in(table).withMetadataMatching(exp).collect().iterator().hasNext()) {
        //This means this re-write event is duplicated with the one we already handled, so directly return
        return;
      }
      //This is the case when the files to be deleted do not exist in table
      //So we directly call addFiles interface to add new files into the table.
      tableMetadata.appendFiles = Optional.of(tableMetadata.appendFiles.or(transaction::newAppend));
      AppendFiles appendFiles = tableMetadata.appendFiles.get();
      newDataFiles.forEach(appendFiles::appendFile);
      return;
    }
    transaction.newRewrite().rewriteFiles(oldDataFiles, newDataFiles).commit();
  }

  /**
   * Given the GMCE, get the iceberg schema with the origin ID specified by data pipeline which
   * is corresponding to the file metrics index.
   * @param gmce GMCE emitted by data pipeline
   * @return iceberg schema with the origin ID
   */
  private Schema getSchemaWithOriginId(GobblinMetadataChangeEvent gmce) {
    Schema schemaWithOriginId = null;
    if (gmce.getAvroSchemaWithIcebergSchemaID() != null) {
      org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema =
          new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(gmce.getAvroSchemaWithIcebergSchemaID());
      schemaWithOriginId = AvroSchemaUtil.toIceberg(avroSchema);
    }
    return schemaWithOriginId;
  }

  protected void dropFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> oldSpecsMap, Table table,
      TableMetadata tableMetadata, TableIdentifier tid) throws IOException {
    PartitionSpec partitionSpec = table.spec();
    Transaction transaction = tableMetadata.transaction.get();
    Set<DataFile> oldDataFiles =
        getIcebergDataFilesToBeDeleted(gmce, table, new HashMap<>(), oldSpecsMap, partitionSpec);
    tableMetadata.deleteFiles = Optional.of(tableMetadata.deleteFiles.or(transaction::newDelete));
    DeleteFiles deleteFiles = tableMetadata.deleteFiles.get();
    oldDataFiles.stream().forEach(deleteFiles::deleteFile);
    parallelRunner.submitCallable(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          long olderThan = getExpireSnapshotTime();
          long start = System.currentTimeMillis();
          ExpireSnapshots expireSnapshots = table.expireSnapshots();
          final Table tmpTable = table;
          expireSnapshots.deleteWith(new Consumer<String>() {
            @Override
            public void accept(String file) {
              if (file.startsWith(tmpTable.location())) {
                tmpTable.io().deleteFile(file);
              }
            }
          }).expireOlderThan(olderThan).commit();
          //TODO: emit these metrics to Ingraphs, in addition to metrics for publishing new snapshots and other Iceberg metadata operations.
          log.info("Spent {} ms to expire snapshots older than {} ({}) in table {}", System.currentTimeMillis() - start,
              new DateTime(olderThan).toString(), olderThan, tid.toString());
        } catch (Exception e) {
          log.error(String.format("Fail to expire snapshots for table %s due to exception ", tid.toString()), e);
        }
        return null;
      }
    }, tid.toString());
  }

  private long getExpireSnapshotTime() {
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendYears()
        .appendSuffix("y")
        .appendMonths()
        .appendSuffix("M")
        .appendDays()
        .appendSuffix("d")
        .appendHours()
        .appendSuffix("h")
        .appendMinutes()
        .appendSuffix("m")
        .toFormatter();
    return DateTime.now()
        .minus(periodFormatter.parsePeriod(
            conf.get(EXPIRE_SNAPSHOTS_LOOKBACK_TIME, DEFAULT_EXPIRE_SNAPSHOTS_LOOKBACK_TIME)))
        .getMillis();
  }

  protected void addFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap, Table table,
      TableMetadata tableMetadata) throws IOException {
    Transaction transaction = tableMetadata.transaction.get();
    tableMetadata.appendFiles = Optional.of(tableMetadata.appendFiles.or(transaction::newAppend));
    AppendFiles appendFiles = tableMetadata.appendFiles.get();
    getIcebergDataFilesToBeAddedHelper(gmce, table, newSpecsMap, tableMetadata)
        .forEach(dataFile -> {
          appendFiles.appendFile(dataFile);
          tableMetadata.addedFiles.put(dataFile.path(), "");
        });
  }

  private Stream<DataFile> getIcebergDataFilesToBeAddedHelper(GobblinMetadataChangeEvent gmce, Table table, Map<String, Collection<HiveSpec>> newSpecsMap,
      TableMetadata tableMetadata) throws IOException {
    return getIcebergDataFilesToBeAdded(gmce.getNewFiles(), table.spec(), newSpecsMap,
        IcebergUtils.getSchemaIdMap(getSchemaWithOriginId(gmce), table.schema())).stream()
        .filter(dataFile -> tableMetadata.addedFiles.getIfPresent(dataFile.path()) == null);
  }

  /**
   * Method to get dataFiles without metrics information
   * This method is used to get files to be deleted from iceberg
   * If oldFilePrefixes is specified in gmce, this method will use those prefixes to find old file in iceberg,
   * or the method will callmethod {IcebergUtils.getIcebergDataFileWithMetric} to get DataFile for specific file path
   */
  private Set<DataFile> getIcebergDataFilesToBeDeleted(GobblinMetadataChangeEvent gmce, Table table,
      Map<String, Collection<HiveSpec>> newSpecsMap, Map<String, Collection<HiveSpec>> oldSpecsMap,
      PartitionSpec partitionSpec) throws IOException {
    Set<DataFile> oldDataFiles = new HashSet<>();
    if (gmce.getOldFilePrefixes() != null) {
      Expression exp = Expressions.alwaysFalse();
      for (String prefix : gmce.getOldFilePrefixes()) {
        //Use both full path and raw path to delete old files
        exp = Expressions.or(exp, Expressions.startsWith(ICEBERG_FILE_PATH_COLUMN, prefix));
        String rawPathPrefix = new Path(prefix).toUri().getRawPath();
        exp = Expressions.or(exp, Expressions.startsWith(ICEBERG_FILE_PATH_COLUMN, rawPathPrefix));
      }
      long start = System.currentTimeMillis();
      oldDataFiles.addAll(Sets.newHashSet(FindFiles.in(table).withMetadataMatching(exp).collect().iterator()));
      //Use INFO level log here to get better estimate.
      //This shouldn't overwhelm the log since we receive less than 3 rewrite_file gmces for one table in one day
      log.info("Spent {}ms to query all old files in iceberg.", System.currentTimeMillis() - start);
    } else {
      for (String file : gmce.getOldFiles()) {
        String specPath = new Path(file).getParent().toString();
        // For the use case of recompaction, the old path may contains /daily path, in this case, we find the spec from newSpecsMap
        StructLike partition = getIcebergPartition(
            oldSpecsMap.containsKey(specPath) ? oldSpecsMap.get(specPath) : newSpecsMap.get(specPath), file,
            partitionSpec);
        oldDataFiles.add(IcebergUtils.getIcebergDataFileWithoutMetric(file, partitionSpec, partition));
      }
    }
    return oldDataFiles;
  }

  /**
   * Method to get dataFiles with metrics information
   * This method is used to get files to be added to iceberg
   * This method will call method {IcebergUtils.getIcebergDataFileWithMetric} to get DataFile for specific file path
   */
  private Set<DataFile> getIcebergDataFilesToBeAdded(List<org.apache.gobblin.metadata.DataFile> files,
      PartitionSpec partitionSpec, Map<String, Collection<HiveSpec>> newSpecsMap, Map<Integer, Integer> schemaIdMap)
      throws IOException {
    Set<DataFile> dataFiles = new HashSet<>();
    for (org.apache.gobblin.metadata.DataFile file : files) {
      try {
        StructLike partition = getIcebergPartition(newSpecsMap.get(new Path(file.getFilePath()).getParent().toString()),
            file.getFilePath(), partitionSpec);
        dataFiles.add(IcebergUtils.getIcebergDataFileWithMetric(file, partitionSpec, partition, conf, schemaIdMap));
      } catch (Exception e) {
        log.warn("Cannot get DataFile for {} dur to {}", file.getFilePath(), e);
      }
    }
    return dataFiles;
  }

  private StructLike getIcebergPartition(Collection<HiveSpec> specs, String filePath, PartitionSpec partitionSpec)
      throws IOException {
    if (specs == null || specs.isEmpty()) {
      throw new IOException("Cannot get hive spec for " + filePath);
    }
    HivePartition hivePartition = specs.iterator().next().getPartition().orNull();
    StructLike partition = hivePartition == null ? null
        : IcebergUtils.getPartition(partitionSpec.partitionType(), hivePartition.getValues());
    return partition;
  }

  /**
   * For flush of each table, we do the following logic:
   * 1. Commit the appendFiles if it exist
   * 2. Update the new table property: high watermark of GMCE, data offset range, schema versions
   * 3. Update the schema
   * 4. Commit the transaction
   * 5. reset tableMetadata
   * @param dbName
   * @param tableName
   */
  @Override
  public void flush(String dbName, String tableName) throws IOException {
    Lock writeLock = readWriteLock.writeLock();
    writeLock.lock();
    try {
      TableIdentifier tid = TableIdentifier.of(dbName, tableName);
      TableMetadata tableMetadata = tableMetadataMap.getOrDefault(tid, new TableMetadata());
      if (tableMetadata.transaction.isPresent()) {
        Transaction transaction = tableMetadata.transaction.get();
        if (tableMetadata.appendFiles.isPresent()) {
          tableMetadata.appendFiles.get().commit();
        }
        if (tableMetadata.deleteFiles.isPresent()) {
          tableMetadata.deleteFiles.get().commit();
        }
        Map<String, String> props = tableMetadata.newProperties.or(
            Maps.newHashMap(tableMetadata.lastProperties.or(getIcebergTable(tid).properties())));
        //Set high waterMark
        Long highWatermark = tableCurrentWaterMarkMap.get(tid);
        props.put(String.format(GMCE_HIGH_WATERMARK_KEY, tableTopicpartitionMap.get(tid)), highWatermark.toString());
        //Set low waterMark
        props.put(String.format(GMCE_LOW_WATERMARK_KEY, tableTopicpartitionMap.get(tid)),
            tableMetadata.lowWatermark.get().toString());
        //Set whether to delete metadata files after commit
        props.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.toString(
            conf.getBoolean(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)));
        props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, Integer.toString(
            conf.getInt(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT)));
        //Set data offset range
        boolean containOffsetRange = setDatasetOffsetRange(tableMetadata, props);
        String topicName = tableName;
        if (containOffsetRange) {
          String topicPartitionString = tableMetadata.dataOffsetRange.get().keySet().iterator().next();
          //In case the topic name is not the table name or the topic name contains '-'
          topicName = topicPartitionString.substring(0, topicPartitionString.lastIndexOf('-'));
        }
        //Update schema
        updateSchema(tableMetadata, props, topicName);
        //Update properties
        UpdateProperties updateProperties = transaction.updateProperties();
        props.forEach(updateProperties::set);
        updateProperties.commit();
        try (AutoCloseableHiveLock lock = this.locks.getTableLock(dbName, tableName)) {
          transaction.commitTransaction();
        }
        Snapshot snapshot = tableMetadata.table.get().currentSnapshot();
        Map<String, String> currentProps = tableMetadata.table.get().properties();
        submitSnapshotCommitEvent(snapshot, tableMetadata, dbName, tableName, currentProps, highWatermark);
        //Reset the table metadata for next accumulation period
        tableMetadata.reset(currentProps, highWatermark);
        log.info(String.format("Finish flushing for table %s", tid.toString()));
      }
    } catch (RuntimeException e) {
      throw new RuntimeException(String.format("Fail to flush table %s %s", dbName, tableName), e);
    } catch (Exception e) {
      throw new IOException(String.format("Fail to flush table %s %s", dbName, tableName), e);
    } finally {
      writeLock.unlock();
    }
  }

  private void submitSnapshotCommitEvent(Snapshot snapshot, TableMetadata tableMetadata, String dbName,
      String tableName, Map<String, String> props, Long highWaterMark) {
    GobblinEventBuilder gobblinTrackingEvent =
        new GobblinEventBuilder(IcebergMCEMetadataKeys.ICEBERG_COMMIT_EVENT_NAME);
    long currentSnapshotID = snapshot.snapshotId();
    long endToEndLag = System.currentTimeMillis() - tableMetadata.lowestGMCEEmittedTime;
    TableIdentifier tid = TableIdentifier.of(dbName, tableName);
    String gmceTopicPartition = tableTopicpartitionMap.get(tid);

    //Add information to automatically trigger repair jon when data loss happen
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.GMCE_TOPIC_NAME, gmceTopicPartition.split("-")[0]);
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.GMCE_TOPIC_PARTITION, gmceTopicPartition.split("-")[1]);
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.GMCE_HIGH_WATERMARK, highWaterMark.toString());
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.GMCE_LOW_WATERMARK,
        tableMetadata.lowWatermark.get().toString());

    //Add information for lag monitoring
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.LAG_KEY_NAME, Long.toString(endToEndLag));
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.SNAPSHOT_KEY_NAME, Long.toString(currentSnapshotID));
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.MANIFEST_LOCATION, snapshot.manifestListLocation());
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.SNAPSHOT_INFORMATION_KEY_NAME,
        Joiner.on(",").withKeyValueSeparator("=").join(snapshot.summary()));
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.TABLE_KEY_NAME, tableName);
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.DATABASE_KEY_NAME, dbName);
    gobblinTrackingEvent.addMetadata(IcebergMCEMetadataKeys.DATASET_HDFS_PATH, tableMetadata.datasetName);
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (entry.getKey().startsWith(OFFSET_RANGE_KEY_PREFIX)) {
        gobblinTrackingEvent.addMetadata(entry.getKey(), entry.getValue());
      }
    }
    eventSubmitter.submit(gobblinTrackingEvent);
  }

  private boolean setDatasetOffsetRange(TableMetadata tableMetadata, Map<String, String> props) {
    if (tableMetadata.dataOffsetRange.isPresent() && !tableMetadata.dataOffsetRange.get().isEmpty()) {
      for (Map.Entry<String, List<Range>> offsets : tableMetadata.dataOffsetRange.get().entrySet()) {
        List<Range> ranges = offsets.getValue();
        String rangeString = ranges.stream()
            .map(r -> Joiner.on(ConfigurationKeys.RANGE_DELIMITER_KEY).join(r.lowerEndpoint(), r.upperEndpoint()))
            .collect(Collectors.joining(ConfigurationKeys.LIST_DELIMITER_KEY));
        props.put(String.format(OFFSET_RANGE_KEY_FORMAT, offsets.getKey()), rangeString);
      }
      return true;
    }
    return false;
  }

  private void updateSchema(TableMetadata tableMetadata, Map<String, String> props, String topicName) {
    //Set default schema versions
    props.put(SCHEMA_CREATION_TIME_KEY, tableMetadata.lastSchemaVersion.or(DEFAULT_CREATION_TIME));
    // Update Schema
    try {
      if (tableMetadata.candidateSchemas.isPresent() && tableMetadata.candidateSchemas.get().size() > 0) {
        Cache candidates = tableMetadata.candidateSchemas.get();
        //Only have default schema, so either we calculate schema from event or the schema does not have creation time, directly update it
        if (candidates.size() == 1 && candidates.getIfPresent(DEFAULT_CREATION_TIME) != null) {
          updateSchemaHelper(DEFAULT_CREATION_TIME, (Schema) candidates.getIfPresent(DEFAULT_CREATION_TIME), props,
              tableMetadata.table.get());
        } else {
          //update schema if candidates contains the schema that has the same creation time with the latest schema
          org.apache.avro.Schema latestSchema =
              (org.apache.avro.Schema) schemaRegistry.getLatestSchemaByTopic(topicName);
          String creationTime = AvroUtils.getSchemaCreationTime(latestSchema);
          if (creationTime == null) {
            log.warn(
                "Schema from schema registry does not contain creation time, check config for schema registry class");
          } else if (candidates.getIfPresent(creationTime) != null) {
            updateSchemaHelper(creationTime, (Schema) candidates.getIfPresent(creationTime), props,
                tableMetadata.table.get());
          }
        }
      }
    } catch (SchemaRegistryException e) {
      log.error("Cannot get schema form schema registry, will not update this schema", e);
    }
  }

  private void updateSchemaHelper(String schemaCreationTime, Schema schema, Map<String, String> props, Table table) {
    try {
      table.updateSchema().unionByNameWith(schema).commit();
      props.put(SCHEMA_CREATION_TIME_KEY, schemaCreationTime);
    } catch (Exception e) {
      log.error("Cannot update schema to " + schema.toString() + "for table " + table.location(), e);
    }
  }

  @Override
  public void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {
    Lock readLock = readWriteLock.readLock();
    readLock.lock();
    try {
      GenericRecord genericRecord = recordEnvelope.getRecord();
      GobblinMetadataChangeEvent gmce =
          (GobblinMetadataChangeEvent) SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
      if (whiteistBlacklist.acceptTable(tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName())) {
        TableIdentifier tid = TableIdentifier.of(tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName());
        String topicPartition = tableTopicpartitionMap.computeIfAbsent(tid,
            t -> ((KafkaStreamingExtractor.KafkaWatermark) recordEnvelope.getWatermark()).getTopicPartition().toString());
        Long currentWatermark = getCurrentWaterMark(tid, topicPartition);
        Long currentOffset =
            ((KafkaStreamingExtractor.KafkaWatermark) recordEnvelope.getWatermark()).getLwm().getValue();
        if (currentOffset > currentWatermark) {
          if (currentWatermark == DEFAULT_WATERMARK) {
            //This means we haven't register this table or the GMCE topic partition changed, we need to reset the low watermark
            tableMetadataMap.computeIfAbsent(tid, t -> new TableMetadata()).lowWatermark =
                Optional.of(currentOffset - 1);
          }
          tableMetadataMap.get(tid).datasetName = gmce.getDatasetIdentifier().getNativeName();
          write(gmce, newSpecsMap, oldSpecsMap, tableSpec);
          tableCurrentWaterMarkMap.put(tid,
              ((KafkaStreamingExtractor.KafkaWatermark) recordEnvelope.getWatermark()).getLwm().getValue());
        } else {
          log.warn(String.format("Skip processing record %s since it has lower watermark", genericRecord.toString()));
        }
      } else {
        log.info(String.format("Skip table %s.%s since it's blacklisted", tableSpec.getTable().getDbName(),
            tableSpec.getTable().getTableName()));
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  private class TableMetadata {
    Optional<Transaction> transaction = Optional.absent();
    Optional<Table> table = Optional.absent();
    Optional<AppendFiles> appendFiles = Optional.absent();
    Optional<DeleteFiles> deleteFiles = Optional.absent();
    Optional<Map<String, String>> lastProperties = Optional.absent();
    Optional<Map<String, String>> newProperties = Optional.absent();
    Optional<Cache<String, Schema>> candidateSchemas = Optional.absent();
    Optional<Map<String, List<Range>>> dataOffsetRange = Optional.absent();
    Optional<String> lastSchemaVersion = Optional.absent();
    Optional<Long> lowWatermark = Optional.absent();
    String datasetName;
    Cache<CharSequence, String> addedFiles = CacheBuilder.newBuilder()
        .expireAfterAccess(conf.getInt(ADDED_FILES_CACHE_EXPIRING_TIME, DEFAULT_ADDED_FILES_CACHE_EXPIRING_TIME),
            TimeUnit.HOURS)
        .build();
    long lowestGMCEEmittedTime = Long.MAX_VALUE;

    public void reset(Map<String, String> props, Long lowWaterMark) {
      this.lastProperties = Optional.of(props);
      this.lastSchemaVersion = Optional.of(props.get(SCHEMA_CREATION_TIME_KEY));
      //clear cache
      this.transaction = Optional.absent();
      this.deleteFiles = Optional.absent();
      this.appendFiles = Optional.absent();
      this.candidateSchemas = Optional.absent();
      this.dataOffsetRange = Optional.absent();
      this.newProperties = Optional.absent();
      this.lowestGMCEEmittedTime = Long.MAX_VALUE;
      this.lowWatermark = Optional.of(lowWaterMark);
    }
  }
}
