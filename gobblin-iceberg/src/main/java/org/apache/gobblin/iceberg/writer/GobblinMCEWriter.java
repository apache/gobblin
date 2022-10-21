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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.commons.collections.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.writer.HiveMetadataWriterWithPartitionInfoException;
import org.apache.gobblin.hive.writer.MetadataWriterKeys;
import org.apache.gobblin.hive.writer.MetadataWriter;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metadata.DataFile;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;


/**
 * This is a wrapper of all the MetadataWriters. This writer will manage a list of {@Link MetadataWriter} which do the actual
 * metadata registration for different metadata store.
 * This writer is responsible for:
 * 0. Consuming {@link GobblinMetadataChangeEvent} and execute metadata registration.
 * 1. Managing a map of Iceberg tables that it is currently processing
 * 2. Ensuring that the underlying metadata writers flush the metadata associated with each Iceberg table
 * 3. Call flush method for a specific table on a change in operation type
 * 4. Calculate {@Link HiveSpec}s and pass them to metadata writers to register metadata
 */
@SuppressWarnings("UnstableApiUsage")
@Slf4j
public class GobblinMCEWriter implements DataWriter<GenericRecord> {
  public static final String GOBBLIN_MCE_WRITER_METRIC_NAMESPACE = GobblinMCEWriter.class.getCanonicalName();
  public static final String DEFAULT_HIVE_REGISTRATION_POLICY_KEY = "default.hive.registration.policy";
  public static final String FORCE_HIVE_DATABASE_NAME = "force.hive.database.name";
  public static final String ACCEPTED_CLUSTER_NAMES = "accepted.cluster.names";
  public static final String METADATA_REGISTRATION_THREADS = "metadata.registration.threads";
  public static final String METADATA_PARALLEL_RUNNER_TIMEOUT_MILLS = "metadata.parallel.runner.timeout.mills";
  public static final String HIVE_PARTITION_NAME = "hive.partition.name";
  public static final String GMCE_METADATA_WRITER_CLASSES = "gmce.metadata.writer.classes";
  public static final String GMCE_METADATA_WRITER_MAX_ERROR_DATASET = "gmce.metadata.writer.max.error.dataset";
  public static final String TRANSIENT_EXCEPTION_MESSAGES_KEY = "gmce.metadata.writer.transient.exception.messages";
  public static final int DEFUALT_GMCE_METADATA_WRITER_MAX_ERROR_DATASET = 0;
  public static final int DEFAULT_ICEBERG_PARALLEL_TIMEOUT_MILLS = 60000;
  public static final String TABLE_NAME_DELIMITER = ".";
  @Getter
  List<MetadataWriter> metadataWriters;
  Map<String, TableStatus> tableOperationTypeMap;
  @Getter
  Map<String, Map<String, List<GobblinMetadataException>>> datasetErrorMap;
  Set<String> acceptedClusters;
  protected State state;
  private final ParallelRunner parallelRunner;
  private int parallelRunnerTimeoutMills;
  private Map<String, Cache<String, Collection<HiveSpec>>> oldSpecsMaps;
  private Map<String, Cache<String, Collection<HiveSpec>>> newSpecsMaps;
  private Map<String, List<HiveRegistrationUnit.Column>> partitionKeysMap;
  private Closer closer = Closer.create();
  protected final AtomicLong recordCount = new AtomicLong(0L);
  @Setter
  private int maxErrorDataset;
  protected EventSubmitter eventSubmitter;
  private final Set<String> transientExceptionMessages;

  @AllArgsConstructor
  static class TableStatus {
    OperationType operationType;
    String datasetPath;
    String gmceTopicPartition;
    long gmceLowWatermark;
    long gmceHighWatermark;
  }

  GobblinMCEWriter(DataWriterBuilder<Schema, GenericRecord> builder, State properties) throws IOException {
    newSpecsMaps = new HashMap<>();
    oldSpecsMaps = new HashMap<>();
    metadataWriters = new ArrayList<>();
    datasetErrorMap = new HashMap<>();
    partitionKeysMap = new HashMap<>();
    acceptedClusters = properties.getPropAsSet(ACCEPTED_CLUSTER_NAMES, ClustersNames.getInstance().getClusterName());
    state = properties;
    maxErrorDataset = state.getPropAsInt(GMCE_METADATA_WRITER_MAX_ERROR_DATASET, DEFUALT_GMCE_METADATA_WRITER_MAX_ERROR_DATASET);
    for (String className : state.getPropAsList(GMCE_METADATA_WRITER_CLASSES, IcebergMetadataWriter.class.getName())) {
      metadataWriters.add(closer.register(GobblinConstructorUtils.invokeConstructor(MetadataWriter.class, className, state)));
    }
    tableOperationTypeMap = new HashMap<>();
    parallelRunner = closer.register(new ParallelRunner(state.getPropAsInt(METADATA_REGISTRATION_THREADS, 20),
        FileSystem.get(HadoopUtils.getConfFromState(properties))));
    parallelRunnerTimeoutMills =
        state.getPropAsInt(METADATA_PARALLEL_RUNNER_TIMEOUT_MILLS, DEFAULT_ICEBERG_PARALLEL_TIMEOUT_MILLS);
    List<Tag<?>> tags = Lists.newArrayList();
    String clusterIdentifier = ClustersNames.getInstance().getClusterName();
    tags.add(new Tag<>(MetadataWriterKeys.CLUSTER_IDENTIFIER_KEY_NAME, clusterIdentifier));
    MetricContext metricContext = Instrumented.getMetricContext(state, this.getClass(), tags);
    eventSubmitter = new EventSubmitter.Builder(metricContext, GOBBLIN_MCE_WRITER_METRIC_NAMESPACE).build();
    transientExceptionMessages = new HashSet<>(properties.getPropAsList(TRANSIENT_EXCEPTION_MESSAGES_KEY, ""));
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    //Do nothing

  }

  /**
   * This method is used to get the specs map for a list of files. It will firstly look up in cache
   * to see if the specs has been calculated previously to reduce the computing time
   * We have an assumption here: "for the same path and the same operation type, the specs should be the same"
   * @param files  List of leaf-level files' names
   * @param specsMap The specs map for the files
   * @param cache  Cache that store the pre-calculated specs information
   * @param registerState State used to compute the specs
   * @param isPrefix If false,  we get the parent file name to calculate the hiveSpec. This is to comply with
   *                 hive's convention on partition which is the parent folder for leaf-level files.
   * @throws IOException
   */
  private void computeSpecMap(List<String> files, ConcurrentHashMap<String, Collection<HiveSpec>> specsMap,
      Cache<String, Collection<HiveSpec>> cache, State registerState, boolean isPrefix) throws IOException {
    HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(registerState);
    for (String file : files) {
      parallelRunner.submitCallable(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            Path regPath = isPrefix ? new Path(file) : new Path(file).getParent();
            //Use raw path to comply with HDFS federation setting.
            Path rawPath = new Path(regPath.toUri().getRawPath());
            specsMap.put(regPath.toString(), cache.get(regPath.toString(), () -> policy.getHiveSpecs(rawPath)));
          } catch (Throwable e) {
            //todo: Emit failed GMCE in the future to easily track the error gmce and investigate the reason for that.
            log.warn("Cannot get Hive Spec for {} using policy {} due to:", file, policy.toString());
            log.warn(e.getMessage());
          }
          return null;
        }
      }, file);
    }
    parallelRunner.waitForTasks(parallelRunnerTimeoutMills);
  }

  @Override
  public void commit() throws IOException {
    this.flush();
  }

  @Override
  public void cleanup() throws IOException {
    //do nothing
  }

  @Override
  public long recordsWritten() {
    return recordCount.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }

  @Override
  public Descriptor getDataDescriptor() {
    return null;
  }

  @Override
  public void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope) throws IOException {
    GenericRecord genericRecord = recordEnvelope.getRecord();
    CheckpointableWatermark watermark = recordEnvelope.getWatermark();
    Preconditions.checkNotNull(watermark);
    //filter out the events that not emitted by accepted clusters
    if (!acceptedClusters.contains(genericRecord.get("cluster"))) {
      return;
    }
    // Use schema from record to avoid issue when schema evolution
    GobblinMetadataChangeEvent gmce =
        (GobblinMetadataChangeEvent) SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
    String datasetName = gmce.getDatasetIdentifier().toString();
    //remove the old hive spec cache after flush
    //Here we assume that new hive spec for one path always be the same(ingestion flow register to same tables)
    oldSpecsMaps.remove(datasetName);

    // Mapping from URI of path of arrival files to the list of HiveSpec objects.
    // We assume in one same operation interval, for same dataset, the table property will not change to reduce the time to compute hiveSpec.
    ConcurrentHashMap<String, Collection<HiveSpec>> newSpecsMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Collection<HiveSpec>> oldSpecsMap = new ConcurrentHashMap<>();

    if (gmce.getNewFiles() != null) {
      State registerState = setHiveRegProperties(state, gmce, true);
      computeSpecMap(Lists.newArrayList(Iterables.transform(gmce.getNewFiles(), DataFile::getFilePath)),
          newSpecsMap, newSpecsMaps.computeIfAbsent(datasetName, t -> CacheBuilder.newBuilder()
              .expireAfterAccess(state.getPropAsInt(MetadataWriter.CACHE_EXPIRING_TIME,
                  MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
              .build()), registerState, false);
    }
    if (gmce.getOldFilePrefixes() != null) {
      State registerState = setHiveRegProperties(state, gmce, false);
      computeSpecMap(gmce.getOldFilePrefixes(), oldSpecsMap, oldSpecsMaps.computeIfAbsent(datasetName, t -> CacheBuilder
          .newBuilder()
          .expireAfterAccess(state.getPropAsInt(MetadataWriter.CACHE_EXPIRING_TIME,
              MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
          .build()), registerState, true);
    } else if (gmce.getOldFiles() != null) {
      State registerState = setHiveRegProperties(state, gmce, false);
      computeSpecMap(gmce.getOldFiles(), oldSpecsMap, oldSpecsMaps.computeIfAbsent(datasetName,
          t -> CacheBuilder.newBuilder()
              .expireAfterAccess(state.getPropAsInt(MetadataWriter.CACHE_EXPIRING_TIME,
                  MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
              .build()), registerState, false);
    }
    if (newSpecsMap.isEmpty() && oldSpecsMap.isEmpty()) {
      return;
    }

    // Sample one entry among all "Path <--> List<HiveSpec>" pair is good enough, reasoning:
    // 0. Objective here is to execute metadata registration for all target table destinations of a dataset,
    // 1. GMCE guarantees all paths coming from single dataset (but not necessary single "partition" in Hive's layout),
    // 2. HiveSpec of paths from a dataset should be targeting at the same set of table destinations,
    // 3. therefore fetching one path's HiveSpec and iterate through it is good enough to cover all table destinations.
    Collection<HiveSpec> specs =
        newSpecsMap.isEmpty() ? oldSpecsMap.values().iterator().next() : newSpecsMap.values().iterator().next();
    for (HiveSpec spec : specs) {
      String dbName = spec.getTable().getDbName();
      String tableName = spec.getTable().getTableName();
      String tableString = Joiner.on(TABLE_NAME_DELIMITER).join(dbName, tableName);
      partitionKeysMap.put(tableString, spec.getTable().getPartitionKeys());
      if (!tableOperationTypeMap.containsKey(tableString)) {
        tableOperationTypeMap.put(tableString, new TableStatus(gmce.getOperationType(),
            gmce.getDatasetIdentifier().getNativeName(), watermark.getSource(),
            ((LongWatermark)watermark.getWatermark()).getValue()-1, ((LongWatermark)watermark.getWatermark()).getValue()));
      } else if (tableOperationTypeMap.get(tableString).operationType != gmce.getOperationType() && gmce.getOperationType() != OperationType.change_property) {
        flush(dbName, tableName);
        tableOperationTypeMap.put(tableString, new TableStatus(gmce.getOperationType(),
            gmce.getDatasetIdentifier().getNativeName(), watermark.getSource(),
            ((LongWatermark)watermark.getWatermark()).getValue()-1, ((LongWatermark)watermark.getWatermark()).getValue()));
      }
      tableOperationTypeMap.get(tableString).gmceHighWatermark = ((LongWatermark)watermark.getWatermark()).getValue();

      List<MetadataWriter> allowedWriters = getAllowedMetadataWriters(gmce, metadataWriters);
      writeWithMetadataWriters(recordEnvelope, allowedWriters, newSpecsMap, oldSpecsMap, spec);
    }
    this.recordCount.incrementAndGet();
  }

  /**
   * Entry point for calling the allowed metadata writers specified in the GMCE
   * Adds fault tolerant ability and make sure we can emit GTE as desired
   * Visible for testing because the WriteEnvelope method has complicated hive logic
   * @param recordEnvelope
   * @param allowedWriters metadata writers that will be written to
   * @param newSpecsMap
   * @param oldSpecsMap
   * @param spec
   * @throws IOException when max number of dataset errors is exceeded
   */
  @VisibleForTesting
  void writeWithMetadataWriters(
      RecordEnvelope<GenericRecord> recordEnvelope,
      List<MetadataWriter> allowedWriters,
      ConcurrentHashMap newSpecsMap,
      ConcurrentHashMap oldSpecsMap,
      HiveSpec spec
  ) throws IOException {
    boolean meetException = false;
    String dbName = spec.getTable().getDbName();
    String tableName = spec.getTable().getTableName();
    String tableString = Joiner.on(TABLE_NAME_DELIMITER).join(dbName, tableName);
    for (MetadataWriter writer : allowedWriters) {
      if (meetException) {
        writer.reset(dbName, tableName);
      } else {
        try {
          writer.writeEnvelope(recordEnvelope, newSpecsMap, oldSpecsMap, spec);
        } catch (Exception e) {
          if (isExceptionTransient(e, transientExceptionMessages)) {
            throw new RuntimeException("Failing container due to transient exception for db: " + dbName + " table: " + tableName, e);
          }
          meetException = true;
          writer.reset(dbName, tableName);
          addOrThrowException(e, tableString, dbName, tableName, getFailedWriterList(writer));
        }
      }
    }
  }

  /**
   *   All metadata writers will be returned if no metadata writers are specified in gmce
   * @param gmce
   * @param metadataWriters
   * @return The metadata writers allowed as specified by GMCE. Relative order of {@code metadataWriters} is maintained
   */
  @VisibleForTesting
  static List<MetadataWriter> getAllowedMetadataWriters(GobblinMetadataChangeEvent gmce, List<MetadataWriter> metadataWriters) {
    if (CollectionUtils.isEmpty(gmce.getAllowedMetadataWriters())) {
      return metadataWriters;
    }

    Set<String> allowSet = new HashSet<>(gmce.getAllowedMetadataWriters());
    return metadataWriters.stream()
        .filter(writer -> allowSet.contains(writer.getClass().getName()))
        .collect(Collectors.toList());
  }

  private void addOrThrowException(Exception e, String tableString, String dbName, String tableName, List<String> failedWriters) throws IOException {
    TableStatus tableStatus = tableOperationTypeMap.get(tableString);
    Map<String, List<GobblinMetadataException>> tableErrorMap = this.datasetErrorMap.getOrDefault(tableStatus.datasetPath, new HashMap<>());
    GobblinMetadataException lastException = null;
    if (tableErrorMap.containsKey(tableString) && !tableErrorMap.get(tableString).isEmpty()) {
      lastException = tableErrorMap.get(tableString).get(tableErrorMap.get(tableString).size() - 1);
    } else {
      tableErrorMap.put(tableString, new ArrayList<>());
    }
    // If operationType has changed, add a new exception to the list so that each failure event represents an offset range all containing the same operation
    if (lastException != null && lastException.operationType.equals(tableStatus.operationType)) {
      lastException.highWatermark = tableStatus.gmceHighWatermark;
    } else {
      lastException = new GobblinMetadataException(tableStatus.datasetPath, dbName, tableName, tableStatus.gmceTopicPartition,
          tableStatus.gmceLowWatermark, tableStatus.gmceHighWatermark, failedWriters, tableStatus.operationType, partitionKeysMap.get(tableString), e);
      tableErrorMap.get(tableString).add(lastException);
    }
    if (e instanceof HiveMetadataWriterWithPartitionInfoException) {
      lastException.addedPartitionValues.addAll(((HiveMetadataWriterWithPartitionInfoException) e).addedPartitionValues);
      lastException.droppedPartitionValues.addAll(((HiveMetadataWriterWithPartitionInfoException) e).droppedPartitionValues);
    }
    this.datasetErrorMap.put(tableStatus.datasetPath, tableErrorMap);
    log.error(String.format("Meet exception when flush table %s", tableString), e);
    if (datasetErrorMap.size() > maxErrorDataset) {
      //Fail the job if the error size exceeds some number
      throw new IOException(String.format("Container fails to flush for more than %s dataset, last exception we met is: ", maxErrorDataset), e);
    }
  }

  // Add fault tolerant ability and make sure we can emit GTE as desired
  private void flush(String dbName, String tableName) throws IOException {
    boolean meetException = false;
    String tableString = Joiner.on(TABLE_NAME_DELIMITER).join(dbName, tableName);
    if (tableOperationTypeMap.get(tableString).gmceLowWatermark == tableOperationTypeMap.get(tableString).gmceHighWatermark) {
      // No need to flush
      return;
    }
    for (MetadataWriter writer : metadataWriters) {
      if(meetException) {
        writer.reset(dbName, tableName);
      } else {
        try {
          writer.flush(dbName, tableName);
        } catch (IOException e) {
          if (isExceptionTransient(e, transientExceptionMessages)) {
            throw new RuntimeException("Failing container due to transient exception for db: " + dbName + " table: " + tableName, e);
          }
          meetException = true;
          writer.reset(dbName, tableName);
          addOrThrowException(e, tableString, dbName, tableName, getFailedWriterList(writer));
        }
      }
    }
    if (!meetException) {
      String datasetPath = tableOperationTypeMap.get(tableString).datasetPath;
      if (datasetErrorMap.containsKey(datasetPath) && datasetErrorMap.get(datasetPath).containsKey(tableString)) {
        // We only want to emit GTE when the table watermark moves. There can be two scenario that watermark move, one is after one flush interval,
        // we commit new watermark to state store, anther is here, where during the flush interval, we flush table because table operation changes.
        // Under this condition, error map contains this dataset means we met error before this flush, but this time when flush succeed and
        // the watermark inside the table moves, so we want to emit GTE to indicate there is some data loss here
        submitFailureEvents(datasetErrorMap.get(datasetPath).get(tableString));
        this.datasetErrorMap.get(datasetPath).remove(tableString);
      }
    }
  }

  /**
   * Check if exception is contained within a known list of transient exceptions. These exceptions should not be caught
   * to avoid advancing watermarks and skipping GMCEs unnecessarily.
   */
  public static boolean isExceptionTransient(Exception e, Set<String> transientExceptionMessages) {
    return transientExceptionMessages.stream().anyMatch(message -> Throwables.getRootCause(e).toString().contains(message));
  }

  /**
   * Call the metadata writers to do flush each table metadata.
   * Flush of metadata writer is the place that do real metadata
   * registrations (e.g. for iceberg, this method will generate a snapshot)
   * Flush of metadata writer is the place that do really metadata
   * registration (For iceberg, this method will generate a snapshot)
   *
   * Note that this is one of the place where the materialization of aggregated metadata happens.
   * When there's a change of {@link OperationType}, it also interrupts metadata aggregation,
   * and triggers materialization of metadata.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    log.info(String.format("begin flushing %s records", String.valueOf(recordCount.get())));
    for (String tableString : tableOperationTypeMap.keySet()) {
      List<String> tid = Splitter.on(TABLE_NAME_DELIMITER).splitToList(tableString);
      flush(tid.get(0), tid.get(1));
    }
    tableOperationTypeMap.clear();
    recordCount.lazySet(0L);
    // Emit events for all current errors, since the GMCE watermark will be advanced
    for (Map.Entry<String, Map<String, List<GobblinMetadataException>>> entry : datasetErrorMap.entrySet()) {
      for (List<GobblinMetadataException> exceptionList : entry.getValue().values()) {
        submitFailureEvents(exceptionList);
      }
      entry.getValue().clear();
    }
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  /**
   * Get a new state object with the properties used to calculate the {@Link HiveSpec} set
   * First set the policy to compute HiveSpec, if the policy is not specified in record,
   * then use the default policy defined in the pipeline
   * Then set the {@Link ConfigurationKeys.HIVE_REGISTRATION_POLICY}  and SCHEMA_LITERAL
   */
  public static State setHiveRegProperties(State state, GobblinMetadataChangeEvent gmce, boolean forNew) {
    Preconditions.checkArgument(state.contains(DEFAULT_HIVE_REGISTRATION_POLICY_KEY),
        String.format("Missing required configuration %s", DEFAULT_HIVE_REGISTRATION_POLICY_KEY));
    String defaultPolicy = state.getProp(DEFAULT_HIVE_REGISTRATION_POLICY_KEY);
    State tmpState = new State(state);
    String policyClass = forNew ? (gmce.getRegistrationPolicy() != null ? gmce.getRegistrationPolicy() : defaultPolicy)
        : (gmce.getRegistrationPolicyForOldData() != null ? gmce.getRegistrationPolicyForOldData() : defaultPolicy);

    tmpState.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY, policyClass);
    if (!forNew) {
      //For old data, we don't use the old spec to update table, we set the flag to true to avoid listing operation
      tmpState.setProp(HiveRegistrationPolicy.MAPREDUCE_JOB_INPUT_PATH_EMPTY_KEY, true);
    }
    if (gmce.getPartitionColumns() != null && !gmce.getPartitionColumns().isEmpty()) {
      //We only support on partition column for now
      //TODO: Support multi partition columns
      tmpState.setProp(HIVE_PARTITION_NAME, String.join(",", gmce.getPartitionColumns()));
    }
    if (gmce.getRegistrationProperties() != null) {
      for (Map.Entry<String, String> entry : gmce.getRegistrationProperties().entrySet()) {
        tmpState.setProp(entry.getKey(), entry.getValue());
      }
    }
    //This config will force all the GMCE to register into the FORCE_HIVE_DATABASE_NAME
    //This is mainly used during verification period that we register all iceberg data under iceberg_test
    if (state.contains(FORCE_HIVE_DATABASE_NAME)) {
       tmpState.setProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME, state.getProp(FORCE_HIVE_DATABASE_NAME));
    }
    //Set schema
    if (gmce.getTableSchema() != null) {
      tmpState.setProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), gmce.getTableSchema());
    }
    return tmpState;
  }

  /**
   * Submit events indicating that a specific set of GMCEs have been skipped, so there is a gap in the registration
   */
  private void submitFailureEvents(List<GobblinMetadataException> exceptionList) {
    if (exceptionList.isEmpty()) {
      return;
    }
    log.warn(String.format("Sending GTEs to indicate table flush failure for %s.%s", exceptionList.get(0).dbName, exceptionList.get(0).tableName));

    for (GobblinMetadataException exception : exceptionList) {
      GobblinEventBuilder gobblinTrackingEvent = new GobblinEventBuilder(MetadataWriterKeys.METADATA_WRITER_FAILURE_EVENT);

      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.DATASET_HDFS_PATH, exception.datasetPath);
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.DATABASE_NAME_KEY, exception.dbName);
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.TABLE_NAME_KEY, exception.tableName);
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.GMCE_TOPIC_NAME, exception.GMCETopicPartition.split("-")[0]);
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.GMCE_TOPIC_PARTITION, exception.GMCETopicPartition.split("-")[1]);
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.GMCE_HIGH_WATERMARK, Long.toString(exception.highWatermark));
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.GMCE_LOW_WATERMARK, Long.toString(exception.lowWatermark));
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.FAILED_WRITERS_KEY, Joiner.on(',').join(exception.failedWriters));
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.OPERATION_TYPE_KEY, exception.operationType.toString());
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.FAILED_TO_ADD_PARTITION_VALUES_KEY, Joiner.on(',').join(exception.addedPartitionValues));
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.FAILED_TO_DROP_PARTITION_VALUES_KEY, Joiner.on(',').join(exception.droppedPartitionValues));
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.PARTITION_KEYS, Joiner.on(',').join(exception.partitionKeys.stream()
          .map(HiveRegistrationUnit.Column::getName).collect(Collectors.toList())));

      String message = Throwables.getRootCause(exception).getMessage();
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.EXCEPTION_MESSAGE_KEY_NAME, message);

      eventSubmitter.submit(gobblinTrackingEvent);
    }
  }

  private List<String> getFailedWriterList(MetadataWriter failedWriter) {
    List<MetadataWriter> failedWriters = metadataWriters.subList(metadataWriters.indexOf(failedWriter), metadataWriters.size());
    return failedWriters.stream().map(writer -> writer.getClass().getName()).collect(Collectors.toList());
  }
}
