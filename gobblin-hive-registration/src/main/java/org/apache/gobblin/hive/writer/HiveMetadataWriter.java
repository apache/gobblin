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

package org.apache.gobblin.hive.writer;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.GobblinMetricsRegistry;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ClustersNames;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;


/**
 * This writer is used to register the hiveSpec into hive metaStore
 * For add_files operation, this writer will use cache to determine whether the partition is registered already or need to be altered
 * and then register the partition if needed
 * For rewrite_files operation, this writer will directly register the new hive spec and try to de-register the old hive spec if oldFilePrefixes is set
 * For drop_files operation, this writer will de-register the hive partition only if oldFilePrefixes is set in the GMCE
 *
 * Added warning suppression for all references of {@link Cache}.
 */
@Slf4j
@SuppressWarnings("UnstableApiUsage")
public class HiveMetadataWriter implements MetadataWriter {

  private static final String HIVE_REGISTRATION_WHITELIST = "hive.registration.whitelist";
  private static final String HIVE_REGISTRATION_BLACKLIST = "hive.registration.blacklist";
  private static final String HIVE_USE_LATEST_SCHEMA_ALLOWLIST = "hive.use.latest.schema.allowlist";
  private static final String HIVE_USE_LATEST_SCHEMA_DENYLIST = "hive.use.latest.schema.denylist";

  private static final String HIVE_REGISTRATION_TIMEOUT_IN_SECONDS = "hive.registration.timeout.seconds";
  private static final long DEFAULT_HIVE_REGISTRATION_TIMEOUT_IN_SECONDS = 60;
  private final Joiner tableNameJoiner = Joiner.on('.');
  private final Closer closer = Closer.create();
  protected final HiveRegister hiveRegister;
  private final WhitelistBlacklist whitelistBlacklist;
  // Always use the latest table Schema for tables in #useLatestTableSchemaWhiteListBlackList
  // unless a newer writer schema arrives
  private final WhitelistBlacklist useLatestTableSchemaAllowDenyList;
  @Getter
  private final KafkaSchemaRegistry schemaRegistry;
  private final HashMap<String, HashMap<List<String>, ListenableFuture<Void>>> currentExecutionMap;

  /* Mapping from tableIdentifier to a cache, key'ed by timestamp and value is not in use. */
  private final HashMap<String, Cache<String, String>> schemaCreationTimeMap;

  /* Mapping from tableIdentifier to a cache, key'ed by a list of partitions with value as HiveSpec object. */
  private final HashMap<String, Cache<List<String>, HiveSpec>> specMaps;

  // Used to store the relationship between table and the gmce topicPartition
  private final HashMap<String, String> tableTopicPartitionMap;

  /* Mapping from tableIdentifier to latest schema observed. */
  private final HashMap<String, String> latestSchemaMap;
  private final long timeOutSeconds;
  protected State state;

  protected EventSubmitter eventSubmitter;

  public enum HivePartitionOperation {
    ADD_OR_MODIFY, DROP
  }

  public HiveMetadataWriter(State state) throws IOException {
    this.state = state;
    this.whitelistBlacklist = new WhitelistBlacklist(state.getProp(HIVE_REGISTRATION_WHITELIST, ""),
        state.getProp(HIVE_REGISTRATION_BLACKLIST, ""));
    this.schemaRegistry = KafkaSchemaRegistry.get(state.getProperties());
    this.currentExecutionMap = new HashMap<>();
    this.schemaCreationTimeMap = new HashMap<>();
    this.specMaps = new HashMap<>();
    this.latestSchemaMap = new HashMap<>();
    this.useLatestTableSchemaAllowDenyList = new WhitelistBlacklist(state.getProp(HIVE_USE_LATEST_SCHEMA_ALLOWLIST, ""),
        state.getProp(HIVE_USE_LATEST_SCHEMA_DENYLIST, ""));
    this.tableTopicPartitionMap = new HashMap<>();
    this.timeOutSeconds =
        state.getPropAsLong(HIVE_REGISTRATION_TIMEOUT_IN_SECONDS, DEFAULT_HIVE_REGISTRATION_TIMEOUT_IN_SECONDS);
    if (!state.contains(HiveRegister.HIVE_REGISTER_CLOSE_TIMEOUT_SECONDS_KEY)) {
      state.setProp(HiveRegister.HIVE_REGISTER_CLOSE_TIMEOUT_SECONDS_KEY, timeOutSeconds);
    }
    this.hiveRegister = this.closer.register(HiveRegister.get(state));
    List<Tag<?>> tags = Lists.newArrayList();
    String clusterIdentifier = ClustersNames.getInstance().getClusterName();
    tags.add(new Tag<>(MetadataWriterKeys.CLUSTER_IDENTIFIER_KEY_NAME, clusterIdentifier));
    MetricContext metricContext = closer.register(GobblinMetricsRegistry.getInstance().getMetricContext(state, HiveMetadataWriter.class, tags));
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, HiveMetadataWriter.class.getCanonicalName()).build();
  }

  @Override
  public void flush(String dbName, String tableName) throws IOException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    if(this.currentExecutionMap.containsKey(tableKey)) {
      log.info("start to flush table: " + tableKey);
      HashMap<List<String>, ListenableFuture<Void>> executionMap = this.currentExecutionMap.get(tableKey);
      //iterator all execution to get the result to make sure they all succeeded
      for (HashMap.Entry<List<String>, ListenableFuture<Void>> execution : executionMap.entrySet()) {
        try {
          execution.getValue().get(timeOutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          // Since TimeoutException should always be a transient issue, throw RuntimeException which will fail/retry container
          throw new RuntimeException("Timeout waiting for result of registration for table " + tableKey, e);
        } catch (InterruptedException | ExecutionException e) {
          if (Throwables.getRootCause(e) instanceof AlreadyExistsException) {
            log.warn("Caught AlreadyExistsException for db {}, table {}, ignoring", dbName, tableName);
          } else {
            Set<String> partitions = executionMap.keySet().stream().flatMap(List::stream).collect(Collectors.toSet());
            throw new HiveMetadataWriterWithPartitionInfoException(partitions, Collections.emptySet(), e);
          }
        }
        Cache<List<String>, HiveSpec> cache = specMaps.get(tableKey);
        if (cache != null) {
          HiveSpec hiveSpec = specMaps.get(tableKey).getIfPresent(execution.getKey());
          if (hiveSpec != null) {
            eventSubmitter.submit(buildCommitEvent(dbName, tableName, execution.getKey(), hiveSpec, HivePartitionOperation.ADD_OR_MODIFY));
          }
        }
      }
      executionMap.clear();
      log.info("finish flushing table: " + tableKey);
    }
  }

  @Override
  public void reset(String dbName, String tableName) throws IOException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    this.currentExecutionMap.remove(tableKey);
    this.schemaCreationTimeMap.remove(tableKey);
    this.latestSchemaMap.remove(tableKey);
    this.specMaps.remove(tableKey);
  }

  public void write(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec, String gmceTopicPartition) throws IOException {
    String dbName = tableSpec.getTable().getDbName();
    String tableName = tableSpec.getTable().getTableName();
    String tableKey = tableNameJoiner.join(dbName, tableName);
    if (!specMaps.containsKey(tableKey) || specMaps.get(tableKey).size() == 0) {
      //Try to create table first to make sure table existed
      this.hiveRegister.createTableIfNotExists(tableSpec.getTable());
    }

    //ToDo: after making sure all spec has topic.name set, we should use topicName as key for schema
    if (useLatestTableSchemaAllowDenyList.acceptTable(dbName, tableName)
        || !latestSchemaMap.containsKey(tableKey)) {
      HiveTable existingTable = this.hiveRegister.getTable(dbName, tableName).get();
      latestSchemaMap.put(tableKey,
          existingTable.getSerDeProps().getProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()));
    }

    tableTopicPartitionMap.put(tableKey, gmceTopicPartition);

    //Calculate the topic name from gmce, fall back to topic.name in hive spec which can also be null
    //todo: make topicName fall back to topic.name in hive spec so that we can also get schema for re-write operation
    String topicName = null;
    if (gmce.getTopicPartitionOffsetsRange() != null && !gmce.getTopicPartitionOffsetsRange().isEmpty()) {
      String topicPartitionString = gmce.getTopicPartitionOffsetsRange().keySet().iterator().next();
      //In case the topic name is not the table name or the topic name contains '-'
      topicName = topicPartitionString.substring(0, topicPartitionString.lastIndexOf('-'));
    }

    switch (gmce.getOperationType()) {
      case add_files: {
        addFiles(gmce, newSpecsMap, dbName, tableName, topicName);
        break;
      }
      case drop_files: {
        deleteFiles(gmce, oldSpecsMap, dbName, tableName);
        break;
      }
      case rewrite_files: {
        //de-register old partitions
        deleteFiles(gmce, oldSpecsMap, dbName, tableName);
        //register new partitions
        addFiles(gmce, newSpecsMap, dbName, tableName, topicName);
        break;
      }
      default: {
        log.error("unsupported operation {}", gmce.getOperationType().toString());
        return;
      }
    }
  }

  public void deleteFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> oldSpecsMap, String dbName,
      String tableName) throws IOException {
    if (gmce.getOldFilePrefixes() == null || gmce.getOldFilePrefixes().isEmpty()) {
      //We only de-register partition when old file prefixes is set, since hive partition refer to a whole directory
      return;
    }
    for (Collection<HiveSpec> specs : oldSpecsMap.values()) {
      for (HiveSpec spec : specs) {
        if (spec.getTable().getDbName().equals(dbName) && spec.getTable().getTableName().equals(tableName)) {
          if (spec.getPartition().isPresent()) {
            deRegisterPartitionHelper(dbName, tableName, spec);
          }
        }
      }
    }
    //TODO: De-register table if table location does not exist (Configurable)

  }

  protected void deRegisterPartitionHelper(String dbName, String tableName, HiveSpec spec) throws IOException {
    hiveRegister.dropPartitionIfExists(dbName, tableName, spec.getTable().getPartitionKeys(),
        spec.getPartition().get().getValues());
    eventSubmitter.submit(buildCommitEvent(dbName, tableName, spec.getPartition().get().getValues(), spec, HivePartitionOperation.DROP));
  }

  public void addFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap, String dbName,
      String tableName, String topicName) throws IOException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    for (Collection<HiveSpec> specs : newSpecsMap.values()) {
      for (HiveSpec spec : specs) {
        if (spec.getTable().getDbName().equals(dbName) && spec.getTable().getTableName().equals(tableName)) {
          List<String> partitionValue =
              spec.getPartition().isPresent() ? spec.getPartition().get().getValues() : Lists.newArrayList();
          Cache<List<String>, HiveSpec> hiveSpecCache = specMaps.computeIfAbsent(tableKey,
              s -> CacheBuilder.newBuilder()
                  .expireAfterAccess(state.getPropAsInt(MetadataWriter.CACHE_EXPIRING_TIME,
                      MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
                  .build());
          HiveSpec existedSpec = hiveSpecCache.getIfPresent(partitionValue);
          schemaUpdateHelper(gmce, spec, topicName, tableKey);
          if (existedSpec != null) {
            //if existedSpec is not null, it means we already registered this partition, so check whether we need to update the table/partition
            if ((this.hiveRegister.needToUpdateTable(existedSpec.getTable(), spec.getTable())) || (
                spec.getPartition().isPresent() && this.hiveRegister.needToUpdatePartition(
                    existedSpec.getPartition().get(), spec.getPartition().get()))) {
              registerSpec(dbName, tableName, partitionValue, spec, hiveSpecCache);
            }
          } else {
            registerSpec(dbName, tableName, partitionValue, spec, hiveSpecCache);
          }
        }
      }
    }
  }

  private void registerSpec(String dbName, String tableName, List<String> partitionValue, HiveSpec spec,
      Cache<List<String>, HiveSpec> hiveSpecCache) {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    HashMap<List<String>, ListenableFuture<Void>> executionMap =
        this.currentExecutionMap.computeIfAbsent(tableKey, s -> new HashMap<>());
    if (executionMap.containsKey(partitionValue)) {
      try {
        executionMap.get(partitionValue).get(timeOutSeconds, TimeUnit.SECONDS);
        eventSubmitter.submit(buildCommitEvent(dbName, tableName, partitionValue, spec, HivePartitionOperation.ADD_OR_MODIFY));
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.error("Error when getting the result of registration for table " + tableKey);
        throw new RuntimeException(e);
      }
    }
    executionMap.put(partitionValue, this.hiveRegister.register(spec));
    hiveSpecCache.put(partitionValue, spec);
  }

  private void schemaUpdateHelper(GobblinMetadataChangeEvent gmce, HiveSpec spec, String topicName, String tableKey)
      throws IOException {
    if (gmce.getSchemaSource() != SchemaSource.NONE) {
      String newSchemaString =
          spec.getTable().getSerDeProps().getProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
      if (newSchemaString != null) {
        Schema newSchema = new Schema.Parser().parse(newSchemaString);
        String newSchemaCreationTime = AvroUtils.getSchemaCreationTime(newSchema);
        Cache<String, String> existedSchemaCreationTimes = schemaCreationTimeMap.computeIfAbsent(tableKey,
            s -> CacheBuilder.newBuilder()
                .expireAfterAccess(
                    state.getPropAsInt(MetadataWriter.CACHE_EXPIRING_TIME, MetadataWriter.DEFAULT_CACHE_EXPIRING_TIME),
                    TimeUnit.HOURS)
                .build());
        if (gmce.getSchemaSource() == SchemaSource.EVENT) {
          // Schema source is Event, update schema anyway
          String schemaToUpdate = overrideSchemaLiteral(spec, newSchemaString, newSchemaCreationTime, gmce.getPartitionColumns());
          latestSchemaMap.put(tableKey, schemaToUpdate);
          // Clear the schema versions cache so next time if we see schema source is schemaRegistry, we will contact schemaRegistry and update
          existedSchemaCreationTimes.cleanUp();
        } else if (gmce.getSchemaSource() == SchemaSource.SCHEMAREGISTRY && newSchemaCreationTime != null
            && existedSchemaCreationTimes.getIfPresent(newSchemaCreationTime) == null) {
          // We haven't seen this schema before, so we query schemaRegistry to get latest schema
          if (StringUtils.isNoneEmpty(topicName)) {
            Schema latestSchema = (Schema) this.schemaRegistry.getLatestSchemaByTopic(topicName);
            String latestCreationTime = AvroUtils.getSchemaCreationTime(latestSchema);
            if (latestCreationTime.equals(newSchemaCreationTime)) {
              String schemaToUpdate = overrideSchemaLiteral(spec, newSchemaString, newSchemaCreationTime, gmce.getPartitionColumns());
              //new schema is the latest schema, we update our record
              latestSchemaMap.put(tableKey, schemaToUpdate);
            }
            existedSchemaCreationTimes.put(newSchemaCreationTime, "");
          }
        }
      }
    } else if (gmce.getRegistrationProperties().containsKey(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB)
        && !gmce.getRegistrationProperties().get(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB).equals(spec.getTable().getDbName())) {
      // If schema source is NONE and schema source db is set, we will directly update the schema to source db schema
      String schemaSourceDb = gmce.getRegistrationProperties().get(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB);
      try {
        String sourceSchema = fetchSchemaFromTable(schemaSourceDb, spec.getTable().getTableName());
        if (sourceSchema != null){
          spec.getTable()
              .getSerDeProps()
              .setProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), sourceSchema);
          HiveMetaStoreUtils.updateColumnsInfoIfNeeded(spec);
        }
      } catch (IOException e) {
        log.warn(String.format("Cannot get schema from table %s.%s", schemaSourceDb, spec.getTable().getTableName()), e);
      }
      return;
    }
    //Force to set the schema even there is no schema literal defined in the spec
    if (latestSchemaMap.containsKey(tableKey)) {
      spec.getTable().getSerDeProps()
          .setProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), latestSchemaMap.get(tableKey));
      HiveMetaStoreUtils.updateColumnsInfoIfNeeded(spec);
    }
  }

  /**
   * Method that overrides schema literal in implementation class
   * @param spec HiveSpec
   * @param latestSchema returns passed schema as is
   * @param schemaCreationTime updates schema with creation time
   * @param partitionNames
   * @return schema literal
   */
  protected String overrideSchemaLiteral(HiveSpec spec, String latestSchema, String schemaCreationTime,
      List<String> partitionNames) {
    return latestSchema;
  }


  private String fetchSchemaFromTable(String dbName, String tableName) throws IOException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    if (latestSchemaMap.containsKey(tableKey)) {
      return latestSchemaMap.get(tableKey);
    }
    Optional<HiveTable> table = hiveRegister.getTable(dbName, tableName);
    return table.isPresent()? table.get().getSerDeProps().getProp(
        AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()) : null;
  }

  @Override
  public void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {
    GenericRecord genericRecord = recordEnvelope.getRecord();
    GobblinMetadataChangeEvent gmce =
        (GobblinMetadataChangeEvent) SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
    if (whitelistBlacklist.acceptTable(tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName())) {
      try {
        write(gmce, newSpecsMap, oldSpecsMap, tableSpec, recordEnvelope.getWatermark().getSource());
      } catch (IOException e) {
        throw new HiveMetadataWriterWithPartitionInfoException(getPartitionValues(newSpecsMap), getPartitionValues(oldSpecsMap), e);
      }
    } else {
      log.debug(String.format("Skip table %s.%s since it's not selected", tableSpec.getTable().getDbName(),
          tableSpec.getTable().getTableName()));
    }
  }

  /**
   * Extract a unique list of partition values as strings from a map of HiveSpecs.
   */
  public Set<String> getPartitionValues(Map<String, Collection<HiveSpec>> specMap) {
    Set<HiveSpec> hiveSpecs = specMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    Set<List<String>> partitionValueLists = hiveSpecs.stream().filter(spec -> spec.getPartition().isPresent())
        .map(spec -> spec.getPartition().get().getValues()).collect(Collectors.toSet());
    return partitionValueLists.stream().flatMap(List::stream).collect(Collectors.toSet());
  }

  protected GobblinEventBuilder buildCommitEvent(String dbName, String tableName, List<String> partitionValues, HiveSpec hiveSpec,
      HivePartitionOperation operation) {
    GobblinEventBuilder gobblinTrackingEvent = new GobblinEventBuilder(MetadataWriterKeys.HIVE_COMMIT_EVENT_NAME);

    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.HIVE_DATABASE_NAME_KEY, dbName);
    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.HIVE_TABLE_NAME_KEY, tableName);
    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.PARTITION_KEYS, Joiner.on(',').join(hiveSpec.getTable().getPartitionKeys().stream()
        .map(HiveRegistrationUnit.Column::getName).collect(Collectors.toList())));
    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.PARTITION_VALUES_KEY, Joiner.on(',').join(partitionValues));
    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.HIVE_PARTITION_OPERATION_KEY, operation.name());
    gobblinTrackingEvent.addMetadata(MetadataWriterKeys.PARTITION_HDFS_PATH, hiveSpec.getPath().toString());

    String gmceTopicPartition = tableTopicPartitionMap.get(tableNameJoiner.join(dbName, tableName));
    if (gmceTopicPartition != null) {
      gobblinTrackingEvent.addMetadata(MetadataWriterKeys.HIVE_EVENT_GMCE_TOPIC_NAME, gmceTopicPartition.split("-")[0]);
    }

    return gobblinTrackingEvent;
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }
}
