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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.AvroUtils;

import org.apache.commons.lang3.StringUtils;
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
  private static final String HIVE_REGISTRATION_TIMEOUT_IN_SECONDS = "hive.registration.timeout.seconds";
  private static final long DEFAULT_HIVE_REGISTRATION_TIMEOUT_IN_SECONDS = 60;
  private final Joiner tableNameJoiner = Joiner.on('.');
  private final Closer closer = Closer.create();
  protected final HiveRegister hiveRegister;
  private final WhitelistBlacklist whitelistBlacklist;
  @Getter
  private final KafkaSchemaRegistry schemaRegistry;
  private final HashMap<String, HashMap<List<String>, ListenableFuture<Void>>> currentExecutionMap;

  /* Mapping from tableIdentifier to a cache, key'ed by timestamp and value is not in use. */
  private final HashMap<String, Cache<String, String>> schemaCreationTimeMap;

  /* Mapping from tableIdentifier to a cache, key'ed by a list of partitions with value as HiveSpec object. */
  private final HashMap<String, Cache<List<String>, HiveSpec>> specMaps;

  /* Mapping from tableIdentifier to latest schema observed. */
  private final HashMap<String, String> latestSchemaMap;
  private final long timeOutSeconds;
  private State state;

  public HiveMetadataWriter(State state) throws IOException {
    this.state = state;
    this.hiveRegister = this.closer.register(HiveRegister.get(state));
    this.whitelistBlacklist = new WhitelistBlacklist(state.getProp(HIVE_REGISTRATION_WHITELIST, ""),
        state.getProp(HIVE_REGISTRATION_BLACKLIST, ""));
    this.schemaRegistry = KafkaSchemaRegistry.get(state.getProperties());
    this.currentExecutionMap = new HashMap<>();
    this.schemaCreationTimeMap = new HashMap<>();
    this.specMaps = new HashMap<>();
    this.latestSchemaMap = new HashMap<>();
    this.timeOutSeconds =
        state.getPropAsLong(HIVE_REGISTRATION_TIMEOUT_IN_SECONDS, DEFAULT_HIVE_REGISTRATION_TIMEOUT_IN_SECONDS);
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
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          throw new RuntimeException("Error when getting the result of registration for table" + tableKey, e);
        }
      }
      executionMap.clear();
      log.info("finish flushing table: " + tableKey);
    }
  }

  public void write(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {
    String dbName = tableSpec.getTable().getDbName();
    String tableName = tableSpec.getTable().getTableName();
    String tableKey = tableNameJoiner.join(dbName, tableName);
    if (!specMaps.containsKey(tableKey) || specMaps.get(tableKey).size() == 0) {
      //Try to create table first to make sure table existed
      this.hiveRegister.createTableIfNotExists(tableSpec.getTable());
    }

    //ToDo: after making sure all spec has topic.name set, we should use topicName as key for schema
    if (!latestSchemaMap.containsKey(tableKey)) {
      HiveTable existingTable = this.hiveRegister.getTable(dbName, tableName).get();
      latestSchemaMap.put(tableKey,
          existingTable.getSerDeProps().getProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()));
    }

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
  }

  public void addFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap, String dbName,
      String tableName, String topicName) throws SchemaRegistryException {
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
              registerSpec(tableKey, partitionValue, spec, hiveSpecCache);
            }
          } else {
            registerSpec(tableKey, partitionValue, spec, hiveSpecCache);
          }
        }
      }
    }
  }

  private void registerSpec(String tableKey, List<String> partitionValue, HiveSpec spec,
      Cache<List<String>, HiveSpec> hiveSpecCache) {
    HashMap<List<String>, ListenableFuture<Void>> executionMap =
        this.currentExecutionMap.computeIfAbsent(tableKey, s -> new HashMap<>());
    if (executionMap.containsKey(partitionValue)) {
      try {
        executionMap.get(partitionValue).get(timeOutSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        log.error("Error when getting the result of registration for table" + tableKey);
        throw new RuntimeException(e);
      }
    }
    executionMap.put(partitionValue, this.hiveRegister.register(spec));
    hiveSpecCache.put(partitionValue, spec);
  }

  private void schemaUpdateHelper(GobblinMetadataChangeEvent gmce, HiveSpec spec, String topicName, String tableKey)
      throws SchemaRegistryException {
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
          latestSchemaMap.put(tableKey, newSchemaString);
          // Clear the schema versions cache so next time if we see schema source is schemaRegistry, we will contact schemaRegistry and update
          existedSchemaCreationTimes.cleanUp();
        } else if (gmce.getSchemaSource() == SchemaSource.SCHEMAREGISTRY && newSchemaCreationTime != null
            && existedSchemaCreationTimes.getIfPresent(newSchemaCreationTime) == null) {
          // We haven't seen this schema before, so we query schemaRegistry to get latest schema
          if (StringUtils.isNoneEmpty(topicName)) {
            Schema latestSchema = (Schema) this.schemaRegistry.getLatestSchemaByTopic(topicName);
            String latestCreationTime = AvroUtils.getSchemaCreationTime(latestSchema);
            if (latestCreationTime.equals(newSchemaCreationTime)) {
              //new schema is the latest schema, we update our record
              latestSchemaMap.put(tableKey, newSchemaString);
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
        }
      } catch (IOException e) {
        log.warn(String.format("Cannot get schema from table %s.%s", schemaSourceDb, spec.getTable().getTableName()), e);
      }
      return;
    }
    //Force to set the schema even there is no schema literal defined in the spec
    spec.getTable()
        .getSerDeProps()
        .setProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), latestSchemaMap.get(tableKey));
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
      write(gmce, newSpecsMap, oldSpecsMap, tableSpec);
    } else {
      log.debug(String.format("Skip table %s.%s since it's not selected", tableSpec.getTable().getDbName(),
          tableSpec.getTable().getTableName()));
    }
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }
}
