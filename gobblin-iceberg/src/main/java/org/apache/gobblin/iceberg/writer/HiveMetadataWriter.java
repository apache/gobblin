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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.AvroUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;


/**
 * This writer is used to register the hiveSpec into hive metaStore
 * For add_files operation, this writer will use cache to determine whether the partition is registered already or need to be altered
 * and then register the partition if needed
 * For rewrite_files operation, this writer will directly register the new hive spec and try to de-register the old hive spec if oldFilePrefixes is set
 * For drop_files operation, this writer will de-register the hive partition only if oldFilePrefixes is set in the GMCE
 */
@Slf4j
public class HiveMetadataWriter implements MetadataWriter {

  public static final String HIVE_REGISTRATION_WHITELIST = "hive.registration.whitelist";
  public static final String HIVE_REGISTRATION_BLACKLIST = "hive.registration.blacklist";
  private final Joiner tableNameJoiner = Joiner.on('.');
  private final Closer closer = Closer.create();
  protected final HiveRegister hiveRegister;
  private final WhitelistBlacklist whiteistBlacklist;
  @Getter
  private final KafkaSchemaRegistry schemaRegistry;
  private final HashMap<String, HashMap<List<String>, ListenableFuture<Void>>> currentExecutionMap;

  private final HashMap<String, Set<String>> schemaCreationTimeMap;
  private final HashMap<String, Cache<List<String>, HiveSpec>> specMaps;
  private final HashMap<String, String> lastestSchemaMap;
  private State state;

  public HiveMetadataWriter(State state) throws IOException {
    this.state = state;
    this.hiveRegister = this.closer.register(HiveRegister.get(state));
    this.whiteistBlacklist = new WhitelistBlacklist(state.getProp(HIVE_REGISTRATION_WHITELIST, ""),
        state.getProp(HIVE_REGISTRATION_BLACKLIST, ""));
    this.schemaRegistry = KafkaSchemaRegistry.get(state.getProperties());
    this.currentExecutionMap = new HashMap<>();
    this.schemaCreationTimeMap = new HashMap<>();
    this.specMaps = new HashMap<>();
    this.lastestSchemaMap = new HashMap<>();
  }

  @Override
  public void flush(String dbName, String tableName) throws IOException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    HashMap<List<String>, ListenableFuture<Void>> executionMap = this.currentExecutionMap.computeIfAbsent(tableKey, s -> new HashMap<>());
    //iterator all execution to get the result to make sure they all succeeded
    for (HashMap.Entry<List<String>, ListenableFuture<Void>> execution: executionMap.entrySet()) {
      //todo: add timeout for the execution
      try {
        execution.getValue().get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error when getting the result of registration for table" + tableKey);
        throw new RuntimeException(e);
      }
    }
  }

  public void write(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {

    String dbName = tableSpec.getTable().getDbName();
    String tableName = tableSpec.getTable().getTableName();

    //Try to create table first to make sure table existed
    this.hiveRegister.createTableIfNotExists(tableSpec.getTable());
    String tableKey = tableNameJoiner.join(dbName, tableName);
    if (!lastestSchemaMap.containsKey(tableKey)) {
      HiveTable existingTable = this.hiveRegister.getTable(dbName, tableName).get();
      lastestSchemaMap.put(tableKey, existingTable.getSerDeProps().getProp(
          AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()));
    }

    //Calculate the topic name from gmce, fall back to topic.name in hive spec which can also be null
    //todo: make topicName fall back to topic.name in hive spec so that we can also get schema for re-write operation
    String topicName = null;
    if (gmce.getTopicPartitionOffsetsRange() != null) {
      String topicPartitionString = gmce.getTopicPartitionOffsetsRange().keySet().iterator().next();
      //In case the topic name is not the table name or the topic name contains '-'
      topicName = topicPartitionString.substring(0, topicPartitionString.lastIndexOf('-'));
    }
    switch (gmce.getOperationType()) {
      case add_files: {
        addFiles(gmce, newSpecsMap, dbName, tableName, topicName);
        break;
      }
      default: {
        log.error("unsupported operation {}", gmce.getOperationType().toString());
        return;
      }
    }
  }

  public void addFiles(GobblinMetadataChangeEvent gmce, Map<String, Collection<HiveSpec>> newSpecsMap, String dbName, String tableName, String topicName)
      throws SchemaRegistryException {
    String tableKey = tableNameJoiner.join(dbName, tableName);
    for (Collection<HiveSpec> specs : newSpecsMap.values()) {
      for (HiveSpec spec: specs) {
         if (spec.getTable().getDbName().equals(dbName) && spec.getTable().getTableName().equals(tableName)) {
           List<String> partitionValue = spec.getPartition().isPresent()? spec.getPartition().get().getValues() : Lists.newArrayList();
           Cache<List<String>, HiveSpec> hiveSpecCache = specMaps.computeIfAbsent(tableKey, s -> CacheBuilder.newBuilder()
               .expireAfterAccess(state.getPropAsInt(GobblinMCEWriter.CACHE_EXPIRING_TIME,
                   GobblinMCEWriter.DEFAULT_CACHE_EXPIRING_TIME), TimeUnit.HOURS)
               .build());
           HiveSpec existedSpec = hiveSpecCache.getIfPresent(partitionValue);
           if (existedSpec != null) {
             //if existedSpec is not null, it means we already registered this partition, so check whether we need to update the table/partition
             if (gmce.getSchemaSource() == SchemaSource.SCHEMAREGISTRY) {
               // Only in this case, we need to update the schema to the latest schema in schemaRegistry
               schemaUpdateHelper(spec, topicName);
             }
             if (this.hiveRegister.needToUpdateTable(existedSpec.getTable(), spec.getTable())) {
                registerSpec(tableKey, partitionValue, spec);
                return;
             }
             if (spec.getPartition().isPresent() &&
                 this.hiveRegister.needToUpdatePartition(existedSpec.getPartition().get(), spec.getPartition().get())) {
               registerSpec(tableKey, partitionValue, spec);
             }
           }
         }
      }
    }

  }

  private void registerSpec(String tableKey, List<String> partitionValue, HiveSpec spec) {
    HashMap<List<String>, ListenableFuture<Void>> executionMap = this.currentExecutionMap.computeIfAbsent(tableKey, s -> new HashMap<>());
    if (executionMap.containsKey(partitionValue)) {
      try {
        executionMap.get(partitionValue).get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error when getting the result of registration for table" + tableKey);
        throw new RuntimeException(e);
      }
      executionMap.put(partitionValue, this.hiveRegister.register(spec));
    }
  }

  private void schemaUpdateHelper(HiveSpec spec, String topicName) throws SchemaRegistryException {
    if (topicName == null || topicName.isEmpty()) {
      return;
    }
    String newSchemaString = spec.getTable().getSerDeProps().getProp(
        AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
    if (newSchemaString != null) {
      Schema newSchema = new Schema.Parser().parse(newSchemaString);
      String newSchemaCreationTime = AvroUtils.getSchemaCreationTime(newSchema);
      Set<String> existedSchemaCreationTimes = schemaCreationTimeMap.computeIfAbsent(topicName, s -> new HashSet<>());
      if (newSchemaCreationTime == null || !existedSchemaCreationTimes.contains(newSchemaCreationTime)) {
        // We haven't seen this schema before, so we query schemaRegistry to get latest schema
        Schema latestSchema = (Schema) this.schemaRegistry.getLatestSchemaByTopic(topicName);
        String latestCreationTime = AvroUtils.getSchemaCreationTime(latestSchema);
        if (latestCreationTime.equals(newSchemaCreationTime)) {
          //new schema is the latest schema, we update our record
          lastestSchemaMap.put(topicName, newSchemaString);
        }
        existedSchemaCreationTimes.add(newSchemaCreationTime);
      }
      spec.getTable().getSerDeProps().setProp(
          AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), lastestSchemaMap.get(topicName));
    }
  }

  @Override
  public void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException {
    GenericRecord genericRecord = recordEnvelope.getRecord();
    GobblinMetadataChangeEvent gmce =
        (GobblinMetadataChangeEvent) SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
    if (whiteistBlacklist.acceptTable(tableSpec.getTable().getDbName(), tableSpec.getTable().getTableName())) {

        write(gmce, newSpecsMap, oldSpecsMap, tableSpec);
    } else {
      log.info(String.format("Skip table %s.%s since it's blacklisted", tableSpec.getTable().getDbName(),
          tableSpec.getTable().getTableName()));
    }
  }

  @Override
  public void close() throws IOException {
      this.closer.close();
  }

}
