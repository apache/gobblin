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
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.hive.writer.MetadataWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;


/**
 * This is a Wrapper of all the MetadataWriters. This writer will manage a list of {@Link MetadataWriter} which do the actual
 * metadata registration for different metadata store.
 * This writer is responsible for:
 * 1. Managing a map of Iceberg tables that it is currently processing
 * 2. Ensuring that the underlying metadata writers flush the metadata associated with each Iceberg table
 * 3. Call flush method for a specific table on a change in operation type
 * 4. Calculate {@Link HiveSpec}s and pass them to metadata writers to register metadata
 */
@Slf4j
public class GobblinMCEWriter implements DataWriter<GenericRecord> {
  public static final String DEFAULT_HIVE_REGISTRATION_POLICY_KEY = "default.hive.registration.policy";
  public static final String FORCE_HIVE_DATABASE_NAME = "force.hive.database.name";
  public static final String METADATA_REGISTRATION_THREADS = "metadata.registration.threads";
  public static final String METADATA_PARALLEL_RUNNER_TIMEOUT_MILLS = "metadata.parallel.runner.timeout.mills";
  public static final String HIVE_PARTITION_NAME = "hive.partition.name";
  public static final String GMCE_METADATA_WRITER_CLASSES = "gmce.metadata.writer.classes";
  public static final int DEFAULT_ICEBERG_PARALLEL_TIMEOUT_MILLS = 60000;
  public static final String TABLE_NAME_DELIMITER = ".";
  @Getter
  List<MetadataWriter> metadataWriters;
  Map<String, OperationType> tableOperationTypeMap;
  Map<String, OperationType> datasetOperationTypeMap;
  protected State state;
  private final ParallelRunner parallelRunner;
  private int parallelRunnerTimeoutMills;
  private Map<String, Cache<String, Collection<HiveSpec>>> oldSpecsMaps;
  private Map<String, Cache<String, Collection<HiveSpec>>> newSpecsMaps;
  private Closer closer = Closer.create();
  protected final AtomicLong recordCount = new AtomicLong(0L);

  public GobblinMCEWriter(DataWriterBuilder<Schema, GenericRecord> builder, State properties) throws IOException {
    newSpecsMaps = new HashMap<>();
    oldSpecsMaps = new HashMap<>();
    metadataWriters = new ArrayList<>();
    state = properties;
    for (String className : state.getPropAsList(GMCE_METADATA_WRITER_CLASSES, IcebergMetadataWriter.class.getName())) {
      metadataWriters.add(closer.register(GobblinConstructorUtils.invokeConstructor(MetadataWriter.class, className, state)));
    }
    tableOperationTypeMap = new HashMap<>();
    datasetOperationTypeMap = new HashMap<>();
    parallelRunner = closer.register(new ParallelRunner(state.getPropAsInt(METADATA_REGISTRATION_THREADS, 20),
        FileSystem.get(HadoopUtils.getConfFromState(properties))));
    parallelRunnerTimeoutMills =
        state.getPropAsInt(METADATA_PARALLEL_RUNNER_TIMEOUT_MILLS, DEFAULT_ICEBERG_PARALLEL_TIMEOUT_MILLS);
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    //Do nothing

  }

  /**
   * This method is used to get the specs map for a list of files. It will firstly look up in cache
   * to see if the specs has been calculated previously to recude the computing time
   * We have an assumption here: "for the same path and the same operation type, the specs should be the same"
   * @param files  List of files' names
   * @param specsMap The specs map for the files
   * @param cache  Cache that store the pre-calculated specs information
   * @param registerState State used to compute the specs
   * @param isPrefix if the name of file is prefix, if not we get the parent file name to calculate the hiveSpec
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
            Path regPath = new Path(file);
            if (!isPrefix) {
              regPath = regPath.getParent();
            }
            //Use raw path for federation purpose
            Path rawPath = new Path(regPath.toUri().getRawPath());
            cache.get(regPath.toString(), () -> policy.getHiveSpecs(rawPath));
            specsMap.put(regPath.toString(), cache.getIfPresent(regPath.toString()));
          } catch (Exception e) {
            log.warn("Cannot get Hive Spec for {} using policy {}", file, policy.toString());
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
    //filter out the events that not for this cluster
    if (!genericRecord.get("cluster").equals(ClustersNames.getInstance().getClusterName())) {
      return;
    }
    // Use schema from record to avoid issue when schema evolution
    GobblinMetadataChangeEvent gmce =
        (GobblinMetadataChangeEvent) SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
    String datasetName = gmce.getDatasetIdentifier().toString();
    //remove the old hive spec cache after flush
    //Here we assume that new hive spec for one path always be the same(ingestion flow register to same tables)
    if (!datasetOperationTypeMap.containsKey(datasetName)) {
      oldSpecsMaps.remove(datasetName);
    }
    if (datasetOperationTypeMap.containsKey(datasetName)
        && datasetOperationTypeMap.get(datasetName) != gmce.getOperationType()) {
      datasetOperationTypeMap.put(datasetName, gmce.getOperationType());
    }
    //We assume in one same operation interval, for same dataset, the table property will not change to reduce the time to compute hiveSpec.
    ConcurrentHashMap<String, Collection<HiveSpec>> newSpecsMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Collection<HiveSpec>> oldSpecsMap = new ConcurrentHashMap<>();

    if (gmce.getNewFiles() != null) {
      State registerState = setHiveRegProperties(state, gmce, true);
      computeSpecMap(Lists.newArrayList(Iterables.transform(gmce.getNewFiles(), dataFile -> dataFile.getFilePath())),
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
    Collection<HiveSpec> specs =
        newSpecsMap.isEmpty() ? oldSpecsMap.values().iterator().next() : newSpecsMap.values().iterator().next();
    for (HiveSpec spec : specs) {
      String dbName = spec.getTable().getDbName();
      String tableName = spec.getTable().getTableName();
      String tableString = Joiner.on(TABLE_NAME_DELIMITER).join(dbName, tableName);
      if (tableOperationTypeMap.containsKey(tableString)
          && tableOperationTypeMap.get(tableString) != gmce.getOperationType()) {
        for (MetadataWriter writer : metadataWriters) {
          writer.flush(dbName, tableName);
        }
      }
      tableOperationTypeMap.put(tableString, gmce.getOperationType());
      for (MetadataWriter writer : metadataWriters) {
        writer.writeEnvelope(recordEnvelope, newSpecsMap, oldSpecsMap, spec);
      }
    }
    this.recordCount.incrementAndGet();
  }

  /**
   * Call the metadata writers to do flush each table metadata.
   * Flush of metadata writer is the place that do really metadata
   * registration (For iceberg, this method will generate a snapshot)
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    log.info(String.format("start to flushing %s records", String.valueOf(recordCount.get())));
    for (String tableString : tableOperationTypeMap.keySet()) {
      List<String> tid = Splitter.on(TABLE_NAME_DELIMITER).splitToList(tableString);
      for (MetadataWriter writer : metadataWriters) {
        writer.flush(tid.get(0), tid.get(1));
      }
    }
    tableOperationTypeMap.clear();
    recordCount.lazySet(0L);
  }

  @Override
  public void close() throws IOException {
    this.flush();
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
}
