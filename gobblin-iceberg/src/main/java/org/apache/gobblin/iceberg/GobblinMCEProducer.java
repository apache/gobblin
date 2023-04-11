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

package org.apache.gobblin.iceberg;

import azkaban.jobExecutor.AbstractJob;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.iceberg.Utils.IcebergUtils;
import org.apache.gobblin.iceberg.publisher.GobblinMCEPublisher;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metadata.DataFile;
import org.apache.gobblin.metadata.DataMetrics;
import org.apache.gobblin.metadata.DataOrigin;
import org.apache.gobblin.metadata.DatasetIdentifier;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.IntegerBytesPair;
import org.apache.gobblin.metadata.IntegerLongPair;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.PartitionedDataWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;

import static org.apache.gobblin.iceberg.writer.GobblinMCEWriter.HIVE_PARTITION_NAME;


/**
 * A class running along with data ingestion pipeline for emitting GobblinMCE (Gobblin Metadata Change Event
 * that includes the information of the file metadata change, i.e., add or delete file, and the column min/max value of the added file.
 * GMCE will be consumed by another metadata ingestion pipeline to register/de-register hive/iceberg metadata)
 *
 * This is an abstract class, we need a sub system like Kakfa, which support at least once delivery, to emit the event
 */
@Slf4j
public abstract class GobblinMCEProducer implements Closeable {

  public static final String GMCE_PRODUCER_CLASS = "GobblinMCEProducer.class.name";
  public static final String GMCE_CLUSTER_NAME = "GobblinMCE.cluster.name";
  public static final String OLD_FILES_HIVE_REGISTRATION_KEY = "old.files.hive.registration.policy";
  private static final String HDFS_PLATFORM_URN = "urn:li:dataPlatform:hdfs";
  private static final String DATASET_ORIGIN_KEY = "dataset.origin";
  private static final String DEFAULT_DATASET_ORIGIN = "PROD";

  @Setter
  protected State state;
  protected MetricContext metricContext;

  public GobblinMCEProducer(State state) {
    this.state = state;
    this.metricContext = Instrumented.getMetricContext(state, this.getClass());
  }

  public void sendGMCE(Map<Path, Metrics> newFiles, List<String> oldFiles, List<String> oldFilePrefixes,
      Map<String, String> offsetRange, OperationType operationType, SchemaSource schemaSource) throws IOException {
    sendGMCE(newFiles, oldFiles, oldFilePrefixes, offsetRange, operationType, schemaSource, null);
  }

  /**
   * This method will use the files to compute the table name and dataset name, for each table it will generate one GMCE and send that to kafka so
   * the metadata ingestion pipeline can use the information to register metadata
   * @param newFiles The map of new files' path and metrics
   * @param oldFiles the list of old file to be dropped
   * @param offsetRange offset range of the new data, can be null
   * @param operationType The opcode of gmce emitted by this method.
   * @param serializedAuditCountMap Audit count map to be used by {@link org.apache.gobblin.iceberg.writer.IcebergMetadataWriter} to track iceberg
   *                                registration counts
   * @throws IOException
   */
  public void sendGMCE(Map<Path, Metrics> newFiles, List<String> oldFiles, List<String> oldFilePrefixes,
      Map<String, String> offsetRange, OperationType operationType, SchemaSource schemaSource, String serializedAuditCountMap) throws IOException {
    GobblinMetadataChangeEvent gmce =
        getGobblinMetadataChangeEvent(newFiles, oldFiles, oldFilePrefixes, offsetRange, operationType, schemaSource, serializedAuditCountMap);
    underlyingSendGMCE(gmce);
  }

  /**
   * Use the producer to send GMCE, the implementation need to make sure the emitting is at-least once in-order delivery
   * (i.e. use kafka producer to send event and config it to be at-least once delivery)
   * @param gmce GMCE that contains information of the metadata change
   */
  public abstract void underlyingSendGMCE(GobblinMetadataChangeEvent gmce);

  private void setBasicInformationForGMCE(GobblinMetadataChangeEvent.Builder gmceBuilder,
      Map<String, String> offsetRange, SchemaSource schemaSource) {
    String origin = state.getProp(DATASET_ORIGIN_KEY, DEFAULT_DATASET_ORIGIN);
    gmceBuilder.setDatasetIdentifier(DatasetIdentifier.newBuilder()
        .setDataPlatformUrn(HDFS_PLATFORM_URN)
        .setDataOrigin(DataOrigin.valueOf(origin))
        .setNativeName(state.getProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR))
        .build());
    gmceBuilder.setCluster(state.getProp(GMCE_CLUSTER_NAME, ClustersNames.getInstance().getClusterName()));
    //retention job does not have job.id
    gmceBuilder.setFlowId(
        state.getProp(AbstractJob.JOB_ID, new Configuration().get(ConfigurationKeys.AZKABAN_FLOW_ID)));
    gmceBuilder.setRegistrationPolicy(state.getProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY));
    gmceBuilder.setSchemaSource(schemaSource);
    gmceBuilder.setPartitionColumns(Lists.newArrayList(state.getProp(HIVE_PARTITION_NAME, "")));
    if (offsetRange != null) {
      gmceBuilder.setTopicPartitionOffsetsRange(offsetRange);
    }
    String schemaString = state.getProp(PartitionedDataWriter.WRITER_LATEST_SCHEMA);
    if (schemaString != null) {
      gmceBuilder.setTableSchema(schemaString);
    }
    if (state.contains(GobblinMCEPublisher.AVRO_SCHEMA_WITH_ICEBERG_ID)) {
      gmceBuilder.setAvroSchemaWithIcebergSchemaID(state.getProp(GobblinMCEPublisher.AVRO_SCHEMA_WITH_ICEBERG_ID));
    }
    if (state.contains(OLD_FILES_HIVE_REGISTRATION_KEY)) {
      gmceBuilder.setRegistrationPolicyForOldData(state.getProp(OLD_FILES_HIVE_REGISTRATION_KEY));
    } else {
      log.warn(
          "properties {} does not set, if it's for rewrite/drop operation, there may be trouble to get partition value for old data",
          OLD_FILES_HIVE_REGISTRATION_KEY);
    }
    Map<String, String> regProperties = new HashMap<>();
    if (state.contains(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME)) {
      regProperties.put(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME,
          state.getProp(HiveRegistrationPolicyBase.HIVE_DATABASE_NAME));
    }
    if (state.contains(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES)) {
      regProperties.put(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES,
          state.getProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_DATABASE_NAMES));
    }
    if (state.contains(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_TABLE_NAMES)) {
      regProperties.put(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_TABLE_NAMES,
          state.getProp(HiveRegistrationPolicyBase.ADDITIONAL_HIVE_TABLE_NAMES));
    }
    if (state.contains(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB)) {
      regProperties.put(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB,
          state.getProp(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB));
    }
    if (state.contains(HiveSerDeManager.HIVE_ROW_FORMAT)) {
      regProperties.put(HiveSerDeManager.HIVE_ROW_FORMAT,
          state.getProp(HiveSerDeManager.HIVE_ROW_FORMAT));
    }
    if (!regProperties.isEmpty()) {
      gmceBuilder.setRegistrationProperties(regProperties);
    }
  }

  public GobblinMetadataChangeEvent getGobblinMetadataChangeEvent(Map<Path, Metrics> newFiles, List<String> oldFiles,
      List<String> oldFilePrefixes, Map<String, String> offsetRange, OperationType operationType,
      SchemaSource schemaSource, String serializedAuditCountMap) {
    if (!verifyInput(newFiles, oldFiles, oldFilePrefixes, operationType)) {
      return null;
    }
    GobblinMetadataChangeEvent.Builder gmceBuilder = GobblinMetadataChangeEvent.newBuilder();
    setBasicInformationForGMCE(gmceBuilder, offsetRange, schemaSource);
    if (newFiles != null && !newFiles.isEmpty()) {
      gmceBuilder.setNewFiles(toGobblinDataFileList(newFiles));
    }
    if (oldFiles != null && !oldFiles.isEmpty()) {
      gmceBuilder.setOldFiles(oldFiles);
    }
    if (oldFilePrefixes != null && !oldFilePrefixes.isEmpty()) {
      gmceBuilder.setOldFilePrefixes(oldFilePrefixes);
    }
    gmceBuilder.setOperationType(operationType);
    gmceBuilder.setAuditCountMap(serializedAuditCountMap);
    return gmceBuilder.build();
  }

  private boolean verifyInput(Map<Path, Metrics> newFiles, List<String> oldFiles, List<String> oldFilePrefixes,
      OperationType operationType) {
    switch (operationType) {
      case rewrite_files: {
        if (newFiles == null || ((oldFiles == null || oldFiles.isEmpty()) && (oldFilePrefixes == null || oldFilePrefixes
            .isEmpty())) || newFiles.isEmpty()) {
          log.error("Rewrite files operation must contain newFiles to be added and oldFiles to be deleted");
          return false;
        }
        break;
      }
      case add_files: {
        if (newFiles == null || newFiles.isEmpty()) {
          log.error("Add files operation must contain newFiles to be added");
          return false;
        }
        break;
      }
      case drop_files: {
        if ((oldFiles == null || oldFiles.isEmpty()) && (oldFilePrefixes == null || oldFilePrefixes.isEmpty())) {
          log.error("Drop files operation must contain old files to be deleted");
          return false;
        }
        break;
      }
      case change_property: {
        if(oldFiles != null) {
          log.warn("{} old files detected while no file alteration is performed", oldFiles.size());
        }
        log.info("Setting GMCE while no file changes need to be performed.");
        break;
      }
      default: {
        //unsupported operation
        log.error("Unsupported operation type {}", operationType);
        return false;
      }
    }
    return true;
  }


  private List<DataFile> toGobblinDataFileList(Map<Path, Metrics> files) {
    return Lists.newArrayList(Iterables.transform(files.entrySet(), file ->
        {
          DataFile.Builder builder = createBuilderWithFilePath(file.getKey());
          addMetricsToFileBuilder(builder, file.getValue());
          return builder.build();
        }
    ));
  }

  private DataFile.Builder createBuilderWithFilePath(Path filePath) {
    return DataFile.newBuilder()
        .setFilePath(filePath.toString())
        .setFileFormat(IcebergUtils.getIcebergFormat(state).toString());
  }

  private void addMetricsToFileBuilder(DataFile.Builder builder, Metrics metrics) {
    // If metrics is null or empty, set FileMetrics a dummy one
    if(metrics == null || metrics.recordCount() == null) {
      builder.setFileMetrics(DataMetrics.newBuilder().setRecordCount(0)
          .build());
      return;
    }
    // If metrics is concrete, fill all fields
    builder.setFileMetrics(DataMetrics.newBuilder()
        .setRecordCount(metrics.recordCount())
        .setColumnSizes(getIntegerLongPairsFromMap(metrics.columnSizes()))
        .setValueCounts(getIntegerLongPairsFromMap(metrics.valueCounts()))
        .setNullValueCounts(getIntegerLongPairsFromMap(metrics.nullValueCounts()))
        .setLowerBounds(getIntegerBytesPairsFromMap(metrics.lowerBounds()))
        .setUpperBounds(getIntegerBytesPairsFromMap(metrics.upperBounds()))
        .build());
  }

  private List<IntegerLongPair> getIntegerLongPairsFromMap(Map<Integer, Long> map) {
    if (map == null || map.size() == 0) {
      return null;
    }
    Iterable<Map.Entry<Integer, Long>> entries = map.entrySet();
    Iterable<IntegerLongPair> pairs =
        Iterables.transform(entries, entry -> new IntegerLongPair(entry.getKey(), entry.getValue()));
    return Lists.newArrayList(pairs);
  }

  private List<IntegerBytesPair> getIntegerBytesPairsFromMap(Map<Integer, ByteBuffer> map) {
    if (map == null || map.size() == 0) {
      return null;
    }
    Iterable<Map.Entry<Integer, ByteBuffer>> entries = map.entrySet();
    Iterable<IntegerBytesPair> pairs =
        Iterables.transform(entries, entry -> new IntegerBytesPair(entry.getKey(), entry.getValue()));
    return Lists.newArrayList(pairs);
  }

  public static GobblinMCEProducer getGobblinMCEProducer(State state) {
      return GobblinConstructorUtils.invokeConstructor(GobblinMCEProducer.class,
          state.getProp(GMCE_PRODUCER_CLASS), state);
  }

  @Override
  public void close() throws IOException {
    //producer close will handle by the cache
    this.metricContext.close();
  }
}
