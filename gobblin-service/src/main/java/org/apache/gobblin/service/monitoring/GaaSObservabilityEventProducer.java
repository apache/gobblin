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

package org.apache.gobblin.service.monitoring;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import azkaban.jobExecutor.AbstractJob;
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
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.PartitionedDataWriter;

import static org.apache.gobblin.iceberg.writer.GobblinMCEWriter.HIVE_PARTITION_NAME;


/**
 * A class running along with data ingestion pipeline for emitting GobblinMCE (Gobblin Metadata Change Event
 * that includes the information of the file metadata change, i.e., add or delete file, and the column min/max value of the added file.
 * GMCE will be consumed by another metadata ingestion pipeline to register/de-register hive/iceberg metadata)
 *
 * This is an abstract class, we need a sub system like Kakfa, which support at least once delivery, to emit the event
 */
@Slf4j
public abstract class GaaSObservabilityEventProducer implements Closeable {

  public static final String GMCE_PRODUCER_CLASS = "GobblinMCEProducer.class.name";
  public static final String GMCE_CLUSTER_NAME = "GobblinMCE.cluster.name";
  public static final String OLD_FILES_HIVE_REGISTRATION_KEY = "old.files.hive.registration.policy";
  private static final String HDFS_PLATFORM_URN = "urn:li:dataPlatform:hdfs";
  private static final String DATASET_ORIGIN_KEY = "dataset.origin";
  private static final String DEFAULT_DATASET_ORIGIN = "PROD";

  @Setter
  protected State state;
  protected MetricContext metricContext;

  public GaaSObservabilityEventProducer(State state) {
    this.state = state;
    this.metricContext = Instrumented.getMetricContext(state, this.getClass());
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
  public void emitEvent(Map<Path, Metrics> newFiles, List<String> oldFiles, List<String> oldFilePrefixes,
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


  public GaaSObservabilityEventExperimental createGaaSObservabilityEvent(State state) {
    GaaSObservabilityEventExperimental.Builder builder = GaaSObservabilityEventExperimental.newBuilder();

    return builder.build();
  }

  public static GaaSObservabilityEventProducer getGobblinMCEProducer(State state) {
      return GobblinConstructorUtils.invokeConstructor(GaaSObservabilityEventProducer.class,
          state.getProp(GMCE_PRODUCER_CLASS), state);
  }

  @Override
  public void close() throws IOException {
    //producer close will handle by the cache
    this.metricContext.close();
  }
}
