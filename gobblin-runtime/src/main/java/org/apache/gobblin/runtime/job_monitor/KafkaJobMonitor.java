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

package org.apache.gobblin.runtime.job_monitor;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecMonitor;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Abstract {@link JobSpecMonitor} that reads {@link JobSpec}s from a Kafka stream. Subclasses should implement
 * {@link KafkaJobMonitor#parseJobSpec(byte[])} to transform the message into one or multiple {@link JobSpec}s.
 */
@Slf4j
public abstract class KafkaJobMonitor extends HighLevelConsumer<byte[], byte[]> implements JobSpecMonitor {

  public static final String KAFKA_JOB_MONITOR_PREFIX = "jobSpecMonitor.kafka";
  public static final String KAFKA_AUTO_OFFSET_RESET_KEY = KAFKA_JOB_MONITOR_PREFIX + ".auto.offset.reset";
  public static final String KAFKA_AUTO_OFFSET_RESET_SMALLEST = "smallest";
  public static final String KAFKA_AUTO_OFFSET_RESET_LARGEST = "largest";
  protected DatasetStateStore datasetStateStore;
  protected final MutableJobCatalog jobCatalog;

  @Getter
  protected ContextAwareMeter newSpecs;
  @Getter
  protected ContextAwareMeter updatedSpecs;
  @Getter
  protected ContextAwareMeter cancelledSpecs;
  @Getter
  protected ContextAwareMeter removedSpecs;
  @Getter
  protected ContextAwareMeter totalSpecs;

  /**
   * @return A collection of {@link JobSpec}s to add/update/remove from the catalog,
   *        parsed from the Kafka message.
   * @throws IOException
   */
  public abstract Collection<JobSpec> parseJobSpec(byte[] message) throws IOException;

  public KafkaJobMonitor(String topic, MutableJobCatalog catalog, Config config) {
    super(topic, ConfigUtils.getConfigOrEmpty(config, KAFKA_JOB_MONITOR_PREFIX), 1);
    this.jobCatalog = catalog;
    try {
      this.datasetStateStore = DatasetStateStore.buildDatasetStateStore(config);
    } catch (Exception e) {
      log.warn("DatasetStateStore could not be created.", e);
    }
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.newSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_NEW_SPECS);
    this.updatedSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_UPDATED_SPECS);
    this.removedSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_REMOVED_SPECS);
    this.cancelledSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_CANCELLED_SPECS);
    this.totalSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_TOTAL_SPECS);
  }

  @VisibleForTesting
  @Override
  protected void buildMetricsContextAndMetrics() {
    super.buildMetricsContextAndMetrics();
  }

  @VisibleForTesting
  @Override
  protected void shutdownMetrics()
      throws IOException {
    super.shutdownMetrics();
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord<byte[],byte[]> message) {
    try {
      Collection<JobSpec> parsedCollection = parseJobSpec(message.getValue());
      for (JobSpec parsedMessage : parsedCollection) {
        SpecExecutor.Verb verb;

        try {
          verb = SpecExecutor.Verb.valueOf(parsedMessage.getMetadata().get(SpecExecutor.VERB_KEY));
        } catch (IllegalArgumentException | NullPointerException e) {
          log.error("Unknown verb {} for spec {}", parsedMessage.getMetadata().get(SpecExecutor.VERB_KEY), parsedMessage.getUri());
          continue;
        }

        this.totalSpecs.mark();

        switch (verb) {
          case ADD:
            this.newSpecs.mark();
            this.jobCatalog.put(parsedMessage);
            break;
          case UPDATE:
            this.updatedSpecs.mark();
            this.jobCatalog.put(parsedMessage);
            break;
          case UNKNOWN: // unknown are considered as add request to maintain backward compatibility
            log.warn("Job Spec Verb is 'UNKNOWN', putting this spec in job catalog anyway.");
            this.jobCatalog.put(parsedMessage);
            break;
          case DELETE:
            this.removedSpecs.mark();
            URI jobSpecUri = parsedMessage.getUri();
            this.jobCatalog.remove(jobSpecUri);
            // Delete the job state if it is a delete spec request
            deleteStateStore(jobSpecUri);
            break;
          case CANCEL:
            this.cancelledSpecs.mark();
            this.jobCatalog.remove(parsedMessage.getUri(), true);
            break;
          default:
            log.error("Cannot process spec {} with verb {}", parsedMessage.getUri(), verb);
        }
      }
    } catch (IOException ioe) {
      String messageStr = new String(message.getValue(), Charsets.UTF_8);
      log.error(String.format("Failed to parse kafka message with offset %d: %s.", message.getOffset(), messageStr), ioe);
    }
  }

  /**
   * It fetches the job name from the given jobSpecUri
   * and deletes its corresponding state store
   * @param jobSpecUri jobSpecUri as created by
   *                   {@link org.apache.gobblin.runtime.api.FlowSpec.Utils#createFlowSpecUri}
   * @throws IOException
   */
  private void deleteStateStore(URI jobSpecUri) throws IOException {
    int EXPECTED_NUM_URI_TOKENS = 3;
    String[] uriTokens = jobSpecUri.getPath().split("/");

    if (null == this.datasetStateStore) {
      log.warn("Job state store deletion failed as datasetstore is not initialized.");
      return;
    }
    if (uriTokens.length != EXPECTED_NUM_URI_TOKENS) {
      log.error("Invalid URI {}.", jobSpecUri);
      return;
    }

    String jobName = uriTokens[EXPECTED_NUM_URI_TOKENS - 1];
    this.datasetStateStore.delete(jobName);
    log.info("JobSpec {} deleted with statestore.", jobSpecUri);
  }
}
