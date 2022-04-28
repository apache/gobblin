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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistryFactory;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaRegistryVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.troubleshooter.JobIssueEventHandler;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A job status monitor for Avro messages. Uses {@link GobblinTrackingEvent} schema to parse the messages and calls
 * {@link #parseJobStatus(GobblinTrackingEvent)} for each received message.
 */
@Slf4j
public class KafkaAvroJobStatusMonitor extends KafkaJobStatusMonitor {
  private static final String JOB_STATUS_MONITOR_MESSAGE_PARSE_FAILURES = "jobStatusMonitor.messageParseFailures";

  private final ThreadLocal<SpecificDatumReader<GobblinTrackingEvent>> reader;
  private final ThreadLocal<BinaryDecoder> decoder;

  private final SchemaVersionWriter schemaVersionWriter;
  @Getter
  private Meter messageParseFailures;

  public KafkaAvroJobStatusMonitor(String topic, Config config, int numThreads,
      JobIssueEventHandler jobIssueEventHandler)
      throws IOException, ReflectiveOperationException {
    super(topic, config, numThreads,  jobIssueEventHandler);

    if (ConfigUtils.getBoolean(config, ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY, false)) {
      KafkaAvroSchemaRegistry schemaRegistry = (KafkaAvroSchemaRegistry) new KafkaAvroSchemaRegistryFactory().
          create(ConfigUtils.configToProperties(config));
      this.schemaVersionWriter = new SchemaRegistryVersionWriter(schemaRegistry, topic, GobblinTrackingEvent.SCHEMA$);
    } else {
      this.schemaVersionWriter = new FixedSchemaVersionWriter();
    }
    this.decoder = ThreadLocal.withInitial(() -> {
      InputStream dummyInputStream = new ByteArrayInputStream(new byte[0]);
      return DecoderFactory.get().binaryDecoder(dummyInputStream, null);
    });
    this.reader = ThreadLocal.withInitial(() -> new SpecificDatumReader<>(GobblinTrackingEvent.SCHEMA$));
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.messageParseFailures = this.getMetricContext().meter(JOB_STATUS_MONITOR_MESSAGE_PARSE_FAILURES);
  }

  @Override
  @VisibleForTesting
  public GobblinTrackingEvent deserializeEvent(DecodeableKafkaRecord<byte[],byte[]> message) {
    try {
      InputStream is = new ByteArrayInputStream(message.getValue());
      schemaVersionWriter.advanceInputStreamToRecord(new DataInputStream(is));
      Decoder decoder = DecoderFactory.get().binaryDecoder(is, this.decoder.get());

      return this.reader.get().read(null, decoder);
    } catch (Exception exc) {
      this.messageParseFailures.mark();
      if (this.messageParseFailures.getFiveMinuteRate() < 1) {
        log.warn("Unable to decode input message at kafka offset" + message.getOffset(), exc);
      } else {
        log.warn("Unable to decode input message at kafka offset" + message.getOffset());
      }
      return null;
    }
  }

  /**
   * Parse the {@link GobblinTrackingEvent}s to determine the {@link ExecutionStatus} of the job.
   * @param event an instance of {@link GobblinTrackingEvent}
   * @return job status as an instance of {@link org.apache.gobblin.configuration.State}
   */
  @Override
  @VisibleForTesting
  public org.apache.gobblin.configuration.State parseJobStatus(GobblinTrackingEvent event) {
    if (!acceptEvent(event)) {
      return null;
    }
    Properties properties = new Properties();
    properties.putAll(event.getMetadata());

    switch (event.getName()) {
      case TimingEvent.FlowTimings.FLOW_COMPILED:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPILED.name());
        break;
      case TimingEvent.FlowTimings.FLOW_RUNNING:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.RUNNING.name());
        break;
      case TimingEvent.LauncherTimings.JOB_PENDING:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.PENDING.name());
        break;
      case TimingEvent.FlowTimings.FLOW_PENDING_RESUME:
      case TimingEvent.LauncherTimings.JOB_PENDING_RESUME:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.PENDING_RESUME.name());
        break;
      case TimingEvent.LauncherTimings.JOB_ORCHESTRATED:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.ORCHESTRATED.name());
        properties.put(TimingEvent.JOB_ORCHESTRATED_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.LauncherTimings.WORK_UNITS_PREPARATION:
      case TimingEvent.LauncherTimings.JOB_PREPARE:
      case TimingEvent.LauncherTimings.JOB_START:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.RUNNING.name());
        properties.put(TimingEvent.JOB_START_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.FlowTimings.FLOW_SUCCEEDED:
      case TimingEvent.LauncherTimings.JOB_SUCCEEDED:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
        properties.put(TimingEvent.JOB_END_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.FlowTimings.FLOW_FAILED:
      case TimingEvent.FlowTimings.FLOW_COMPILE_FAILED:
      case TimingEvent.LauncherTimings.JOB_FAILED:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.FAILED.name());
        properties.put(TimingEvent.JOB_END_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.FlowTimings.FLOW_CANCELLED:
      case TimingEvent.LauncherTimings.JOB_CANCEL:
      case TimingEvent.JOB_SKIPPED_TIME:
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.CANCELLED.name());
        properties.put(TimingEvent.JOB_END_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.FlowTimings.FLOW_RUN_DEADLINE_EXCEEDED:
      case TimingEvent.FlowTimings.FLOW_START_DEADLINE_EXCEEDED:
        properties.put(TimingEvent.FlowEventConstants.DOES_CANCELED_FLOW_MERIT_RETRY, true);
        properties.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.CANCELLED.name());
        properties.put(TimingEvent.JOB_END_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      case TimingEvent.JOB_COMPLETION_PERCENTAGE:
        properties.put(TimingEvent.JOB_LAST_PROGRESS_EVENT_TIME, properties.getProperty(TimingEvent.METADATA_END_TIME));
        break;
      default:
        return null;
    }
    return new org.apache.gobblin.configuration.State(properties);
  }


  /**
   * Filter for {@link GobblinTrackingEvent}. Used to quickly determine whether an event should be used to produce
   * a {@link JobStatus}.
   */
  private boolean acceptEvent(GobblinTrackingEvent event) {
    if ((!event.getMetadata().containsKey(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD)) ||
        (!event.getMetadata().containsKey(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD)) ||
        (!event.getMetadata().containsKey(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD))) {
      return false;
    }
    return true;
  }
}
