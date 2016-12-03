/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;

import gobblin.configuration.State;
import gobblin.util.ConfigUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.PartitionAwareDataWriterBuilder;

/**
 * Base class for creating KafkaDataWriter builders.
 */

public abstract class BaseKafkaDataWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

  private static final Long MILLIS_TO_NANOS = 1000L * 1000L;

  protected abstract AsyncDataWriter<GenericRecord> getAsyncDataWriter(Properties props);

  /**
   * Build a {@link DataWriter}.
   *
   * @throws IOException if there is anything wrong building the writer
   * @return the built {@link DataWriter}
   */
  @Override
  public DataWriter<GenericRecord> build()
      throws IOException {
    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);
    long commitTimeoutInNanos = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_DEFAULT) * MILLIS_TO_NANOS;
    long commitStepWaitTimeMillis = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_DEFAULT);
    double failureAllowance = ConfigUtils.getDouble(config, KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG,
        KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;

    return AsyncBestEffortDataWriter.builder()
        .config(config)
        .recordsAttemptedMetricName(KafkaWriterMetricNames.RECORDS_PRODUCED_METER)
        .recordsSuccessMetricName(KafkaWriterMetricNames.RECORDS_SUCCESS_METER)
        .recordsFailedMetricName(KafkaWriterMetricNames.RECORDS_FAILED_METER)
        .commitTimeoutInNanos(commitTimeoutInNanos)
        .commitStepWaitTimeInMillis(commitStepWaitTimeMillis)
        .failureAllowance(failureAllowance)
        .asyncDataWriter(getAsyncDataWriter(taskProps))
        .build();
  }

}
