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

package org.apache.gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationException;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.AsyncWriterManager;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;


/**
 * Base kafka data writer builder. It builds an async kafka {@link DataWriter}
 */
public abstract class AbstractKafkaDataWriterBuilder<S, D> extends DataWriterBuilder<S, D> {

  protected abstract AsyncDataWriter<D> getAsyncDataWriter(Properties props) throws ConfigurationException;

  /**
   * Build a {@link DataWriter}.
   *
   * @throws IOException if there is anything wrong building the writer
   * @return the built {@link DataWriter}
   */
  @Override
  public DataWriter<D> build()
      throws IOException {
    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);
    long commitTimeoutMillis = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_DEFAULT);
    long commitStepWaitTimeMillis = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_DEFAULT);
    double failureAllowance = ConfigUtils.getDouble(config, KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG,
        KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;

    return AsyncWriterManager.builder()
        .config(config)
        .commitTimeoutMillis(commitTimeoutMillis)
        .commitStepWaitTimeInMillis(commitStepWaitTimeMillis)
        .failureAllowanceRatio(failureAllowance)
        .retriesEnabled(false)
        .asyncDataWriter(getAsyncDataWriter(taskProps))
        .build();
  }
}
