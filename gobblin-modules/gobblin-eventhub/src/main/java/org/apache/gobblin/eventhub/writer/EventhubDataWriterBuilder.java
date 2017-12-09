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

package org.apache.gobblin.eventhub.writer;

import java.io.IOException;
import java.util.Properties;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.AsyncWriterManager;
import org.apache.gobblin.writer.BufferedAsyncDataWriter;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;


/**
 * Builder that hands back a {@link EventhubDataWriter}
 */
public class EventhubDataWriterBuilder extends DataWriterBuilder {

  /**
   * Create an eventhub data writer, wrapped into a buffered async data writer
   */
  public AsyncDataWriter getAsyncDataWriter(Properties properties) {
    EventhubDataWriter eventhubDataWriter = new EventhubDataWriter(properties);
    EventhubBatchAccumulator accumulator = new EventhubBatchAccumulator(properties);
    BatchedEventhubDataWriter batchedEventhubDataWriter = new BatchedEventhubDataWriter(accumulator, eventhubDataWriter);
    return batchedEventhubDataWriter;
  }

    @Override
    public DataWriter build()
        throws IOException {
      State state = this.destination.getProperties();
      Properties taskProps = state.getProperties();
      Config config = ConfigUtils.propertiesToConfig(taskProps);
      long commitTimeoutMillis = ConfigUtils.getLong(config, BatchedEventhubDataWriter.COMMIT_TIMEOUT_MILLIS_CONFIG,
              BatchedEventhubDataWriter.COMMIT_TIMEOUT_MILLIS_DEFAULT);
      long commitStepWaitTimeMillis = ConfigUtils.getLong(config, BatchedEventhubDataWriter.COMMIT_STEP_WAIT_TIME_CONFIG,
              BatchedEventhubDataWriter.COMMIT_STEP_WAIT_TIME_DEFAULT);
      double failureAllowance = ConfigUtils.getDouble(config, BatchedEventhubDataWriter.FAILURE_ALLOWANCE_PCT_CONFIG,
              BatchedEventhubDataWriter.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;

      return AsyncWriterManager.builder()
          .config(config)
          .commitTimeoutMillis(commitTimeoutMillis)
          .commitStepWaitTimeInMillis(commitStepWaitTimeMillis)
          .failureAllowanceRatio(failureAllowance)
          .retriesEnabled(false)
          .asyncDataWriter(getAsyncDataWriter(taskProps)).maxOutstandingWrites(10000)
          .build();
    }
}
