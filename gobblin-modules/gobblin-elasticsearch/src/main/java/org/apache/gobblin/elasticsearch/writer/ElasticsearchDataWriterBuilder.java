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
package org.apache.gobblin.elasticsearch.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncWriterManager;
import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.gobblin.writer.BufferedAsyncDataWriter;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.writer.SequentialBasedBatchAccumulator;

import com.google.gson.JsonObject;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;

public class ElasticsearchDataWriterBuilder extends DataWriterBuilder {

  @Override
  public DataWriter build() throws IOException {

    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);

    SequentialBasedBatchAccumulator<JsonObject> batchAccumulator = new SequentialBasedBatchAccumulator<>(taskProps);

    BatchAsyncDataWriter asyncDataWriter;
    switch (ElasticsearchWriterConfigurationKeys.ClientType.valueOf(
        ConfigUtils.getString(config,
            ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_TYPE,
            ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_TYPE_DEFAULT).toUpperCase())) {
      case REST: {
        asyncDataWriter = new ElasticsearchRestWriter(config);
        break;
      }
      case TRANSPORT: {
        asyncDataWriter = new ElasticsearchTransportClientWriter(config);
        break;
      }
      default: {
        throw new IllegalArgumentException("Need to specify which "
            + ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_TYPE
            + " client to use (rest/transport)");
      }
    }
    BufferedAsyncDataWriter bufferedAsyncDataWriter = new BufferedAsyncDataWriter(batchAccumulator, asyncDataWriter);

    double failureAllowance = ConfigUtils.getDouble(config, ElasticsearchWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG,
        ElasticsearchWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;
    boolean retriesEnabled = ConfigUtils.getBoolean(config, ElasticsearchWriterConfigurationKeys.RETRIES_ENABLED,
        ElasticsearchWriterConfigurationKeys.RETRIES_ENABLED_DEFAULT);
    int maxRetries = ConfigUtils.getInt(config, ElasticsearchWriterConfigurationKeys.MAX_RETRIES,
        ElasticsearchWriterConfigurationKeys.MAX_RETRIES_DEFAULT);


    return AsyncWriterManager.builder()
        .failureAllowanceRatio(failureAllowance)
        .retriesEnabled(retriesEnabled)
        .numRetries(maxRetries)
        .config(config)
        .asyncDataWriter(bufferedAsyncDataWriter)
        .build();
  }
}