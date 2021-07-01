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
import java.net.UnknownHostException;
import java.util.concurrent.Future;

import org.apache.commons.math3.util.Pair;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.Batch;
import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;


@Slf4j
class ElasticsearchTransportClientWriter extends ElasticsearchWriterBase implements BatchAsyncDataWriter<Object> {

  private final TransportClient client;

  ElasticsearchTransportClientWriter(Config config) throws UnknownHostException {
    super(config);
    // Check if ssl is being configured, throw error that transport client does not support ssl
    Preconditions.checkArgument(!ConfigUtils.getBoolean(config,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_ENABLED, false),
        "Transport client does not support ssl, try the Rest client instead");

    this.client = createTransportClient(config);

    log.info("ElasticsearchWriter configured successfully with: indexName={}, indexType={}, idMappingEnabled={}, typeMapperClassName={}",
        this.indexName, this.indexType, this.idMappingEnabled, this.typeMapper);
  }

  @Override
  int getDefaultPort() {
    return ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_TRANSPORT_WRITER_DEFAULT_PORT;
  }

  @Override
  public Future<WriteResponse> write(Batch<Object> batch, @Nullable WriteCallback callback) {

    Pair<BulkRequest, FutureCallbackHolder> preparedBatch = this.prepareBatch(batch, callback);
    client.bulk(preparedBatch.getFirst(), preparedBatch.getSecond().getActionListener());
    return preparedBatch.getSecond().getFuture();

  }

  @Override
  public void flush() throws IOException {
    // Elasticsearch client doesn't support a flush method
  }

  @Override
  public void close() throws IOException {
    log.info("Got a close call in ElasticSearchTransportWriter");
    super.close();
    this.client.close();
  }

  @VisibleForTesting
  TransportClient getTransportClient() {
    return this.client;
  }

  private TransportClient createTransportClient(Config config) throws UnknownHostException {
    TransportClient transportClient;

    // Set TransportClient settings
    Settings.Builder settingsBuilder = Settings.builder();
    if (config.hasPath(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SETTINGS)) {
      settingsBuilder.put(ConfigUtils.configToProperties(config,
              ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SETTINGS));
    }
    settingsBuilder.put("client.transport.ignore_cluster_name",true);
    settingsBuilder.put("client.transport.sniff", true);
    transportClient = new PreBuiltTransportClient(settingsBuilder.build());
    this.hostAddresses.forEach(transportClient::addTransportAddress);
    return transportClient;
  }
}
