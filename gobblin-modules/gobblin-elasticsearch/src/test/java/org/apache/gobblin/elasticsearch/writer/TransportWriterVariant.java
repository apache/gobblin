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

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.testng.Assert;

import com.typesafe.config.Config;

import org.apache.gobblin.writer.BatchAsyncDataWriter;


/**
 * A variant that uses the {@link ElasticsearchTransportClientWriter}
 */
public class TransportWriterVariant implements WriterVariant {
  @Override
  public String getName() {
    return "transport";
  }

  @Override
  public ConfigBuilder getConfigBuilder() {
    return new ConfigBuilder()
        .setClientType("transport");
  }

  @Override
  public BatchAsyncDataWriter getBatchAsyncDataWriter(Config config)
      throws IOException {
    ElasticsearchTransportClientWriter transportClientWriter = new ElasticsearchTransportClientWriter(config);
    return transportClientWriter;
  }

  @Override
  public TestClient getTestClient(Config config)
      throws IOException {
    final ElasticsearchTransportClientWriter transportClientWriter = new ElasticsearchTransportClientWriter(config);
    final TransportClient transportClient = transportClientWriter.getTransportClient();
    return new TestClient() {
      @Override
      public GetResponse get(GetRequest getRequest)
          throws IOException {
        try {
          return transportClient.get(getRequest).get();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }

      @Override
      public void recreateIndex(String indexName)
          throws IOException {
        DeleteIndexRequestBuilder dirBuilder = transportClient.admin().indices().prepareDelete(indexName);
        try {
          DeleteIndexResponse diResponse = dirBuilder.execute().actionGet();
        } catch (IndexNotFoundException ie) {
          System.out.println("Index not found... that's ok");
        }

        CreateIndexRequestBuilder cirBuilder = transportClient.admin().indices().prepareCreate(indexName);
        CreateIndexResponse ciResponse = cirBuilder.execute().actionGet();
        Assert.assertTrue(ciResponse.isAcknowledged(), "Create index succeeeded");
      }

      @Override
      public void close()
          throws IOException {
        transportClientWriter.close();
      }
    };
  }
}
