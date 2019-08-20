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
import java.util.Collections;

import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.testng.Assert;

import com.typesafe.config.Config;


/**
 * A variant that uses the {@link ElasticsearchRestWriter}
 */
public class RestWriterVariant implements WriterVariant {

  private ElasticsearchRestWriter _restWriter;
  @Override
  public String getName() {
    return "rest";
  }

  @Override
  public ConfigBuilder getConfigBuilder() {
    return new ConfigBuilder()
        .setClientType("REST");
  }

  @Override
  public BatchAsyncDataWriter getBatchAsyncDataWriter(Config config)
      throws IOException {
    _restWriter = new ElasticsearchRestWriter(config);
    return _restWriter;
  }

  @Override
  public TestClient getTestClient(Config config)
      throws IOException {
    final ElasticsearchRestWriter restWriter = new ElasticsearchRestWriter(config);
    final RestHighLevelClient highLevelClient = restWriter.getRestHighLevelClient();
    return new TestClient() {
      @Override
      public GetResponse get(GetRequest getRequest)
          throws IOException {
        return highLevelClient.get(getRequest);
      }

      @Override
      public void recreateIndex(String indexName)
          throws IOException {
        RestClient restClient = restWriter.getRestLowLevelClient();
        try {
          restClient.performRequest("DELETE", "/" + indexName);
        } catch (Exception e) {
          // ok since index may not exist
        }

        String indexSettings = "{\"settings\" : {\"index\":{\"number_of_shards\":1,\"number_of_replicas\":1}}}";
        HttpEntity entity = new StringEntity(indexSettings, ContentType.APPLICATION_JSON);

        Response putResponse = restClient.performRequest("PUT", "/" + indexName, Collections.emptyMap(), entity);
        Assert.assertEquals(putResponse.getStatusLine().getStatusCode(),200, "Recreate index succeeded");
      }

      @Override
      public void close()
          throws IOException {
        restWriter.close();

      }
    };
  }
}
