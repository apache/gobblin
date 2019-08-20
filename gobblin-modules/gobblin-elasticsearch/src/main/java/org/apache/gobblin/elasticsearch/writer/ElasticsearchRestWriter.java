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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.math3.util.Pair;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.Batch;
import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ElasticsearchRestWriter extends ElasticsearchWriterBase implements BatchAsyncDataWriter<Object> {

  private final RestHighLevelClient client;
  private final RestClient lowLevelClient;

  ElasticsearchRestWriter(Config config)
      throws IOException {
    super(config);


    int threadCount = ConfigUtils.getInt(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_THREADPOOL_SIZE,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_THREADPOOL_DEFAULT);
    try {

      PasswordManager passwordManager = PasswordManager.getInstance();
      Boolean sslEnabled = ConfigUtils.getBoolean(config,
          ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_ENABLED,
          ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_ENABLED_DEFAULT);
      if (sslEnabled) {

        // keystore
        String keyStoreType = ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_KEYSTORE_TYPE,
                ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_KEYSTORE_TYPE_DEFAULT);
        String keyStoreFilePassword = passwordManager.readPassword(ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_KEYSTORE_PASSWORD, ""));
        String identityFilepath = ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_KEYSTORE_LOCATION, "");

        // truststore
        String trustStoreType = ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_TYPE,
                ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_TYPE_DEFAULT);
        String trustStoreFilePassword = passwordManager.readPassword(ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_PASSWORD, ""));
        String cacertsFilepath = ConfigUtils
            .getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_LOCATION, "");
        String truststoreAbsolutePath = Paths.get(cacertsFilepath).toAbsolutePath().normalize().toString();
        log.info("Truststore absolutePath is:" + truststoreAbsolutePath);


        this.lowLevelClient =
            buildRestClient(this.hostAddresses, threadCount, true, keyStoreType, keyStoreFilePassword, identityFilepath,
                trustStoreType, trustStoreFilePassword, cacertsFilepath);
      }
      else {
        this.lowLevelClient = buildRestClient(this.hostAddresses, threadCount);
      }
      client = new RestHighLevelClient(this.lowLevelClient);

      log.info("Elasticsearch Rest Writer configured successfully with: indexName={}, "
              + "indexType={}, idMappingEnabled={}, typeMapperClassName={}, ssl={}",
          this.indexName, this.indexType, this.idMappingEnabled, this.typeMapper.getClass().getCanonicalName(),
          sslEnabled);

    } catch (Exception e) {
      throw new IOException("Failed to instantiate rest elasticsearch client", e);
    }
  }

  @Override
  int getDefaultPort() {
    return ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_REST_WRITER_DEFAULT_PORT;
  }


  private static RestClient buildRestClient(List<InetSocketTransportAddress> hosts, int threadCount)
      throws Exception {
    return buildRestClient(hosts, threadCount, false, null, null, null, null, null, null);
  }


  //TODO: Support pass through of configuration (e.g. timeouts etc) of rest client from above
  private static RestClient buildRestClient(List<InetSocketTransportAddress> hosts, int threadCount, boolean sslEnabled,
      String keyStoreType, String keyStoreFilePassword, String identityFilepath, String trustStoreType,
      String trustStoreFilePassword, String cacertsFilepath) throws Exception {


    HttpHost[] httpHosts = new HttpHost[hosts.size()];
    String scheme = sslEnabled?"https":"http";
    for (int h = 0; h < httpHosts.length; h++) {
      InetSocketTransportAddress host = hosts.get(h);
      httpHosts[h] = new HttpHost(host.getAddress(), host.getPort(), scheme);
    }

    RestClientBuilder builder = RestClient.builder(httpHosts);

    if (sslEnabled) {
      log.info("ssl configuration: trustStoreType = {}, cacertsFilePath = {}", trustStoreType, cacertsFilepath);
      KeyStore truststore = KeyStore.getInstance(trustStoreType);
      FileInputStream trustInputStream = new FileInputStream(cacertsFilepath);
      try {
        truststore.load(trustInputStream, trustStoreFilePassword.toCharArray());
      }
      finally {
        trustInputStream.close();
      }
      SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);

      log.info("ssl key configuration: keyStoreType = {}, keyFilePath = {}", keyStoreType, identityFilepath);

      KeyStore keystore = KeyStore.getInstance(keyStoreType);
      FileInputStream keyInputStream = new FileInputStream(identityFilepath);
      try {
        keystore.load(keyInputStream, keyStoreFilePassword.toCharArray());
      }
      finally {
        keyInputStream.close();
      }
      sslBuilder.loadKeyMaterial(keystore, keyStoreFilePassword.toCharArray());

      final SSLContext sslContext = sslBuilder.build();
      builder = builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
          // Set ssl context
          .setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier())
          // Configure number of threads for clients
          .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));
    } else {
      builder = builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
          // Configure number of threads for clients
          .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));
    }

    // Configure timeouts
    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
        .setConnectionRequestTimeout(0)); // Important, otherwise the client has spurious timeouts

    return builder.build();
  }

  @Override
  public Future<WriteResponse> write(final Batch<Object> batch, @Nullable WriteCallback callback) {

    Pair<BulkRequest, FutureCallbackHolder> preparedBatch = this.prepareBatch(batch, callback);
    try {
      client.bulkAsync(preparedBatch.getFirst(), preparedBatch.getSecond().getActionListener());
      return preparedBatch.getSecond().getFuture();
    }
    catch (Exception e) {
      throw new RuntimeException("Caught unexpected exception while calling bulkAsync API", e);
    }
  }



  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {
    super.close();
    this.lowLevelClient.close();
  }

  @VisibleForTesting
  public RestHighLevelClient getRestHighLevelClient() {
    return this.client;
  }

  @VisibleForTesting
  public RestClient getRestLowLevelClient() {
    return this.lowLevelClient;
  }

}
