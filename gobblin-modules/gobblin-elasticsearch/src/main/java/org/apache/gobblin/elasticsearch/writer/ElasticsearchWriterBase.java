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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.math3.util.Pair;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.elasticsearch.typemapping.JsonSerializer;
import org.apache.gobblin.elasticsearch.typemapping.TypeMapper;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.Batch;
import org.apache.gobblin.writer.WriteCallback;

/**
 * A base class for different types of Elasticsearch writers
 */
@Slf4j
public abstract class ElasticsearchWriterBase implements Closeable {
  protected final String indexName;
  protected final String indexType;
  protected final TypeMapper typeMapper;
  protected final JsonSerializer serializer;
  protected final boolean idMappingEnabled;
  protected final String idFieldName;
  List<InetSocketTransportAddress> hostAddresses;
  protected final MalformedDocPolicy malformedDocPolicy;

  ElasticsearchWriterBase(Config config)
      throws UnknownHostException {

    this.indexName = config.getString(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_NAME);
    Preconditions.checkNotNull(this.indexName, "Index Name not provided. Please set "
        + ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_NAME);
    Preconditions.checkArgument(this.indexName.equals(this.indexName.toLowerCase()),
        "Index name must be lowercase, you provided " + this.indexName);
    this.indexType = config.getString(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_TYPE);
    Preconditions.checkNotNull(this.indexName, "Index Type not provided. Please set "
        + ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_TYPE);
    this.idMappingEnabled = ConfigUtils.getBoolean(config,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_MAPPING_ENABLED,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_MAPPING_DEFAULT);
    this.idFieldName = ConfigUtils.getString(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_FIELD,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_FIELD_DEFAULT);
    String typeMapperClassName = ConfigUtils.getString(config,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS_DEFAULT);
    if (typeMapperClassName.isEmpty()) {
      throw new IllegalArgumentException(this.getClass().getCanonicalName() + " needs to be configured with "
          + ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS + " to enable type mapping");
    }
    try {
      Class<?> typeMapperClass = (Class<?>) Class.forName(typeMapperClassName);

      this.typeMapper = (TypeMapper) ConstructorUtils.invokeConstructor(typeMapperClass);
      this.typeMapper.configure(config);
      this.serializer = this.typeMapper.getSerializer();
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      log.error("Failed to instantiate type-mapper from class " + typeMapperClassName, e);
      throw Throwables.propagate(e);
    }

    this.malformedDocPolicy = MalformedDocPolicy.valueOf(ConfigUtils.getString(config,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY,
        ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY_DEFAULT));

    // If list is empty, connect to the default host and port
    if (!config.hasPath(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS)) {
      InetSocketTransportAddress hostAddress = new InetSocketTransportAddress(
          InetAddress.getByName(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_DEFAULT_HOST),
          getDefaultPort());
      this.hostAddresses = new ArrayList<>(1);
      this.hostAddresses.add(hostAddress);
      log.info("Adding host {} to Elasticsearch writer", hostAddress);
    } else {
      // Get list of hosts
      List<String> hosts = ConfigUtils.getStringList(config, ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS);
      // Add host addresses
      Splitter hostSplitter = Splitter.on(":").trimResults();
      this.hostAddresses = new ArrayList<>(hosts.size());
      for (String host : hosts) {

        List<String> hostSplit = hostSplitter.splitToList(host);
        Preconditions.checkArgument(hostSplit.size() == 1 || hostSplit.size() == 2,
            "Malformed host name for Elasticsearch writer: " + host + " host names must be of form [host] or [host]:[port]");

        InetAddress hostInetAddress = InetAddress.getByName(hostSplit.get(0));
        InetSocketTransportAddress hostAddress = null;

        if (hostSplit.size() == 1) {
          hostAddress = new InetSocketTransportAddress(hostInetAddress, this.getDefaultPort());
        } else if (hostSplit.size() == 2) {
          hostAddress = new InetSocketTransportAddress(hostInetAddress, Integer.parseInt(hostSplit.get(1)));
        }
        this.hostAddresses.add(hostAddress);
        log.info("Adding host {} to Elasticsearch writer", hostAddress);
      }
    }
  }

  abstract int getDefaultPort();


  protected Pair<BulkRequest, FutureCallbackHolder> prepareBatch(Batch<Object> batch, WriteCallback callback) {
    BulkRequest bulkRequest = new BulkRequest();
    final StringBuilder stringBuilder = new StringBuilder();
    for (Object record : batch.getRecords()) {
      try {
        byte[] serializedBytes = this.serializer.serializeToJson(record);
        log.debug("serialized record: {}", serializedBytes);
        IndexRequest indexRequest = new IndexRequest(this.indexName, this.indexType)
            .source(serializedBytes, 0, serializedBytes.length, XContentType.JSON);
        if (this.idMappingEnabled) {
          String id = this.typeMapper.getValue(this.idFieldName, record);
          indexRequest.id(id);
          stringBuilder.append(";").append(id);
        }
        bulkRequest.add(indexRequest);
      }
      catch (Exception e) {
        log.error("Encountered exception {}", e);
      }
    }
    FutureCallbackHolder futureCallbackHolder = new FutureCallbackHolder(callback,
        exception -> log.error("Batch: {} failed on ids; {} with exception {}", batch.getId(),
            stringBuilder.toString(), exception),
        this.malformedDocPolicy);
    return new Pair(bulkRequest, futureCallbackHolder);
  }

  @Override
  public void close() throws IOException {
    this.serializer.close();
  }

}
