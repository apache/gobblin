/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import gobblin.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;


/**
 * A schema registry class that provides two services: get the latest schema of a topic, and register a schema.
 *
 * @author ziliu
 */
public class KafkaAvroSchemaRegistry implements KafkaSchemaRegistry<String, Schema> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSchemaRegistry.class);

  private static final int GET_SCHEMA_BY_ID_MAX_TIRES = 3;
  private static final int GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = 1;
  private static final String GET_RESOURCE_BY_ID = "/id=";
  private static final String GET_RESOURCE_BY_TYPE = "/latest_with_type=";
  private static final String SCHEMA_ID_HEADER_NAME = "Location";
  private static final String SCHEMA_ID_HEADER_PREFIX = "/id=";
  private static final String KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE = "kafka.schema.registry.max.cache.size";
  private static final String DEFAULT_KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE = "1000";
  private static final String KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN =
      "kafka.schema.registry.cache.expire.after.write.min";
  private static final String DEFAULT_KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN = "10";

  public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
  public static final int SCHEMA_ID_LENGTH_BYTE = 16;
  public static final byte MAGIC_BYTE = 0x0;

  private final LoadingCache<String, Schema> cachedSchemasById;
  private final HttpClient httpClient;
  private final String url;

  /**
   * @param properties properties should contain property "kafka.schema.registry.url", and optionally
   * "kafka.schema.registry.max.cache.size" (default = 1000) and
   * "kafka.schema.registry.cache.expire.after.write.min" (default = 10).
   */
  public KafkaAvroSchemaRegistry(Properties properties) {
    Preconditions.checkArgument(properties.containsKey(KAFKA_SCHEMA_REGISTRY_URL),
        String.format("Property %s not provided.", KAFKA_SCHEMA_REGISTRY_URL));

    this.url = properties.getProperty(KAFKA_SCHEMA_REGISTRY_URL);
    int maxCacheSize = Integer.parseInt(
        properties.getProperty(KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE, DEFAULT_KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE));
    int expireAfterWriteMin =
        Integer.parseInt(properties.getProperty(KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN,
            DEFAULT_KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN));
    this.cachedSchemasById = CacheBuilder.newBuilder().maximumSize(maxCacheSize)
        .expireAfterWrite(expireAfterWriteMin, TimeUnit.MINUTES).build(new KafkaSchemaCacheLoader());
    this.httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
  }

  /**
   * Get schema from schema registry by key
   *
   * @param key Schema key
   * @return Schema with the corresponding key
   * @throws SchemaRegistryException if failed to retrieve schema.
   */
  @Override
  public Schema getSchemaByKey(String key) throws SchemaRegistryException {
    try {
      return cachedSchemasById.get(key);
    } catch (ExecutionException e) {
      throw new SchemaRegistryException(String.format("Schema with key %s cannot be retrieved", key), e);
    }
  }

  /**
   * Get the latest schema of a topic.
   *
   * @param topic topic name
   * @return the latest schema
   * @throws SchemaRegistryException if failed to retrieve schema.
   */
  @Override
  public synchronized Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
    String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_TYPE + topic;

    LOG.debug("Fetching from URL : " + schemaUrl);

    GetMethod get = new GetMethod(schemaUrl);

    int statusCode;
    String schemaString;
    try {
      statusCode = KafkaAvroSchemaRegistry.this.httpClient.executeMethod(get);
      schemaString = get.getResponseBodyAsString();
    } catch (HttpException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      get.releaseConnection();
    }

    if (statusCode != HttpStatus.SC_OK) {
      throw new SchemaRegistryException(
          String.format("Latest schema for topic %s cannot be retrieved. Status code = %d", topic, statusCode));
    }

    Schema schema;
    if (schemaString.startsWith("{")) {
      try {
        schema = new Schema.Parser().parse(schemaString);
      } catch (Exception e) {
        throw new SchemaRegistryException(String.format("Latest schema for topic %s cannot be retrieved", topic), e);
      }
    } else {
      throw new SchemaRegistryException(
          String.format("Latest schema for topic %s cannot be retrieved: schema should start with '{'", topic));
    }

    return schema;
  }

  /**
   * Register a schema to the Kafka schema registry under the provided input name. This method will change the name
   * of the schema to the provided name. This is useful because certain services (like Gobblin kafka adaptor and
   * Camus) get the schema for a topic by querying for the latest schema with the topic name, requiring the topic
   * name and schema name to match for all topics. This method registers the schema to the schema registry in such a
   * way that any schema can be written to any topic.
   *
   * @param schema {@link org.apache.avro.Schema} to register.
   * @param overrideName Name of the schema when registerd to the schema registry. This name should match the name
   *                     of the topic where instances will be published.
   * @return schema ID of the registered schema.
   * @throws SchemaRegistryException if registration failed
   */
  @Override
  public String register(Schema schema, String overrideName) throws SchemaRegistryException {
    return register(AvroUtils.switchName(schema, overrideName));
  }

  /**
   * Register a schema to the Kafka schema registry
   *
   * @param schema
   * @return schema ID of the registered schema
   * @throws SchemaRegistryException if registration failed
   */
  @Override
  public synchronized String register(Schema schema) throws SchemaRegistryException {
    LOG.info("Registering schema " + schema.toString());

    PostMethod post = new PostMethod(url);
    post.addParameter("schema", schema.toString());

    try {
      LOG.debug("Loading: " + post.getURI());
      int statusCode = httpClient.executeMethod(post);
      if (statusCode != HttpStatus.SC_CREATED) {
        throw new SchemaRegistryException("Error occurred while trying to register schema: " + statusCode);
      }

      String response;
      response = post.getResponseBodyAsString();
      if (response != null) {
        LOG.info("Received response " + response);
      }

      String schemaKey;
      Header[] headers = post.getResponseHeaders(SCHEMA_ID_HEADER_NAME);
      if (headers.length != 1) {
        throw new SchemaRegistryException(
            "Error reading schema id returned by registerSchema call: headers.length = " + headers.length);
      } else if (!headers[0].getValue().startsWith(SCHEMA_ID_HEADER_PREFIX)) {
        throw new SchemaRegistryException(
            "Error parsing schema id returned by registerSchema call: header = " + headers[0].getValue());
      } else {
        LOG.info("Registered schema successfully");
        schemaKey = headers[0].getValue().substring(SCHEMA_ID_HEADER_PREFIX.length());
      }

      return schemaKey;
    } catch (Throwable t) {
      throw new SchemaRegistryException(t);
    } finally {
      post.releaseConnection();
    }
  }

  private class KafkaSchemaCacheLoader extends CacheLoader<String, Schema> {

    private final Map<String, FailedFetchHistory> failedFetchHistories;

    private KafkaSchemaCacheLoader() {
      super();
      this.failedFetchHistories = Maps.newHashMap();
    }

    @Override
    public Schema load(String key) throws Exception {
      if (shouldFetchFromSchemaRegistry(key)) {
        try {
          return fetchSchemaByKey(key);
        } catch (SchemaRegistryException e) {
          addFetchToFailureHistory(key);
          throw e;
        }
      }

      // Throw exception if we've just tried to fetch this id, or if we've tried too many times for this id.
      throw new SchemaRegistryException(String.format("Schema with key %s cannot be retrieved", key));
    }

    private void addFetchToFailureHistory(String id) {
      if (!this.failedFetchHistories.containsKey(id)) {
        this.failedFetchHistories.put(id, new FailedFetchHistory(1, System.nanoTime()));
      } else {
        this.failedFetchHistories.get(id).incrementNumOfAttempts();
        this.failedFetchHistories.get(id).setPreviousAttemptTime(System.nanoTime());
      }
    }

    private boolean shouldFetchFromSchemaRegistry(String id) {
      if (!this.failedFetchHistories.containsKey(id)) {
        return true;
      }
      FailedFetchHistory failedFetchHistory = this.failedFetchHistories.get(id);
      boolean maxTriesNotExceeded = failedFetchHistory.getNumOfAttempts() < GET_SCHEMA_BY_ID_MAX_TIRES;
      boolean minRetryIntervalSatisfied =
          System.nanoTime() - failedFetchHistory.getPreviousAttemptTime() >= TimeUnit.SECONDS
              .toNanos(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS);
      return maxTriesNotExceeded && minRetryIntervalSatisfied;
    }

    private Schema fetchSchemaByKey(String key) throws SchemaRegistryException, HttpException, IOException {
      String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_ID + key;

      GetMethod get = new GetMethod(schemaUrl);

      int statusCode;
      String schemaString;
      try {
        statusCode = KafkaAvroSchemaRegistry.this.httpClient.executeMethod(get);
        schemaString = get.getResponseBodyAsString();
      } finally {
        get.releaseConnection();
      }

      if (statusCode != HttpStatus.SC_OK) {
        throw new SchemaRegistryException(
            String.format("Schema with key %s cannot be retrieved, statusCode = %d", key, statusCode));
      }

      Schema schema;
      if (schemaString.startsWith("{")) {
        try {
          schema = new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
          throw new SchemaRegistryException(String.format("Schema with ID = %s cannot be parsed", key), e);
        }
      } else {
        throw new SchemaRegistryException(
            String.format("Schema with key %s cannot be parsed: schema should start with '{'", key));
      }

      return schema;
    }

    private class FailedFetchHistory {
      private int getNumOfAttempts() {
        return numOfAttempts;
      }

      private long getPreviousAttemptTime() {
        return previousAttemptTime;
      }

      private void setPreviousAttemptTime(long previousAttemptTime) {
        this.previousAttemptTime = previousAttemptTime;
      }

      private void incrementNumOfAttempts() {
        this.numOfAttempts++;
      }

      private int numOfAttempts;
      private long previousAttemptTime;

      private FailedFetchHistory(int numOfAttempts, long previousAttemptTime) {
        this.numOfAttempts = numOfAttempts;
        this.previousAttemptTime = previousAttemptTime;
      }
    }
  }
}
