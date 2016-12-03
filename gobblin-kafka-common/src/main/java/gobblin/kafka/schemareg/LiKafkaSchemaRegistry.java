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

package gobblin.kafka.schemareg;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.kafka.serialize.MD5Digest;
import gobblin.util.AvroUtils;


/**
 * Integration with LinkedIn's implementation of a schema registry that uses md5-hash for schema ids.
 */
public class LiKafkaSchemaRegistry implements KafkaSchemaRegistry<MD5Digest, Schema> {


  private static final Logger LOG = LoggerFactory.getLogger(LiKafkaSchemaRegistry.class);
  private static final String GET_RESOURCE_BY_ID = "/id=";
  private static final String GET_RESOURCE_BY_TYPE = "/latest_with_type=";
  private static final String SCHEMA_ID_HEADER_NAME = "Location";
  private static final String SCHEMA_ID_HEADER_PREFIX = "/id=";

  private final GenericObjectPool<HttpClient> httpClientPool;
  private final String url;

  /**
   * @param props properties should contain property "kafka.schema.registry.url", and optionally
   * "kafka.schema.registry.max.cache.size" (default = 1000) and
   * "kafka.schema.registry.cache.expire.after.write.min" (default = 10).
   */
  public LiKafkaSchemaRegistry(Properties props) {
    Preconditions.checkArgument(props.containsKey(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_URL),
        String.format("Property %s not provided.", KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_URL));

    this.url = props.getProperty(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_URL);

    int objPoolSize =
        Integer.parseInt(props.getProperty(ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS,
            "" + ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT));
    LOG.info("Create HttpClient pool with size " + objPoolSize);

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(objPoolSize);
    config.setMaxIdle(objPoolSize);
    this.httpClientPool = new GenericObjectPool<>(new HttpClientFactory(), config);
  }


  @Override
  public Schema getById(MD5Digest id)
      throws IOException, SchemaRegistryException {
    return fetchSchemaByKey(id);
  }

  /**
   *
   * @return whether this implementation of the schema registry has an internal cache
   */
  @Override
  public boolean hasInternalCache() {
    return false;
  }

  /**
   * Get the latest schema of a topic.
   *
   * @param topic topic name
   * @return the latest schema
   * @throws SchemaRegistryException if failed to retrieve schema.
   */
  @Override
  public Schema getLatestSchema(String topic) throws SchemaRegistryException {
    String schemaUrl = this.url + GET_RESOURCE_BY_TYPE + topic;

    LOG.debug("Fetching from URL : " + schemaUrl);

    GetMethod get = new GetMethod(schemaUrl);

    int statusCode;
    String schemaString;
    HttpClient httpClient = this.borrowClient();
    try {
      statusCode = httpClient.executeMethod(get);
      schemaString = get.getResponseBodyAsString();
    } catch (HttpException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      get.releaseConnection();
      this.httpClientPool.returnObject(httpClient);
    }

    if (statusCode != HttpStatus.SC_OK) {
      throw new SchemaRegistryException(
          String.format("Latest schema for topic %s cannot be retrieved. Status code = %d", topic, statusCode));
    }

    Schema schema;
    try {
      schema = new Schema.Parser().parse(schemaString);
    } catch (Throwable t) {
      throw new SchemaRegistryException(String.format("Latest schema for topic %s cannot be retrieved", topic), t);
    }

    return schema;
  }

  private HttpClient borrowClient() throws SchemaRegistryException {
    try {
      return this.httpClientPool.borrowObject();
    } catch (Exception e) {
      throw new SchemaRegistryException("Unable to borrow " + HttpClient.class.getSimpleName());
    }
  }

  /**
   * Register a schema to the Kafka schema registry under the provided input name. This method will change the name
   * of the schema to the provided name. This is useful because certain services (like Gobblin kafka adaptor and
   * Camus) get the schema for a topic by querying for the latest schema with the topic name, requiring the topic
   * name and schema name to match for all topics. This method registers the schema to the schema registry in such a
   * way that any schema can be written to any topic.
   *
   * @param schema {@link org.apache.avro.Schema} to register.
   * @param name Name of the schema when registerd to the schema registry. This name should match the name
   *                     of the topic where instances will be published.
   * @return schema ID of the registered schema.
   * @throws SchemaRegistryException if registration failed
   */
  @Override
  public MD5Digest register(String name, Schema schema) throws SchemaRegistryException {
    return register(AvroUtils.switchName(schema, name));
  }

  /**
   * Register a schema to the Kafka schema registry
   *
   * @param schema
   * @return schema ID of the registered schema
   * @throws SchemaRegistryException if registration failed
   */
  public synchronized MD5Digest register(Schema schema) throws SchemaRegistryException {
    LOG.info("Registering schema " + schema.toString());

    PostMethod post = new PostMethod(url);
    post.addParameter("schema", schema.toString());

    HttpClient httpClient = this.borrowClient();
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
      MD5Digest schemaId = MD5Digest.fromString(schemaKey);
      return schemaId;
    } catch (Throwable t) {
      throw new SchemaRegistryException(t);
    } finally {
      post.releaseConnection();
      this.httpClientPool.returnObject(httpClient);
    }
  }

  /**
   * Fetch schema by key.
   */
  protected Schema fetchSchemaByKey(MD5Digest key) throws SchemaRegistryException {
    String schemaUrl = this.url + GET_RESOURCE_BY_ID + key.asString();

    GetMethod get = new GetMethod(schemaUrl);

    int statusCode;
    String schemaString;
    HttpClient httpClient = this.borrowClient();
    try {
      statusCode = httpClient.executeMethod(get);
      schemaString = get.getResponseBodyAsString();
    } catch (IOException e) {
      throw new SchemaRegistryException(e);
    } finally {
      get.releaseConnection();
      this.httpClientPool.returnObject(httpClient);
    }

    if (statusCode != HttpStatus.SC_OK) {
      throw new SchemaRegistryException(
          String.format("Schema with key %s cannot be retrieved, statusCode = %d", key, statusCode));
    }

    Schema schema;
    try {
      schema = new Schema.Parser().parse(schemaString);
    } catch (Throwable t) {
      throw new SchemaRegistryException(String.format("Schema with ID = %s cannot be parsed", key), t);
    }

    return schema;
  }

}
