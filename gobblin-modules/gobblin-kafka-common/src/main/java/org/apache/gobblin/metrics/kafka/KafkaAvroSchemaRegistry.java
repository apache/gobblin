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

package gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
import gobblin.kafka.schemareg.HttpClientFactory;
import gobblin.util.AvroUtils;


/**
 * An implementation of {@link KafkaSchemaRegistry}.
 *
 * @author Ziyang Liu
 */
public class KafkaAvroSchemaRegistry extends KafkaSchemaRegistry<String, Schema> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSchemaRegistry.class);

  private static final String GET_RESOURCE_BY_ID = "/id=";
  private static final String GET_RESOURCE_BY_TYPE = "/latest_with_type=";
  private static final String SCHEMA_ID_HEADER_NAME = "Location";
  private static final String SCHEMA_ID_HEADER_PREFIX = "/id=";

  public static final int SCHEMA_ID_LENGTH_BYTE = 16;
  public static final byte MAGIC_BYTE = 0x0;

  private final GenericObjectPool<HttpClient> httpClientPool;
  private final String url;

  /**
   * @param properties properties should contain property "kafka.schema.registry.url", and optionally
   * "kafka.schema.registry.max.cache.size" (default = 1000) and
   * "kafka.schema.registry.cache.expire.after.write.min" (default = 10).
   */
  public KafkaAvroSchemaRegistry(Properties props) {
    super(props);
    Preconditions.checkArgument(props.containsKey(KAFKA_SCHEMA_REGISTRY_URL),
        String.format("Property %s not provided.", KAFKA_SCHEMA_REGISTRY_URL));

    this.url = props.getProperty(KAFKA_SCHEMA_REGISTRY_URL);

    int objPoolSize =
        Integer.parseInt(props.getProperty(ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS,
            "" + ConfigurationKeys.KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT));
    LOG.info("Create HttpClient pool with size " + objPoolSize);

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(objPoolSize);
    config.setMaxIdle(objPoolSize);
    this.httpClientPool = new GenericObjectPool<>(new HttpClientFactory(), config);
  }

  /**
   * Returns the length of schema ids in this schema registry in bytes.
   */
  public int getSchemaIdLengthByte() {
    return SCHEMA_ID_LENGTH_BYTE;
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
      return cachedSchemasByKeys.get(key);
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
  public Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
    String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_TYPE + topic;

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

      return schemaKey;
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
  @Override
  protected Schema fetchSchemaByKey(String key) throws SchemaRegistryException {
    String schemaUrl = KafkaAvroSchemaRegistry.this.url + GET_RESOURCE_BY_ID + key;

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
