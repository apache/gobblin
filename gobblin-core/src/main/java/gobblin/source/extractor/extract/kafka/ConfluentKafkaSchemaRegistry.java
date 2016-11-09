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

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;

import com.google.common.annotations.VisibleForTesting;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;


/**
 * Extension of {@link KafkaSchemaRegistry} that wraps Confluent's {@link SchemaRegistryClient}.
 *
 * <p>
 *   While Confluent's Schema Registry Client API provides more functionality that Gobblin's {@link KafkaSchemaRegistry},
 *   most of the methods are not necessary for Gobblin's Kafka Adaptor. Thus only a subset of the
 *   {@link SchemaRegistryClient} methods are used.
 * </p>
 *
 * <p>
 *   Like the {@link KafkaSchemaRegistry} this class allows fetching a {@link Schema} by a unique {@link Integer} id
 *   that uniquely identifies the {@link Schema}. It is also capable of fetching the latest {@link Schema} for a topic.
 * </p>
 */
@Slf4j
public class ConfluentKafkaSchemaRegistry extends KafkaSchemaRegistry<Integer, Schema> {

  public static final String CONFLUENT_MAX_SCHEMAS_PER_SUBJECT =
      "kafka.schema_registry.confluent.max_schemas_per_subject";

  public static final String CONFLUENT_SCHEMA_NAME_SUFFIX = "kafka.schema_registry.confluent.schema_name_suffix";
  
  // Default suffix of the topic name to register / retrieve from the registry
  private static final String DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX = "-value";
  
  @Getter
  private final SchemaRegistryClient schemaRegistryClient;

  private final String schemaNameSuffix;
  
  public ConfluentKafkaSchemaRegistry(Properties props) {
    this(props, new CachedSchemaRegistryClient(props.getProperty(KAFKA_SCHEMA_REGISTRY_URL),
        Integer.parseInt(props.getProperty(CONFLUENT_MAX_SCHEMAS_PER_SUBJECT, String.valueOf(Integer.MAX_VALUE)))));
  }

  @VisibleForTesting
  ConfluentKafkaSchemaRegistry(Properties props, SchemaRegistryClient schemaRegistryClient) {
    super(props);
    this.schemaRegistryClient = schemaRegistryClient;
    this.schemaNameSuffix = props.getProperty(CONFLUENT_SCHEMA_NAME_SUFFIX, DEFAULT_CONFLUENT_SCHEMA_NAME_SUFFIX);
  }

  @Override
  protected Schema fetchSchemaByKey(Integer key) throws SchemaRegistryException {
    try {
      return this.schemaRegistryClient.getByID(key);
    } catch (IOException | RestClientException e) {
      throw new SchemaRegistryException(e);
    }
  }

  @Override
  public Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
    String schemaName = topic + this.schemaNameSuffix;
    try {
      return new Schema.Parser().parse(this.schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
    } catch (IOException | RestClientException e) {
      log.error("Failed to get schema for topic " + topic + "; subject " + schemaName);
      throw new SchemaRegistryException(e);
    }
  }

  @Override
  public Integer register(Schema schema) throws SchemaRegistryException {
    return register(schema, schema.getName());
  }

  @Override
  public Integer register(Schema schema, String name) throws SchemaRegistryException {
    try {
      String schemaName = name + this.schemaNameSuffix;
      return this.schemaRegistryClient.register(schemaName, schema);
    } catch (IOException | RestClientException e) {
      throw new SchemaRegistryException(e);
    }
  }
}
