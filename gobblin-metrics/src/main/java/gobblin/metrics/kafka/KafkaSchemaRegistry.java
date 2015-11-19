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

/**
 * A schema registry interface for Kafka, which supports fetching schema by key, fetching the latest schema
 * of a topic, and registering a schema.
 *
 * @param <K> key type
 * @param <S> schema type
 *
 * @author ziliu
 */
public interface KafkaSchemaRegistry<K, S> {

  /**
   * Get schema from schema registry by key.
   * @throws SchemaRegistryException if the given key doesn't exist in the schema registry.
   */
  public S getSchemaByKey(K key) throws SchemaRegistryException;

  /**
   * Get the latest schema of a topic.
   * @throws SchemaRegistryException if the given topic doesn't exist in the schema registry.
   */
  public S getLatestSchemaByTopic(String topic) throws SchemaRegistryException;

  /**
   * Register a schema to the schema registry
   * @return the key of the registered schema.
   * @throws SchemaRegistryException if registration failed.
   */
  public K register(S schema) throws SchemaRegistryException;

  /**
   * Register a schema to the schema registry under the given name
   * @return the key of the registered schema.
   * @throws SchemaRegistryException if registration failed.
   */
  public K register(S schema, String name) throws SchemaRegistryException;

}
