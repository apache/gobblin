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

package org.apache.gobblin.kafka.schemareg;

import java.io.IOException;

import org.apache.avro.Schema;


/**
 * An interface for a Kafka Schema Registry
 * Classes implementing this interface will typically be constructed by a {@link KafkaSchemaRegistryFactory}
 * and should have a constructor that takes a {@link java.util.Properties} object as a parameter.
 *
 * @param <K> : the type of the schema identifier (e.g. int, string, md5, ...)
 * @param <S> : the type of the schema system in use (e.g. avro's Schema, ... )
 */
public interface  KafkaSchemaRegistry<K, S> {

  /**
   * Register this schema under the provided name
   * @param name
   * @param schema
   * @return the schema identifier
   * @throws IOException
   * @throws SchemaRegistryException
   */
  public K register(String name, S schema) throws IOException, SchemaRegistryException;

  /**
   * Get a schema given an id
   * @param id
   * @return the Schema
   * @throws IOException
   * @throws SchemaRegistryException
   */
  public S getById(K id)
      throws IOException, SchemaRegistryException;

  /**
   * Get the latest schema that was registered under this name
   * @param name
   * @return the latest Schema
   * @throws SchemaRegistryException
   */
  public S getLatestSchema(String name) throws IOException, SchemaRegistryException;



  /**
   *
   * SchemaRegistry implementations that do not have an internal cache can set this to false
   * and Gobblin will supplement such registries with a cache on top (if enabled).
   * @return whether this implementation of the schema registry has an internal cache
   */
  boolean hasInternalCache();


}
