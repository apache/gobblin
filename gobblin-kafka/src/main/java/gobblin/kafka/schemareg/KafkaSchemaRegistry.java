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

import org.apache.avro.Schema;


/**
 * An interface for a Kafka Schema Registry
 *
 * @param <K> : the type of the schema identifier (e.g. int, string, md5, ...)
 *
 */
public interface  KafkaSchemaRegistry<K> {

  /**
   * Register this schema under the provided name
   * @param name
   * @param schema
   * @return the schema identifier
   * @throws IOException
   * @throws SchemaRegistryException
   */
  public K register(String name, Schema schema) throws IOException, SchemaRegistryException;

  /**
   * Get a schema given an id
   * @param id
   * @return the Schema
   * @throws IOException
   * @throws SchemaRegistryException
   */
  public Schema getById(K id)
      throws IOException, SchemaRegistryException;

  /**
   * Get the latest schema that was registered under this name
   * @param name
   * @return the latest Schema
   * @throws SchemaRegistryException
   */
  public Schema getLatestSchema(String name) throws IOException, SchemaRegistryException;



  /**
   *
   * SchemaRegistry implementations that do not have an internal cache can set this to false
   * and Gobblin will supplement such registries with a cache on top (if enabled).
   * @return whether this implementation of the schema registry has an internal cache
   */
  boolean hasInternalCache();


}
