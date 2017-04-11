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

package gobblin.kafka.schemareg;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;


/**
 * An implementation that wraps a passed in schema registry and caches interactions with it
 * {@inheritDoc}
 * */
@Slf4j
public class CachingKafkaSchemaRegistry<K,S> implements KafkaSchemaRegistry<K,S> {

  private static final int DEFAULT_MAX_SCHEMA_REFERENCES = 10;
  private final KafkaSchemaRegistry<K,S> _kafkaSchemaRegistry;
  private final HashMap<String, Map<S, K>> _namedSchemaCache;
  private final HashMap<K, S> _idBasedCache;
  private final int _maxSchemaReferences;


  public CachingKafkaSchemaRegistry(KafkaSchemaRegistry kafkaSchemaRegistry)
  {
    this(kafkaSchemaRegistry, DEFAULT_MAX_SCHEMA_REFERENCES);
  }

  /**
   * Create a caching schema registry.
   * @param kafkaSchemaRegistry: a schema registry that needs caching
   * @param maxSchemaReferences: the maximum number of unique references that can exist for a given schema.
   */
  public CachingKafkaSchemaRegistry(KafkaSchemaRegistry kafkaSchemaRegistry, int maxSchemaReferences)
  {
    Preconditions.checkArgument(kafkaSchemaRegistry!=null, "KafkaSchemaRegistry cannot be null");
    Preconditions.checkArgument(!kafkaSchemaRegistry.hasInternalCache(), "SchemaRegistry already has a cache.");
    _kafkaSchemaRegistry = kafkaSchemaRegistry;
    _namedSchemaCache = new HashMap<>();
    _idBasedCache = new HashMap<>();
    _maxSchemaReferences = maxSchemaReferences;
  }

  @Override
  synchronized public K register(String name, S schema)
      throws IOException, SchemaRegistryException {

    Map<S, K> schemaIdMap;
    if (_namedSchemaCache.containsKey(name))
    {
      schemaIdMap = _namedSchemaCache.get(name);
    }
    else {
      // we really care about reference equality to de-dup using cache
      // when it comes to registering schemas, so use an IdentityHashMap here
      schemaIdMap = new IdentityHashMap<>();
      _namedSchemaCache.put(name, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema))
    {
      return schemaIdMap.get(schema);
    }
    else
    {
      // check if schemaIdMap is getting too full
      Preconditions.checkState(schemaIdMap.size() < _maxSchemaReferences, "Too many schema objects for " + name +". Cache is overfull.");
    }
    K id = _kafkaSchemaRegistry.register(name, schema);
    schemaIdMap.put(schema, id);
    _idBasedCache.put(id, schema);
    return id;
  }

  @Override
  synchronized public S getById(K id)
      throws IOException, SchemaRegistryException {
    if (_idBasedCache.containsKey(id))
    {
      return _idBasedCache.get(id);
    }
    else
    {
      S schema = _kafkaSchemaRegistry.getById(id);
      _idBasedCache.put(id, schema);
      return schema;
    }
  }

  /**
   * This call is not cached because we never want to miss out on the latest schema.
   * {@inheritDoc}
   */
  @Override
  public S getLatestSchema(String name)
      throws IOException, SchemaRegistryException {
    return _kafkaSchemaRegistry.getLatestSchema(name);
  }

  @Override
  public boolean hasInternalCache() {
    return true;
  }
}
