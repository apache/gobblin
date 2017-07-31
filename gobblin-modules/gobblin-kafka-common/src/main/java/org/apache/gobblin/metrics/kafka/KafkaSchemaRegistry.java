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

package org.apache.gobblin.metrics.kafka;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;


/**
 * A abstract schema registry class for Kafka, which supports fetching schema by key, fetching the latest schema
 * of a topic, and registering a schema.
 *
 * @param <K> key type
 * @param <S> schema type
 *
 * @author Ziyang Liu
 */
@Slf4j
public abstract class KafkaSchemaRegistry<K, S> {

  public static final String KAFKA_SCHEMA_REGISTRY_CLASS = "kafka.schema.registry.class";
  public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";

  public static final int GET_SCHEMA_BY_ID_MAX_TIRES = 3;
  public static final int GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = 1;
  public static final String KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE = "kafka.schema.registry.max.cache.size";
  public static final String DEFAULT_KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE = "1000";
  public static final String KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN =
      "kafka.schema.registry.cache.expire.after.write.min";
  public static final String DEFAULT_KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN = "10";

  protected final Properties props;

  // Cache that stores schemas by keys.
  protected final LoadingCache<K, S> cachedSchemasByKeys;

  protected KafkaSchemaRegistry(Properties props) {
    this.props = props;
    int maxCacheSize = Integer.parseInt(
        props.getProperty(KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE, DEFAULT_KAFKA_SCHEMA_REGISTRY_MAX_CACHE_SIZE));
    int expireAfterWriteMin = Integer.parseInt(props.getProperty(KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN,
        DEFAULT_KAFKA_SCHEMA_REGISTRY_CACHE_EXPIRE_AFTER_WRITE_MIN));
    this.cachedSchemasByKeys = CacheBuilder.newBuilder().maximumSize(maxCacheSize)
        .expireAfterWrite(expireAfterWriteMin, TimeUnit.MINUTES).build(new KafkaSchemaCacheLoader());
  }

  @SuppressWarnings("unchecked")
  public static <K, S> KafkaSchemaRegistry<K, S> get(Properties props) {
    Preconditions.checkArgument(props.containsKey(KAFKA_SCHEMA_REGISTRY_CLASS),
        "Missing required property " + KAFKA_SCHEMA_REGISTRY_CLASS);
    Class<? extends KafkaSchemaRegistry<?, ?>> clazz;
    try {
      clazz =
          (Class<? extends KafkaSchemaRegistry<?, ?>>) Class.forName(props.getProperty(KAFKA_SCHEMA_REGISTRY_CLASS));
      return (KafkaSchemaRegistry<K, S>) ConstructorUtils.invokeConstructor(clazz, props);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException
        | InstantiationException e) {
      log.error("Failed to instantiate " + KafkaSchemaRegistry.class, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get schema from schema registry by key.
   * @throws SchemaRegistryException if failed to get schema by key.
   */
  public S getSchemaByKey(K key) throws SchemaRegistryException {
    try {
      return cachedSchemasByKeys.get(key);
    } catch (ExecutionException e) {
      throw new SchemaRegistryException(String.format("Schema with key %s cannot be retrieved", key), e);
    }
  }

  /**
   * Fetch schema by key.
   *
   * This method is called in {@link #getSchemaByKey(K)} to fetch the schema if the given key does not exist
   * in the cache.
   * @throws SchemaRegistryException if failed to fetch schema by key.
   */
  protected abstract S fetchSchemaByKey(K key) throws SchemaRegistryException;

  /**
   * Get the latest schema of a topic.
   * @throws SchemaRegistryException if failed to get schema by topic.
   */
  public abstract S getLatestSchemaByTopic(String topic) throws SchemaRegistryException;

  /**
   * Register a schema to the schema registry
   * @return the key of the registered schema.
   * @throws SchemaRegistryException if registration failed.
   */
  public abstract K register(S schema) throws SchemaRegistryException;

  /**
   * Register a schema to the schema registry under the given name
   * @return the key of the registered schema.
   * @throws SchemaRegistryException if registration failed.
   */
  public abstract K register(S schema, String name) throws SchemaRegistryException;

  private class KafkaSchemaCacheLoader extends CacheLoader<K, S> {

    private final ConcurrentMap<K, FailedFetchHistory> failedFetchHistories;

    private KafkaSchemaCacheLoader() {
      super();
      this.failedFetchHistories = Maps.newConcurrentMap();
    }

    @Override
    public S load(K key) throws Exception {
      if (shouldFetchFromSchemaRegistry(key)) {
        try {
          return KafkaSchemaRegistry.this.fetchSchemaByKey(key);
        } catch (SchemaRegistryException e) {
          addFetchToFailureHistory(key);
          throw e;
        }
      }

      // Throw exception if we've just tried to fetch this id, or if we've tried too many times for this id.
      throw new SchemaRegistryException(String.format("Schema with key %s cannot be retrieved", key));
    }

    private void addFetchToFailureHistory(K key) {
      this.failedFetchHistories.putIfAbsent(key, new FailedFetchHistory(System.nanoTime()));
      this.failedFetchHistories.get(key).incrementNumOfAttempts();
      this.failedFetchHistories.get(key).setPreviousAttemptTime(System.nanoTime());
    }

    private boolean shouldFetchFromSchemaRegistry(K key) {
      if (!this.failedFetchHistories.containsKey(key)) {
        return true;
      }
      FailedFetchHistory failedFetchHistory = this.failedFetchHistories.get(key);
      boolean maxTriesNotExceeded = failedFetchHistory.getNumOfAttempts() < GET_SCHEMA_BY_ID_MAX_TIRES;
      boolean minRetryIntervalSatisfied =
          System.nanoTime() - failedFetchHistory.getPreviousAttemptTime() >= TimeUnit.SECONDS
              .toNanos(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS);
      return maxTriesNotExceeded && minRetryIntervalSatisfied;
    }

    private class FailedFetchHistory {

      private final AtomicInteger numOfAttempts;
      private long previousAttemptTime;

      private FailedFetchHistory(long previousAttemptTime) {
        this.numOfAttempts = new AtomicInteger();
        this.previousAttemptTime = previousAttemptTime;
      }

      private int getNumOfAttempts() {
        return numOfAttempts.get();
      }

      private long getPreviousAttemptTime() {
        return previousAttemptTime;
      }

      private void setPreviousAttemptTime(long previousAttemptTime) {
        this.previousAttemptTime = previousAttemptTime;
      }

      private void incrementNumOfAttempts() {
        this.numOfAttempts.incrementAndGet();
      }
    }
  }

}
