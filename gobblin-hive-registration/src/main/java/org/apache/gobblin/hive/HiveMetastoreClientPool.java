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

package org.apache.gobblin.hive;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AutoReturnableObject;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A pool of {@link IMetaStoreClient} for querying the Hive metastore.
 */
@Slf4j
public class HiveMetastoreClientPool {

  private final GenericObjectPool<IMetaStoreClient> pool;
  private final HiveMetaStoreClientFactory factory;
  @Getter
  private final HiveConf hiveConf;
  @Getter
  private final HiveRegProps hiveRegProps;

  private static final long DEFAULT_POOL_CACHE_TTL_MINUTES = 30;

  public static final String POOL_CACHE_TTL_MINUTES_KEY = "hive.metaStorePoolCache.ttl";

  public static final String POOL_EVICTION_POLICY_CLASS_NAME = "pool.eviction.policy.class.name";

  public static final String DEFAULT_POOL_EVICTION_POLICY_CLASS_NAME = "org.apache.commons.pool2.impl.DefaultEvictionPolicy";

  public static final String POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "pool.min.evictable.idle.time.millis";

  /**
   * To provide additional or override configuration of a certain hive metastore,
   * <p> firstly, set {@code hive.additionalConfig.targetUri=<the target hive metastore uri>}
   * <p> Then all configurations with {@value #POOL_HIVE_ADDITIONAL_CONFIG_PREFIX} prefix will be extracted
   * out of the job configurations and applied on top. for example, if there is a job configuration
   * {@code hive.additionalConfig.hive.metastore.sasl.enabled=false},
   * {@code hive.metastore.sasl.enabled=false} will be extracted and applied
   */
  public static final String POOL_HIVE_ADDITIONAL_CONFIG_PREFIX = "hive.additionalConfig.";

  public static final String POOL_HIVE_ADDITIONAL_CONFIG_TARGET = POOL_HIVE_ADDITIONAL_CONFIG_PREFIX + "targetUri";

  public static final long DEFAULT_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = 600000L;

  public static final String POOL_TIME_BETWEEN_EVICTION_MILLIS = "pool.time.between eviction.millis";

  public static final long DEFAULT_POOL_TIME_BETWEEN_EVICTION_MILLIS = 60000L;


  private static Cache<Optional<String>, HiveMetastoreClientPool> poolCache = null;

  private static final Cache<Optional<String>, HiveMetastoreClientPool> createPoolCache(final Properties properties) {
    long duration = properties.containsKey(POOL_CACHE_TTL_MINUTES_KEY)
        ? Long.parseLong(properties.getProperty(POOL_CACHE_TTL_MINUTES_KEY)) : DEFAULT_POOL_CACHE_TTL_MINUTES;
    return CacheBuilder.newBuilder()
        .expireAfterAccess(duration, TimeUnit.MINUTES)
        .removalListener(new RemovalListener<Optional<String>, HiveMetastoreClientPool>() {
          @Override
          public void onRemoval(RemovalNotification<Optional<String>, HiveMetastoreClientPool> notification) {
            if (notification.getValue() != null) {
              notification.getValue().close();
            }
          }
        }).build();
  }

  /**
   * Get a {@link HiveMetastoreClientPool} for the requested metastore URI. Useful for using the same pools across
   * different classes in the code base. Note that if a pool already exists for that metastore, the max number of
   * objects available will be unchanged, and it might be lower than requested by this method.
   *
   * @param properties {@link Properties} used to generate the pool.
   * @param metastoreURI URI of the Hive metastore. If absent, use default metastore.
   * @return a {@link HiveMetastoreClientPool}.
   * @throws IOException
   */
  public static HiveMetastoreClientPool get(final Properties properties, final Optional<String> metastoreURI)
      throws IOException {
    synchronized (HiveMetastoreClientPool.class) {
      if (poolCache == null) {
        poolCache = createPoolCache(properties);
      }
    }
    try {
      return poolCache.get(metastoreURI, new Callable<HiveMetastoreClientPool>() {
        @Override
        public HiveMetastoreClientPool call() throws Exception {
          return new HiveMetastoreClientPool(properties, metastoreURI);
        }
      });
    } catch (ExecutionException ee) {
      throw new IOException("Failed to get " + HiveMetastoreClientPool.class.getSimpleName(), ee.getCause());
    }
  }

  /**
   * Constructor for {@link HiveMetastoreClientPool}.
   * By default we will using the default eviction strategy for the client pool. Client will be evicted if the following conditions are met:
   *  * <ul>
   *  * <li>the object has been idle longer than
   *  *     {@link GenericObjectPool#getMinEvictableIdleTimeMillis()}</li>
   *  * <li>there are more than {@link GenericObjectPool#getMinIdle()} idle objects in
   *  *     the pool and the object has been idle for longer than
   *  *     {@link GenericObjectPool#getSoftMinEvictableIdleTimeMillis()} </li>
   *  * </ul>
   * @deprecated It is recommended to use the static {@link #get} method instead. Use this constructor only if you
   *             different pool configurations are required.
   */
  @Deprecated
  public HiveMetastoreClientPool(Properties properties, Optional<String> metastoreURI) {
    this.hiveRegProps = new HiveRegProps(new State(properties));
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(this.hiveRegProps.getNumThreads());
    config.setMaxIdle(this.hiveRegProps.getNumThreads());
    config.setMaxWaitMillis(this.hiveRegProps.getMaxWaitMillisBorrowingClient());

    String extraConfigTarget = properties.getProperty(POOL_HIVE_ADDITIONAL_CONFIG_TARGET, "");

    this.factory = new HiveMetaStoreClientFactory(metastoreURI);
    if (metastoreURI.isPresent() && StringUtils.isNotEmpty(extraConfigTarget)
        && metastoreURI.get().equals(extraConfigTarget)) {
      log.info("Setting additional hive config for metastore {}", extraConfigTarget);
      properties.forEach((key, value) -> {
        String configKey = key.toString();
        if (configKey.startsWith(POOL_HIVE_ADDITIONAL_CONFIG_PREFIX) && !configKey.equals(
            POOL_HIVE_ADDITIONAL_CONFIG_TARGET)) {
          log.info("Setting additional hive config {}={}", configKey.substring(POOL_HIVE_ADDITIONAL_CONFIG_PREFIX.length()),
              value.toString());
          this.factory.getHiveConf().set(configKey.substring(POOL_HIVE_ADDITIONAL_CONFIG_PREFIX.length()), value.toString());
        }
      });
    }
    this.pool = new GenericObjectPool<>(this.factory, config);
    //Set the eviction policy for the client pool
    this.pool.setEvictionPolicyClassName(properties.getProperty(POOL_EVICTION_POLICY_CLASS_NAME, DEFAULT_POOL_EVICTION_POLICY_CLASS_NAME));
    this.pool.setMinEvictableIdleTimeMillis(PropertiesUtils.getPropAsLong(properties, POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS, DEFAULT_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS));
    this.pool.setTimeBetweenEvictionRunsMillis(PropertiesUtils.getPropAsLong(properties, POOL_TIME_BETWEEN_EVICTION_MILLIS, DEFAULT_POOL_TIME_BETWEEN_EVICTION_MILLIS));
    this.hiveConf = this.factory.getHiveConf();
  }

  public void close() {
    this.pool.close();
  }

  /**
   * @return an auto returnable wrapper around a {@link IMetaStoreClient}.
   * @throws IOException
   * Note: if you must acquire multiple locks, please use {@link #safeGetClients} instead, as this call may deadlock.
   */
  public AutoReturnableObject<IMetaStoreClient> getClient() throws IOException {
    return new AutoReturnableObject<>(this.pool);
  }

  /**
   * A class wrapping multiple named {@link IMetaStoreClient}s.
   */
  public static class MultiClient implements AutoCloseable {
    private final Map<String, AutoReturnableObject<IMetaStoreClient>> clients;
    private final Closer closer;

    private MultiClient(Map<String, HiveMetastoreClientPool> namedPools) throws IOException {
      this.clients = Maps.newHashMap();
      this.closer = Closer.create();
      Map<HiveMetastoreClientPool, Integer> requiredClientsPerPool = Maps.newHashMap();
      for (Map.Entry<String, HiveMetastoreClientPool> entry : namedPools.entrySet()) {
        if (requiredClientsPerPool.containsKey(entry.getValue())) {
          requiredClientsPerPool.put(entry.getValue(), requiredClientsPerPool.get(entry.getValue()) + 1);
        } else {
          requiredClientsPerPool.put(entry.getValue(), 1);
        }
      }
      for (Map.Entry<HiveMetastoreClientPool, Integer> entry : requiredClientsPerPool.entrySet()) {
        if (entry.getKey().pool.getMaxTotal() < entry.getValue()) {
          throw new IOException(
              String.format("Not enough clients available in the pool. Required %d, max available %d.",
                  entry.getValue(), entry.getKey().pool.getMaxTotal()));
        }
      }
      for (Map.Entry<String, HiveMetastoreClientPool> entry : namedPools.entrySet()) {
        this.clients.put(entry.getKey(), this.closer.register(entry.getValue().getClient()));
      }
    }

    /**
     * Get the {@link IMetaStoreClient} with the provided name.
     * @throws IOException
     */
    public IMetaStoreClient getClient(String name) throws IOException {
      if (!this.clients.containsKey(name)) {
        throw new IOException("There is no client with name " + name);
      }
      return this.clients.get(name).get();
    }

    @Override
    public void close() throws IOException {
      this.closer.close();
    }
  }

  /**
   * A method to get multiple {@link IMetaStoreClient}s while preventing deadlocks.
   * @param namedPools A map from String to {@link HiveMetastoreClientPool}.
   * @return a {@link MultiClient} with a {@link IMetaStoreClient} for each entry in the input map. The client can
   *          be retrieved by its name in the input map.
   * @throws IOException
   */
  public static synchronized MultiClient safeGetClients(Map<String, HiveMetastoreClientPool> namedPools)
      throws IOException {
    return new MultiClient(namedPools);
  }

}
