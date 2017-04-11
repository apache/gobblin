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

package gobblin.hive;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.util.AutoReturnableObject;

import lombok.Getter;


/**
 * A pool of {@link IMetaStoreClient} for querying the Hive metastore.
 */
public class HiveMetastoreClientPool {

  private final GenericObjectPool<IMetaStoreClient> pool;
  private final HiveMetaStoreClientFactory factory;
  @Getter
  private final HiveConf hiveConf;
  @Getter
  private final HiveRegProps hiveRegProps;

  private static final long DEFAULT_POOL_CACHE_TTL_MINUTES = 30;
  private static final Cache<Optional<String>, HiveMetastoreClientPool> poolCache =
      CacheBuilder.newBuilder()
          .expireAfterAccess(DEFAULT_POOL_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
          .removalListener(new RemovalListener<Optional<String>, HiveMetastoreClientPool>() {
        @Override
        public void onRemoval(RemovalNotification<Optional<String>, HiveMetastoreClientPool> notification) {
          if (notification.getValue() != null) {
            notification.getValue().close();
          }
        }
      }).build();

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
   * @deprecated It is recommended to use the static {@link #get} method instead. Use this constructor only if you
   *             different pool configurations are required.
   */
  @Deprecated
  public HiveMetastoreClientPool(Properties properties, Optional<String> metastoreURI) {
    this.hiveRegProps = new HiveRegProps(new State(properties));
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(this.hiveRegProps.getNumThreads());
    config.setMaxIdle(this.hiveRegProps.getNumThreads());

    this.factory = new HiveMetaStoreClientFactory(metastoreURI);
    this.pool = new GenericObjectPool<>(this.factory, config);
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
