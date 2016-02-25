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

package gobblin.hive;

import lombok.Getter;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import gobblin.configuration.State;
import gobblin.util.AutoReturnableObject;


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

  private static final Cache<Optional<String>, HiveMetastoreClientPool> poolCache =
      CacheBuilder.newBuilder().removalListener(new RemovalListener<Optional<String>, HiveMetastoreClientPool>() {
        @Override public void onRemoval(RemovalNotification<Optional<String>, HiveMetastoreClientPool> notification) {
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
        @Override public HiveMetastoreClientPool call() throws Exception {
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
  public HiveMetastoreClientPool(Properties properties, Optional<String> metastoreURI) throws IOException {
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
   */
  public AutoReturnableObject<IMetaStoreClient> getClient() throws IOException {
    return new AutoReturnableObject<>(this.pool);
  }

}
