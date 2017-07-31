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

package org.apache.gobblin.runtime.locks;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.Config;

/**
 * A default base implementation for {@link JobLockFactoryManager} implementations. It maintains a
 * cache from a factory configuration to factory instance.
 */
public abstract class AbstractJobLockFactoryManager<T extends JobLock, F extends JobLockFactory<T>>
       implements JobLockFactoryManager<T, F> {
  private final Cache<Config, F> _factoryCache;

  protected AbstractJobLockFactoryManager(Cache<Config, F> factoryCache) {
    _factoryCache = factoryCache;
  }

  public AbstractJobLockFactoryManager() {
    this(CacheBuilder.newBuilder().<Config, F>build());
  }

  /** Extracts the factory-specific configuration from the system configuration. The extracted
   * object should uniquely identify the factory instance. */
  protected abstract Config getFactoryConfig(Config sysConfig);

  /** Creates a new factory instance using the specified factory and system configs. */
  protected abstract F createFactoryInstance(final Optional<Logger> log,
                                             final Config sysConfig,
                                             final Config factoryConfig);

  /** {@inheritDoc} */
  @Override
  public F getJobLockFactory(final Config sysConfig, final Optional<Logger> log) {
    final Config factoryConfig = getFactoryConfig(sysConfig);
    try {
      return _factoryCache.get(factoryConfig, new Callable<F>() {
        @Override public F call() throws Exception {
          return createFactoryInstance(log, sysConfig, factoryConfig);
        }
      }) ;
    } catch (ExecutionException e) {
      throw new RuntimeException("Unable to create a job lock factory: " + e, e);
    }
  }

}
