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
package gobblin.config.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import gobblin.config.client.api.VersionStabilityPolicy;

/**
 * Caches {@link ConfigClient}s for every {@link VersionStabilityPolicy}.
 */
public class ConfigClientCache {

  private static final Cache<VersionStabilityPolicy, ConfigClient> CONFIG_CLIENTS_CACHE = CacheBuilder.newBuilder()
      .maximumSize(VersionStabilityPolicy.values().length).build();

  public static ConfigClient getClient(final VersionStabilityPolicy policy) {
    try {
      return CONFIG_CLIENTS_CACHE.get(policy, new Callable<ConfigClient>() {
        @Override
        public ConfigClient call() throws Exception {
          return ConfigClient.createConfigClient(policy);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to get Config client", e);
    }
  }
}
