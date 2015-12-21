/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.store.api;

import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

import gobblin.annotation.Alpha;

/**
 * ConfigStoreWithBatchFetches indicate this {@link ConfigStore} support (efficient) fetching of
 * batches of config keys with the same config version. For {@link ConfigStore} implementations that
 * implement this interface, the config client library will delegate the batch fetches to the
 * store instead of doing that itself. A typical use case for this interface is if the {@link ConfigStore}
 * supports an RPC call which can fetch multiple config objects with a single call.
 *
 * @author mitu
 *
 */
@Alpha
public interface ConfigStoreWithBatchFetches extends ConfigStore {
  /**
   *
   * @param  configKeys     the config keys whose {@link Config} objects are to be fetched
   * @param  version        the configuration version of the config keys
   * @return the Map from the config key to its the {@com.typesafe.config.Config} object
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Map<ConfigKeyPath, Config> getOwnConfigs(Collection<ConfigKeyPath> configKeys, String version)
      throws VersionDoesNotExistException;
}
