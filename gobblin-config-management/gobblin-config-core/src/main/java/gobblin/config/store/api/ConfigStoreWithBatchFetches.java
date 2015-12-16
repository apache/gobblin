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

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

/**
 * ConfigStoreWithBatchFetches indicate this {@ConfigStore} support batch fetching for a collection of configUris with
 * specified configuration store version
 * @author mitu
 *
 */
public interface ConfigStoreWithBatchFetches extends ConfigStore {
  /**
   * 
   * @param configUris - the collection of configUris, all the Uris are relative to this configuration store
   * @param version - configuration store version
   * @return the Map whose key is the input configUris, the value is the {@com.typesafe.config.Config} format of 
   *  own configuration for corresponding key
   */
  public Map<URI, Config> getOwnConfigs(Collection<URI> configUris, String version) throws VersionDoesNotExistException;
}
