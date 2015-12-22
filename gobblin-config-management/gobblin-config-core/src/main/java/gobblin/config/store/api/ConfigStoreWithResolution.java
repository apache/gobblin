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
import java.util.List;

import com.typesafe.config.Config;


/**
 * ConfigStoreWithResolution is used to indicate the {@ConfigStore} which support the configuration
 * resolution by following the imported path.
 * @author mitu
 *
 */
public interface ConfigStoreWithResolution extends ConfigStore {

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly and indirectly specified configuration in com.typesafe.config.Config format for input uri 
   *  against input configuration version
   */
  public Config getResolvedConfig(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly and indirectly imported URIs, including configKeyUris imported by ancestors 
   *  for input uri against input configuration version
   *  the earlier URI in the List will have higher priority when resolving configuration conflict
   *  
   */
  public List<URI> getImportsRecursively(URI uri, String version) throws VersionDoesNotExistException;

}
