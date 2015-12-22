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
import java.util.List;

import com.typesafe.config.Config;


/**
 * The ConfigStore interface used to describe a configuration store.
 * @author mitu
 *
 */
public interface ConfigStore {

  /**
   * @return the current version for that configuration store.
   */
  public String getCurrentVersion();

  /**
   * @return the configuration store root URI
   */
  public URI getStoreURI();

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the direct children URIs for input uri against input configuration version
   */
  public Collection<URI> getChildren(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly imported URIs for input uri against input configuration version
   *  the earlier URI in the List will have higher priority when resolving configuration conflict
   */
  public List<URI> getOwnImports(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly specified configuration in com.typesafe.config.Config format for input uri 
   *  against input configuration version
   */
  public Config getOwnConfig(URI uri, String version) throws VersionDoesNotExistException;
}
