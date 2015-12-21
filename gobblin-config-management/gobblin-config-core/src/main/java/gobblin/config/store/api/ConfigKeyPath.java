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

import gobblin.annotation.Alpha;

/**
 * The ConfigKeyPath is used to describe the relative path for a given configuration key URI to
 * the root URI of a config store ({@link ConfigStore#getStoreURI()}). For example,
 * for a configuration store with root URI hfs://namenode.grid.company.com:9000/configs/hfs_config_root/
 * and a config key URI hfs://namenode.grid.company.com:9000/configs/hfs_config_root/data/tracking/,
 * the ConfigKeyPath will be /data/tracking.

 * @author mitu
 *
 */
@Alpha
public interface ConfigKeyPath {

  /**
   * The path to the parent.
   * @throws UnsupportedOperationException if the current node is the root.
   */
  public ConfigKeyPath getParent();

  /**
   * The last component of this path. For example, for /a/b/c, it will return "c". If the current
   * path points to the root ("/"), the result is the empty string "".
   */
  public String getOwnPathName();

  /**
   * Creates a path that is a child of the current path by appending one component to the path.
   * For example, if the current path points to "/a/b", createChild("c") will return a path that
   * points to "/a/b/c" . */
  public ConfigKeyPath createChild(String childPathName);

  /**
   * The absolute configuration key path. This is joining all path components from the root using
   * "/" as a separator.
   */
  public String getAbsolutePathString();

  /** Check if the current path is the root path ("/"). */
  public boolean isRootPath();

  /** Parses the string representation of a path into a ConfigKeyPath object */
  public ConfigKeyPath fromPathString(String configKeyPath);

  /**
   * Creates a new  ConfigKeyPath object from a config key URI.
   *
   * @param  configKeyURI       the URI of the config key; it must be hosted by the specified store
   * @param  configStore        the store that can handle the above config key, i.e. it root URI
   *                            must be a prefix of the config key URI
   * @return a ConfigKeyPath rooted at the config store root URI
   * @throws IllegalArgumentException if the config store root URI is not a prefix of the config key
   *                            root URI
   */
  public ConfigKeyPath fromUri(URI configKeyURI, ConfigStore configStore);

  /**
   * Creates a new  ConfigKeyPath object from a config key URI. This is a convenience version of
   * {@link #fromUri(URI, ConfigStore)} which parses the config key URI string into a {@link URI}.
   *
   * @param  configKeyURI       the URI of the config key; it must be hosted by the specified store
   * @param  configStore        the store that can handle the above config key, i.e. it root URI
   *                            must be a prefix of the config key URI
   * @return a ConfigKeyPath rooted at the config store root URI
   * @throws IllegalArgumentException if the config store root URI is not a prefix of the config key
   *                            root URI
   */
  public ConfigKeyPath fromUriString(String configKeyURI, ConfigStore configStore);
}
