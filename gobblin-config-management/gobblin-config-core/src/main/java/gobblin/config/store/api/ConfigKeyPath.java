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

package gobblin.config.store.api;

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
}
