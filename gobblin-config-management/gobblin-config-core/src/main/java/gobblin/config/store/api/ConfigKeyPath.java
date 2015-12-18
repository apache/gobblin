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

/**
 * The ConfigKeyPath is used to describe the relative path for a given configuration key URI to 
 * configuration store
 * @author mitu
 *
 */
public interface ConfigKeyPath {

  public ConfigKeyPath getParent();
  
  public String getOwnPathName();
  
  public ConfigKeyPath createChild(String childPathName);
  
  /**
   * 
   * @return the absoluate configuration key path 
   * for example 
   * configuration store with root URI: etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest
   * getAbsolutePathString will return /datasets/a1/a2 where datasets is a subdirectory under /user/mitu/HdfsBasedConfigTest
   */
  public String getAbsolutePathString();
  
  public boolean isRootPath();
}
