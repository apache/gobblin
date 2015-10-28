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

package gobblin.dataset.config;

import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;

/**
 * A ConfigStore is the configuration store per scheme.
 * @author mitu
 *
 */
public interface ConfigStore {

  /**
   * @return the latest configuration version in this ConfigStore
   */
  public String getLatestVersion();

  /**
   * @return the configuration store scheme, example DAI-ETL, DALI or Espresso
   */
  public String getScheme();
  
  /**
   * Loaded the latest configuration version. 
   */
  public void loadConfigs();
  
  /**
   * Loaded the specified configuration version.
   * @param version ,the configuration version to be loaded
   */
  public void loadConfigs(String version);
  
  /**
   * @param urn input urn
   * @return the Config of input urn
   */
  public Config getConfig(String urn);
  
  /**
   * @param urn input urn
   * @param version, specified configuration version
   * @return the Config of input urn for the specified configuration version
   */
  public Config getConfig(String urn, String version);
  
  /**
   * @param tag, input tag
   * @return the Map whose key is the dataset which has input tag associated, not include descendant datasets,
   *  value is resolved config for the corresponding key. If input is dataset, result is empty as dataset is not taggable
   */
  public Map<String, Config> getTaggedConfig(String tag);
  
  /**
   * @param tag, input tag
   * @param version, specified configuration version
   * @return the Map whose key is the dataset, against specifed configuration version, which has input tag associated, not include descendant datasets,
   *  value is resolved config ,against specifed configuration version, for the corresponding key. 
   *  If input is dataset, result is empty as dataset is not taggable
   */
  public Map<String, Config> getTaggedConfig(String tag, String version);
  
  /**
   * @param tag input tag
   * @return all the tags associated with input tag directly or in-directly 
   */
  public List<String> getAssociatedTags(String tag);
  
  
  /**
   * @param tag input tag
   * @param version, specified configuration version
   * @return all the tags ,against specifed configuration version, associated with input tag directly or in-directly 
   */
  public List<String> getAssociatedTags(String tag, String version);
  
}
