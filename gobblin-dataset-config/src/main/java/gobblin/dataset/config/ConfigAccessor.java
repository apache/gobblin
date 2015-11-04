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
 * A ConfigAccessor is the bundle of {@ConfigStore} and String representation of the version for that {@ConfigStore}.
 * The client should use this object as the proxy for {@ConfigStore} because it hides the version information
 * @author mitu
 *
 */

public class ConfigAccessor {

  private final String configVersion;
  private final ConfigStore configStore;
  
  public ConfigAccessor (String version, ConfigStore store) {
    this.configStore = store;
    this.configVersion = version;
  }
  
  /**
   * @param urn - the urn which need to get the configuration. This urn could be either dataset or tag.
   * @return the resolved Config of input urn in configStore for the configVersion
   */
  public Config getConfig(String urn){
    return configStore.getConfig(urn, configVersion);
  }
  
  /**
   * @param tag - the tag which need to be checked for
   * @return the Map whose key is the dataset id, in configStore for the configVersion, which has input tag associated, 
   * not include descendant datasets; value is resolved config, for the corresponding key. 
   *  If input is dataset, result is empty map as dataset is not taggable
   */
  public Map<String, Config> getTaggedConfig(String tag){
    return this.configStore.getTaggedConfig(tag, configVersion);
  }
  
  /**
   * @param tag - the tag which need to be checked for. If the input tag is dataset, empty list will be returned
   * @return all the tags, in configStore for the configVersion, associated with input tag directly or in-directly 
   */
  public List<String> getAssociatedTags(String tag){
    return this.configStore.getAssociatedTags(tag, configVersion);
  }
}
