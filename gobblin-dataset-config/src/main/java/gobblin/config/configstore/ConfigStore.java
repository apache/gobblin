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

package gobblin.config.configstore;

import java.io.Serializable;
import java.net.URI;
import java.util.Collection;

import com.typesafe.config.Config;

/**
 * A ConfigStore is the configuration store per scheme.
 * @author mitu
 *
 */
public interface ConfigStore extends Serializable{

  /**
   * @return the configuration store scheme, example DAI-ETL, DALI or Espresso
   */
  public String getScheme();
  
  public String getCurrentVersion();
  
  public URI getParent(URI uri);
  
  public Collection<URI> getChildren(URI uri);
  
  public Collection<URI> getImports(URI uri);
  
  public Config getOwnConfig(URI uri);
  
}
