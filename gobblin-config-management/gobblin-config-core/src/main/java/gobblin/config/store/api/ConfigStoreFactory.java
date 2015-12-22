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


/**
 * ConfigStoreFactory is used to created {@ConfigStore}s. 
 * @author mitu
 *
 * @param <T> The java class which extends {@ConfigStore}
 */
public interface ConfigStoreFactory<T extends ConfigStore> {

  /**
   * @return the scheme of this configuration store factory. All the configuration store created by 
   * this configuration factory will share the same scheme name.
   */
  public String getScheme();

  /**
   * @param uri - The specified URI in the configuration store
   * @return {@ConfigStore} which is created from the input URI
   */
  public T createConfigStore(URI uri) throws ConfigStoreCreationException;

}
