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

package gobblin.config.client;

import gobblin.config.store.api.ConfigStoreFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;


public class ConfigStoreFactoryRegister {
  private static final Logger LOG = Logger.getLogger(ConfigStoreFactoryRegister.class);

  //key is the configStore scheme name, value is the ConfigStoreFactory
  @SuppressWarnings("rawtypes")
  private final Map<String, ConfigStoreFactory> configStoreFactoryMap = new HashMap<>();

  @SuppressWarnings("rawtypes")
  public ConfigStoreFactoryRegister() {
    ServiceLoader<ConfigStoreFactory> loader = ServiceLoader.load(ConfigStoreFactory.class);
    for (ConfigStoreFactory f : loader) {
      this.configStoreFactoryMap.put(f.getScheme(), f);
      LOG.info("Created the config store factory with scheme name " + f.getScheme());
    }
  }

  @SuppressWarnings("rawtypes")
  public ConfigStoreFactory getConfigStoreFactory(String scheme) {
    return this.configStoreFactoryMap.get(scheme);
  }

  @SuppressWarnings("rawtypes")
  public void register(ConfigStoreFactory factory) {
    this.configStoreFactoryMap.put(factory.getScheme(), factory);
    LOG.info("Registered the config store factory with scheme name " + factory.getScheme());
  }
}
