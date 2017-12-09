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

package org.apache.gobblin.config.store.api;

import java.net.URI;

import org.apache.gobblin.annotation.Alpha;


/**
 * ConfigStoreFactory is used to created {@link ConfigStore}s. Each ConfigStoreFactory is responsible for
 * instantiating {@link ConfigStore}s that can handle specific config key URI scheme. Typically those
 * {@link ConfigStore}s correspond to different physical instances and are differentiated by the
 * authority in the URI. The ConfigStoreFactory will typically also define a default
 * {@link ConfigStore} which is to be used if no authority is specified in the config key URI.
 *
 * @author mitu
 *
 * @param <T> The java class of the {@link ConfigStore} implementation(s) supported by this factory
 */
@Alpha
public interface ConfigStoreFactory<T extends ConfigStore> {

  /**
   * @return the URI scheme for which this configuration store factory is responsible.
   * All the configuration store created by this configuration factory will share the same scheme
   * name.
   */
  public String getScheme();

  /**
   * Obtains the {@link ConfigStore} to handle a specific config key.
   *
   * @param  configKey       The URI of the config key that needs to be accessed.
   * @return {@link ConfigStore} which can handle the specified config key. If the config key URI is
   *         missing the authority part, the factory may choose a default store if available or throw
   *         a ConfigStoreCreationException
   * @throws ConfigStoreCreationException if the URI cannot be mapped to a config store
   * @throws IllegalArgumentException if the scheme of the config key URI does not match
   *         the value of {@link #getScheme()}.
   */
  public T createConfigStore(URI configKey) throws ConfigStoreCreationException;

}
