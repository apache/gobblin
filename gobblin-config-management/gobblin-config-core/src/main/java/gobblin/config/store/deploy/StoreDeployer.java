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
package gobblin.config.store.deploy;

import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreFactory;

import java.net.URI;
import java.util.ServiceLoader;

import lombok.extern.slf4j.Slf4j;


/**
 * A tool to deploy configs provided by a {@link DeployableConfigSource} to a {@link ConfigStore}. The deployment
 * semantics are defined the {@link ConfigStore} themselves. A {@link ConfigStore} must implement {@link Deployable} for
 * the {@link StoreDeployer} to deploy on it. If the {@link ConfigStore} for <code>storeUri</code> does not implement
 * {@link Deployable}, the deployment will be a no-op
 */
@Slf4j
public class StoreDeployer {

  /**
   * Deploy configs in <code>classpathStoreRoot</code> to <code>storeUri</code>
   *
   * @param storeUri to which confgs are deployed
   * @param confgSource The source that provides deployable configs.
   * @param version to be used for this deployment
   *
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void deploy(URI storeUri, DeployableConfigSource confgSource, String version) throws Exception {

    ServiceLoader<ConfigStoreFactory> loader = ServiceLoader.load(ConfigStoreFactory.class);

    for (ConfigStoreFactory storeFactory : loader) {

      log.info("Found ConfigStore with scheme : " + storeFactory.getScheme());

      if (storeUri.getScheme().equals(storeFactory.getScheme())) {

        log.info("Using ConfigStore with scheme : " + storeFactory.getScheme());

        ConfigStore configStore = storeFactory.createConfigStore(storeUri);

        if (configStore instanceof Deployable<?>) {

          ((Deployable) configStore).deploy(new FsDeploymentConfig(confgSource, version));

        } else {
          log.error(String.format("Deployment failed. The store %s does not implement %s", storeFactory.getClass(),
              Deployable.class.getName()));
        }
      }
    }
  }
}
