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

package gobblin.restli.throttling;

import java.net.URI;
import java.net.URISyntaxException;

import com.typesafe.config.Config;

import gobblin.broker.TTLResourceEntry;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.util.ConfigUtils;
import gobblin.util.limiter.broker.SharedLimiterKey;


/**
 * A {@link gobblin.restli.throttling.ThrottlingPolicyFactory.SpecificPolicyFactory} that looks up policies using a
 * {@link ConfigClient}.
 *
 * The config store prefix should be specified at key {@link #CONFIG_KEY_URI_PREFIX_KEY}
 * (e.g. "simple-hdfs://myCluster.com/myConfigStore"), and keys in the config store should be prepended with
 * {@link #THROTTLING_CONFIG_PREFIX}.
 */
public class ConfigClientBasedPolicyFactory implements ThrottlingPolicyFactory.SpecificPolicyFactory {

  private static final long CONFIG_CLIENT_TTL_IN_MILLIS = 60000;
  private static TTLResourceEntry<ConfigClient> CONFIG_CLIENT;

  public static final String CONFIG_KEY_URI_PREFIX_KEY = "configKeyUriPrefix";

  public static final String THROTTLING_CONFIG_PREFIX = "globalThrottling";
  public static final String POLICY_KEY = THROTTLING_CONFIG_PREFIX + "." + ThrottlingPolicyFactory.POLICY_KEY;

  @Override
  public ThrottlingPolicy createPolicy(SharedLimiterKey key, SharedResourcesBroker<ThrottlingServerScopes> broker, Config config) {

    try {
      Config resourceConfig =
          getConfigClient().getConfig(new URI(config.getString(CONFIG_KEY_URI_PREFIX_KEY) + key.getResourceLimitedPath()));

      ThrottlingPolicyFactory.SpecificPolicyFactory factory =
          ThrottlingPolicyFactory.POLICY_CLASS_RESOLVER.resolveClass(resourceConfig.getString(POLICY_KEY)).newInstance();
      return factory.createPolicy(key, broker, ConfigUtils.getConfigOrEmpty(resourceConfig, THROTTLING_CONFIG_PREFIX));
    } catch (URISyntaxException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException |
        ReflectiveOperationException exc) {
      throw new RuntimeException(exc);
    }
  }

  private synchronized static ConfigClient getConfigClient() {
    if (CONFIG_CLIENT == null || !CONFIG_CLIENT.isValid()) {
      CONFIG_CLIENT = new TTLResourceEntry<>(ConfigClient.createConfigClient(VersionStabilityPolicy.READ_FRESHEST),
          CONFIG_CLIENT_TTL_IN_MILLIS, false);
    }
    return CONFIG_CLIENT.getResource();
  }
}
