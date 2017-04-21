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

import com.typesafe.config.Config;

import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.ClassAliasResolver;
import gobblin.util.limiter.broker.SharedLimiterKey;


/**
 * A {@link SharedResourceFactory} to create {@link ThrottlingPolicy}s.
 */
public class ThrottlingPolicyFactory implements SharedResourceFactory<ThrottlingPolicy, SharedLimiterKey, ThrottlingServerScopes> {

  public static final String NAME = "throttlingPolicy";

  public static final String POLICY_KEY = "policy";
  public static final String FAIL_ON_UNKNOWN_RESOURCE_ID = "faiOnUnknownResourceId";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SharedResourceFactoryResponse<ThrottlingPolicy> createResource(SharedResourcesBroker<ThrottlingServerScopes> broker,
      ScopedConfigView<ThrottlingServerScopes, SharedLimiterKey> configView) throws NotConfiguredException {

    Config config = configView.getConfig();

    if (!config.hasPath(POLICY_KEY)) {
      if (config.hasPath(FAIL_ON_UNKNOWN_RESOURCE_ID) && config.getBoolean(FAIL_ON_UNKNOWN_RESOURCE_ID)) {
        throw new NotConfiguredException("Missing key " + POLICY_KEY);
      } else {
        return new ResourceInstance<ThrottlingPolicy>(new NoopPolicy());
      }
    }
    ClassAliasResolver<SpecificPolicyFactory> resolver = new ClassAliasResolver<>(SpecificPolicyFactory.class);

    try {
      SpecificPolicyFactory factory = resolver.resolveClass(config.getString(POLICY_KEY)).newInstance();
      return new ResourceInstance<>(factory.createPolicy(broker, config));
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException(roe);
    }
  }

  @Override
  public ThrottlingServerScopes getAutoScope(SharedResourcesBroker<ThrottlingServerScopes> broker,
      ConfigView<ThrottlingServerScopes, SharedLimiterKey> config) {
    return ThrottlingServerScopes.GLOBAL;
  }

  public interface SpecificPolicyFactory {
    ThrottlingPolicy createPolicy(SharedResourcesBroker<ThrottlingServerScopes> broker, Config config);
  }

}
