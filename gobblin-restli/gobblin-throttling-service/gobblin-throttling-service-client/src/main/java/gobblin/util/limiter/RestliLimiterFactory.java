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

package gobblin.util.limiter;

import com.linkedin.restli.client.RestClient;

import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.restli.SharedRestClientFactory;
import gobblin.restli.SharedRestClientKey;
import gobblin.util.limiter.broker.SharedLimiterKey;


/**
 * A {@link gobblin.util.limiter.broker.SharedLimiterFactory} that creates {@link RestliServiceBasedLimiter}s. It
 * automatically acquires a {@link RestClient} from the broker for restli service name {@link #RESTLI_SERVICE_NAME}.
 */
public class RestliLimiterFactory<S extends ScopeType<S>>
    implements SharedResourceFactory<RestliServiceBasedLimiter, SharedLimiterKey, S> {

  public static final String FACTORY_NAME = "limiter.restli";
  public static final String RESTLI_SERVICE_NAME = "throttling";
  public static final String SERVICE_IDENTIFIER_KEY = "serviceId";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public ResourceInstance<RestliServiceBasedLimiter> createResource(SharedResourcesBroker broker,
      ScopedConfigView<?, SharedLimiterKey> config) throws NotConfiguredException {

    String serviceIdentifier = config.getConfig().hasPath(SERVICE_IDENTIFIER_KEY) ?
        config.getConfig().getString(SERVICE_IDENTIFIER_KEY) : "UNKNOWN";

    RestClient restClient = (RestClient)
        broker.getSharedResource(new SharedRestClientFactory(), new SharedRestClientKey(RESTLI_SERVICE_NAME));

    return new ResourceInstance<>(new RestliServiceBasedLimiter(restClient, config.getKey().getResourceLimited(), serviceIdentifier));
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedLimiterKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
