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

package org.apache.gobblin.util.limiter;

import com.google.common.collect.ImmutableMap;
import com.linkedin.restli.client.RestClient;

import org.apache.gobblin.broker.ResourceCoordinate;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metrics.broker.MetricContextFactory;
import org.apache.gobblin.metrics.broker.MetricContextKey;
import org.apache.gobblin.metrics.broker.SubTaggedMetricContextKey;
import org.apache.gobblin.restli.SharedRestClientKey;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link org.apache.gobblin.util.limiter.broker.SharedLimiterFactory} that creates {@link RestliServiceBasedLimiter}s. It
 * automatically acquires a {@link RestClient} from the broker for restli service name {@link #RESTLI_SERVICE_NAME}.
 */
@Slf4j
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
  public SharedResourceFactoryResponse<RestliServiceBasedLimiter> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, SharedLimiterKey> config) throws NotConfiguredException {

    S scope = config.getScope();
    if (scope != scope.rootScope()) {
      return new ResourceCoordinate<>(this, config.getKey(), scope.rootScope());
    }

    String serviceIdentifier = config.getConfig().hasPath(SERVICE_IDENTIFIER_KEY) ?
        config.getConfig().getString(SERVICE_IDENTIFIER_KEY) : "UNKNOWN";
    String resourceLimited = config.getKey().getResourceLimitedPath();

    MetricContextKey metricContextKey =
        new SubTaggedMetricContextKey(RestliServiceBasedLimiter.class.getSimpleName() + "_" + resourceLimited,
        ImmutableMap.of("resourceLimited", resourceLimited));

    return new ResourceInstance<>(
        RestliServiceBasedLimiter.builder()
            .resourceLimited(resourceLimited)
            .serviceIdentifier(serviceIdentifier)
            .metricContext(broker.getSharedResource(new MetricContextFactory<S>(), metricContextKey))
            .requestSender(broker.getSharedResource(new RedirectAwareRestClientRequestSender.Factory<S>(), new SharedRestClientKey(RESTLI_SERVICE_NAME)))
            .build()
    );
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedLimiterKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
