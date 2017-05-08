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

import com.google.common.collect.ImmutableMap;
import com.linkedin.restli.client.RestClient;

import gobblin.broker.ResourceCoordinate;
import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.MetricContextKey;
import gobblin.metrics.broker.SubTaggedMetricContextKey;
import gobblin.restli.SharedRestClientKey;
import gobblin.util.ConfigUtils;
import gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link gobblin.util.limiter.broker.SharedLimiterFactory} that creates {@link RestliServiceBasedLimiter}s. It
 * automatically acquires a {@link RestClient} from the broker for restli service name {@link #RESTLI_SERVICE_NAME}.
 */
@Slf4j
public class RestliLimiterFactory<S extends ScopeType<S>>
    implements SharedResourceFactory<Limiter, SharedLimiterKey, S> {

  public static final String FACTORY_NAME = "limiter.restli";
  public static final String RESTLI_SERVICE_NAME = "throttling";
  public static final String SERVICE_IDENTIFIER_KEY = "serviceId";

  public static final String ALLOW_NOOP_LIMITER_IF_URI_MISSING_KEY = "allowNoopLimiterIfUriMissing";
  public static final boolean DEFAULT_ALLOW_NOOP_LIMITER_IF_URI_MISSING = true;

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<Limiter> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, SharedLimiterKey> config) throws NotConfiguredException {

    S scope = config.getScope();
    if (scope != scope.rootScope()) {
      return new ResourceCoordinate<>(this, config.getKey(), scope.rootScope());
    }

    String serviceIdentifier = config.getConfig().hasPath(SERVICE_IDENTIFIER_KEY) ?
        config.getConfig().getString(SERVICE_IDENTIFIER_KEY) : "UNKNOWN";
    String resourceLimited = config.getKey().getResourceLimited();

    MetricContextKey metricContextKey =
        new SubTaggedMetricContextKey(RestliServiceBasedLimiter.class.getSimpleName() + "_" + resourceLimited,
        ImmutableMap.of("resourceLimited", resourceLimited));

    RequestSender requestSender;
    try {
      requestSender = broker.getSharedResource(new RedirectAwareRestClientRequestSender.Factory<S>(),
          new SharedRestClientKey(RESTLI_SERVICE_NAME));
    } catch (NotConfiguredException nce) {
      if (ConfigUtils.getBoolean(config.getConfig(), ALLOW_NOOP_LIMITER_IF_URI_MISSING_KEY, DEFAULT_ALLOW_NOOP_LIMITER_IF_URI_MISSING)) {
        log.info("Using Noop limiter as no throttling server uri was found.");
        return new ResourceInstance<>(new NoopLimiter());
      } else {
        throw nce;
      }
    }

    return new ResourceInstance<>(
        RestliServiceBasedLimiter.builder()
            .resourceLimited(resourceLimited)
            .serviceIdentifier(serviceIdentifier)
            .metricContext(broker.getSharedResource(new MetricContextFactory<S>(), metricContextKey))
            .requestSender(requestSender)
            .build()
    );
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedLimiterKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
