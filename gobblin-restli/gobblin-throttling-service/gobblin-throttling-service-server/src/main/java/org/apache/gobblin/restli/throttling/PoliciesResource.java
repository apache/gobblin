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

import java.util.Map;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;

import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.SubTaggedMetricContextKey;
import gobblin.util.limiter.broker.SharedLimiterKey;

import javax.inject.Inject;
import javax.inject.Named;

import static gobblin.restli.throttling.LimiterServerResource.*;


/**
 * A Rest.li endpoint for getting the {@link ThrottlingPolicy} associated with a resource id.
 */
@RestLiCollection(name = "policies", namespace = "gobblin.restli.throttling")
public class PoliciesResource extends CollectionResourceTemplate<String, Policy> {

  @Inject
  @Named(BROKER_INJECT_NAME)
  SharedResourcesBroker broker;

  @Override
  public Policy get(String resourceId) {
    try {
      ThrottlingPolicy throttlingPolicy =
          (ThrottlingPolicy) this.broker.getSharedResource(new ThrottlingPolicyFactory(), new SharedLimiterKey(resourceId));

      Policy restliPolicy = new Policy();
      restliPolicy.setPolicyName(throttlingPolicy.getClass().getSimpleName());
      restliPolicy.setResource(resourceId);
      restliPolicy.setParameters(new StringMap(throttlingPolicy.getParameters()));
      restliPolicy.setPolicyDetails(throttlingPolicy.getDescription());

      MetricContext resourceContext = (MetricContext) broker.getSharedResource(new MetricContextFactory(),
          new SubTaggedMetricContextKey(resourceId, ImmutableMap.of(RESOURCE_ID_TAG, resourceId)));
      StringMap metrics = new StringMap();
      for (Map.Entry<String, Meter> meter : resourceContext.getMeters().entrySet()) {
        metrics.put(meter.getKey(), Double.toString(meter.getValue().getOneMinuteRate()));
      }
      restliPolicy.setMetrics(metrics);

      return restliPolicy;
    } catch (NotConfiguredException nce) {
      throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Policy not found for resource " + resourceId);
    }
  }
}
