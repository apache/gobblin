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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.annotation.Alpha;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.limiter.broker.SharedLimiterKey;


/**
 * A {@link ThrottlingPolicy} that does no throttling and eagerly returns a large amount of permits.
 */
@Alpha
public class NoopPolicy implements ThrottlingPolicy {

  public static final String FACTORY_ALIAS = "noop";

  @Alias(FACTORY_ALIAS)
  public static class Factory implements ThrottlingPolicyFactory.SpecificPolicyFactory {
    @Override
    public ThrottlingPolicy createPolicy(SharedLimiterKey key,
        SharedResourcesBroker<ThrottlingServerScopes> broker, Config config) {
      return new NoopPolicy();
    }
  }

  @Override
  public PermitAllocation computePermitAllocation(PermitRequest request) {

    PermitAllocation allocation = new PermitAllocation();
    // For overflow safety, don't return max long
    allocation.setPermits(Math.max(Long.MAX_VALUE / 100, request.getPermits()));
    allocation.setExpiration(Long.MAX_VALUE);

    return allocation;
  }

  @Override
  public Map<String, String> getParameters() {
    return ImmutableMap.of();
  }

  @Override
  public String getDescription() {
    return "Noop policy. Infinite permits available.";
  }
}
