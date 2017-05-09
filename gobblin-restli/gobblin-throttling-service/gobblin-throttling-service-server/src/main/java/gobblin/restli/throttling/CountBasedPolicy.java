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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.limiter.CountBasedLimiter;
import gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.Getter;


/**
 * A count based {@link ThrottlingPolicy} used for testing. Not recommended as an actual policy.
 */
public class CountBasedPolicy implements ThrottlingPolicy {

  public static final String COUNT_KEY = "count";
  public static final String FACTORY_ALIAS = "countForTesting";

  @Alias(FACTORY_ALIAS)
  public static class Factory implements ThrottlingPolicyFactory.SpecificPolicyFactory {
    @Override
    public ThrottlingPolicy createPolicy(SharedLimiterKey key, SharedResourcesBroker<ThrottlingServerScopes> broker, Config config) {
      Preconditions.checkArgument(config.hasPath(COUNT_KEY), "Missing key " + COUNT_KEY);
      return new CountBasedPolicy(config.getLong(COUNT_KEY));
    }
  }

  private final CountBasedLimiter limiter;
  @Getter
  private final long count;

  public CountBasedPolicy(long count) {
    this.count = count;
    this.limiter = new CountBasedLimiter(count);
  }

  @Override
  public PermitAllocation computePermitAllocation(PermitRequest request) {
    long permits = request.getPermits();
    long allocated = 0;

    try {
      if (limiter.acquirePermits(permits) != null) {
        allocated = permits;
      } else {
        throw new RestLiServiceException(HttpStatus.S_403_FORBIDDEN, "Not enough permits.");
      }
    } catch (InterruptedException ie) {
      // return no permits
    }

    PermitAllocation allocation = new PermitAllocation();
    allocation.setPermits(allocated);
    allocation.setExpiration(Long.MAX_VALUE);
    if (allocated <= 0) {
      allocation.setMinRetryDelayMillis(60000);
    }
    return allocation;
  }

  @Override
  public Map<String, String> getParameters() {
    return ImmutableMap.of("maxPermits", Long.toString(this.count));
  }

  @Override
  public String getDescription() {
    return "Count based policy. Max permits: " + this.count;
  }
}
