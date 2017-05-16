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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.GetMode;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.annotation.Alpha;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link ThrottlingPolicy} based on a QPS (queries per second). It internally uses a {@link DynamicTokenBucket}.
 */
@Alpha
@Slf4j
public class QPSPolicy implements ThrottlingPolicy {

  public static final String FACTORY_ALIAS = "qps";

  /**
   * The qps the policy should enforce.
   */
  public static final String QPS = "qps";
  /**
   * The time the policy should spend trying to satisfy the full permit request.
   */
  public static final String FULL_REQUEST_TIMEOUT_MILLIS = "fullRequestTimeoutMillis";
  public static final long DEFAULT_FULL_REQUEST_TIMEOUT = 50;
  /**
   * Maximum number of tokens (in milliseconds) that can be accumulated when underutilized.
   */
  public static final String MAX_BUCKET_SIZE_MILLIS = "maxBucketSizeMillis";
  public static final long DEFAULT_MAX_BUCKET_SIZE = 10000;

  @Getter
  private final long qps;
  @VisibleForTesting
  @Getter
  private final DynamicTokenBucket tokenBucket;

  @Alias(FACTORY_ALIAS)
  public static class Factory implements ThrottlingPolicyFactory.SpecificPolicyFactory {
    @Override
    public ThrottlingPolicy createPolicy(SharedLimiterKey key, SharedResourcesBroker<ThrottlingServerScopes> broker, Config config) {
      return new QPSPolicy(config);
    }
  }

  public QPSPolicy(Config config) {
    Preconditions.checkArgument(config.hasPath(QPS), "QPS required.");

    this.qps = config.getLong(QPS);
    long fullRequestTimeoutMillis = config.hasPath(FULL_REQUEST_TIMEOUT_MILLIS)
        ? config.getLong(FULL_REQUEST_TIMEOUT_MILLIS) : DEFAULT_FULL_REQUEST_TIMEOUT;
    long maxBucketSizeMillis = config.hasPath(MAX_BUCKET_SIZE_MILLIS)
        ? config.getLong(MAX_BUCKET_SIZE_MILLIS) : DEFAULT_MAX_BUCKET_SIZE;
    this.tokenBucket = new DynamicTokenBucket(qps, fullRequestTimeoutMillis, maxBucketSizeMillis);
  }

  @Override
  public PermitAllocation computePermitAllocation(PermitRequest request) {
    long permitsRequested = request.getPermits();
    Long minPermits = request.getMinPermits(GetMode.NULL);
    if (minPermits == null) {
      minPermits = permitsRequested;
    }

    long permitsGranted = this.tokenBucket.getPermits(permitsRequested, minPermits, LimiterServerResource.TIMEOUT_MILLIS);

    PermitAllocation allocation = new PermitAllocation();
    allocation.setPermits(permitsGranted);
    allocation.setExpiration(Long.MAX_VALUE);
    if (permitsGranted <= 0) {
      allocation.setMinRetryDelayMillis(LimiterServerResource.TIMEOUT_MILLIS);
    }
    return allocation;
  }

  @Override
  public Map<String, String> getParameters() {
    return ImmutableMap.of("qps", Long.toString(this.qps));
  }

  @Override
  public String getDescription() {
    return "QPS based policy. QPS: " + this.qps;
  }
}
