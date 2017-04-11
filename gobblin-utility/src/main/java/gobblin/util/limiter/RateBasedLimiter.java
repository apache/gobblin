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

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;

import lombok.Getter;


/**
 * An implementation of {@link Limiter} that limits the rate of some events. This implementation uses
 * Guava's {@link RateLimiter}.
 *
 * <p>
 *   {@link #acquirePermits(long)} is blocking and will always return {@link true} after the permits
 *   are successfully acquired (probably after being blocked for some amount of time). Permit refills
 *   are not supported in this implementation. Also {@link #acquirePermits(long)} only accepts input
 *   arguments that can be safely casted to an integer bounded by {@link Integer#MAX_VALUE}.
 * </p>
 *
 * @author Yinan Li
 */
public class RateBasedLimiter extends NonRefillableLimiter {

  @Alias(value = "qps")
  public static class Factory implements LimiterFactory {
    public static final String QPS_KEY = "qps";

    @Override
    public Limiter buildLimiter(Config config) {
      if (!config.hasPath(QPS_KEY)) {
        throw new RuntimeException("Missing key " + QPS_KEY);
      }
      return new RateBasedLimiter(config.getLong(QPS_KEY));
    }
  }

  private final RateLimiter rateLimiter;
  @Getter
  private double rateLimitPerSecond;

  public RateBasedLimiter(double rateLimit) {
    this(rateLimit, TimeUnit.SECONDS);
  }

  public RateBasedLimiter(double rateLimit, TimeUnit timeUnit) {
    this.rateLimitPerSecond = convertRate(rateLimit, timeUnit, TimeUnit.SECONDS);
    this.rateLimiter = RateLimiter.create(this.rateLimitPerSecond);
  }

  @Override
  public void start() {
    // Nothing to do
  }

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {
    this.rateLimiter.acquire(Ints.checkedCast(permits));
    return NO_OP_CLOSEABLE;
  }

  @Override
  public void stop() {
    // Nothing to do
  }

  private static double convertRate(double originalRate, TimeUnit originalTimeUnit, TimeUnit targetTimeUnit) {
    return originalRate / targetTimeUnit.convert(1, originalTimeUnit);
  }
}
