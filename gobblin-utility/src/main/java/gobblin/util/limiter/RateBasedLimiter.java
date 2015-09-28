/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.limiter;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;


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
 * @author ynli
 */
public class RateBasedLimiter extends NonRefillableLimiter {

  private final RateLimiter rateLimiter;

  public RateBasedLimiter(double rateLimit) {
    this(rateLimit, TimeUnit.SECONDS);
  }

  public RateBasedLimiter(double rateLimit, TimeUnit timeUnit) {
    this.rateLimiter = RateLimiter.create(convertRate(rateLimit, timeUnit, TimeUnit.SECONDS));
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

  private double convertRate(double originalRate, TimeUnit originalTimeUnit, TimeUnit targetTimeUnit) {
    return originalRate / targetTimeUnit.convert(1, originalTimeUnit);
  }
}
