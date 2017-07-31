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

import java.io.Closeable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * An implementation of {@link Limiter} that limits the time elapsed for some events.
 *
 * <p>
 *   This implementation uses a task scheduled in a {@link ScheduledThreadPoolExecutor} that will
 *   fire once after a given amount of time has elapsed. The task once fired, will flip a boolean
 *   flag that tells if permits should be issued. The flag is initially set to {@code true}. Thus,
 *   no permits are issued once the flag is flipped after the given amount of time has elapsed.
 * </p>
 *
 * <p>
 *   {@link #acquirePermits(long)} will return {@code false} once the time limit is reached. Permit
 *   refills are not supported in this implementation.
 * </p>
 *
 * @author Yinan Li
 */
public class TimeBasedLimiter extends NonRefillableLimiter {

  @Alias(value = "time")
  public static class Factory implements LimiterFactory {
    public static final String MAX_SECONDS_KEY = "maxSeconds";

    @Override
    public Limiter buildLimiter(Config config) {
      if (!config.hasPath(MAX_SECONDS_KEY)) {
        throw new RuntimeException("Missing key " + MAX_SECONDS_KEY);
      }
      return new TimeBasedLimiter(config.getLong(MAX_SECONDS_KEY));
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeBasedLimiter.class);

  private final long timeLimit;
  private final TimeUnit timeUnit;
  private final ScheduledThreadPoolExecutor flagFlippingExecutor;
  // A flag telling if a permit is allowed to be issued
  private volatile boolean canIssuePermit = true;

  public TimeBasedLimiter(long timeLimit) {
    this(timeLimit, TimeUnit.SECONDS);
  }

  public TimeBasedLimiter(long timeLimit, TimeUnit timeUnit) {
    this.timeLimit = timeLimit;
    this.timeUnit = timeUnit;
    this.flagFlippingExecutor = new ScheduledThreadPoolExecutor(1,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("TimeBasedThrottler")));
  }

  @Override
  public void start() {
    this.flagFlippingExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        // Flip the flag once the scheduled time is reached
        TimeBasedLimiter.this.canIssuePermit = false;
      }
    }, this.timeLimit, this.timeUnit);
  }

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {
    return this.canIssuePermit ? NO_OP_CLOSEABLE : null;
  }

  @Override
  public void stop() {
    this.flagFlippingExecutor.shutdownNow();
  }
}
