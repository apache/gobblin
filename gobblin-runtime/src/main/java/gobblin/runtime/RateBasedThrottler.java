/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

import gobblin.util.ExecutorsUtils;


/**
 * An implementation of {@link Throttler} that throttles based on the rate of some events.
 *
 * <p>
 *   This implementation uses a {@link Semaphore} that is initialized to a given rate threshold (max
 *   number of events allowed per a given time unit). Each call to {@link #waitForNextPermit()} tries
 *   to acquire a permit by calling {@link Semaphore#acquire()}, which may caused the caller to be
 *   blocked if all the permits have been used within the current time unit. Then there's a scheduled
 *   task running in a {@link ScheduledThreadPoolExecutor} that drains all the remaining permits in
 *   the {@link Semaphore} and restocks the permits by calling {@link Semaphore#release(int)} per every
 *   time unit. So this effectively limits the rate permits can be acquired and therefore the rate of
 *   the events.
 * </p>
 *
 * @author ynli
 */
public class RateBasedThrottler implements Throttler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RateBasedThrottler.class);

  private final int rateLimit;
  private final TimeUnit timeUnit;
  private final Semaphore permits;
  private final ScheduledThreadPoolExecutor permitRestockExecutor;

  public RateBasedThrottler(int rateLimit, TimeUnit timeUnit) {
    this.rateLimit = rateLimit;
    this.timeUnit = timeUnit;
    this.permits = new Semaphore(rateLimit);
    this.permitRestockExecutor = new ScheduledThreadPoolExecutor(
        1, ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("RateBaseThrottler")));
  }

  @Override
  public void start() {
    this.permitRestockExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // Drain all the permits left and restock the permits
        permits.drainPermits();
        permits.release(rateLimit);
      }
    }, 1, 1, this.timeUnit);
  }

  @Override
  public boolean waitForNextPermit() throws InterruptedException {
    this.permits.acquire();
    return true;
  }

  @Override
  public void stop() {
    this.permitRestockExecutor.shutdownNow();
  }
}
