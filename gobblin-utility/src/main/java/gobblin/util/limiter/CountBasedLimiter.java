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


/**
 * An implementation of {@link Limiter} that limits the number of permits allowed to be issued.
 *
 * <p>
 *   {@link #acquirePermits(long)} will return {@code false} once if there's not enough permits
 *   available to satisfy the request. Permit refills are not supported in this implementation.
 * </p>
 * </p>
 *
 * @author ynli
 */
public class CountBasedLimiter extends NonRefillableLimiter {

  private final long countLimit;
  private long count;

  public CountBasedLimiter(long countLimit) {
    this.countLimit = countLimit;
    this.count = 0;
  }

  @Override
  public void start() {
    // Nothing to do
  }

  @Override
  public synchronized Closeable acquirePermits(long permits)
      throws InterruptedException {
    // Check if the request can be satisfied
    if (this.count + permits <= this.countLimit) {
      this.count += permits;
      return NO_OP_CLOSEABLE;
    }
    return null;
  }

  @Override
  public void stop() {
    // Nothing to do
  }
}
