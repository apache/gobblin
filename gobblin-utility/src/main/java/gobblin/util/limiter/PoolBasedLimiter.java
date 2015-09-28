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
import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.google.common.primitives.Ints;


/**
 * An implementation of {@link Limiter} that ony allows permits to be acquired from a pool.
 *
 * <p>
 *   This implementation uses a {@link Semaphore} as a permit pool. {@link #acquirePermits(long)}
 *   is blocking and will always return {@link true} after the permits are successfully acquired
 *   (probably after being blocked for some amount of time). Permit refills are supported by this
 *   implementation using {@link Semaphore#release(int)}. Also {@link #acquirePermits(long)} only
 *   accepts input arguments that can be safely casted to an integer bounded by
 *   {@link Integer#MAX_VALUE}.
 * </p>
 *
 * @author ynli
 */
public class PoolBasedLimiter implements Limiter {

  private final Semaphore permitPool;

  public PoolBasedLimiter(int poolSize) {
    this.permitPool = new Semaphore(poolSize);
  }

  @Override
  public void start() {
    // Nothing to do
  }

  @Override
  public Closeable acquirePermits(long permits)
      throws InterruptedException {
    int permitsToAcquire = Ints.checkedCast(permits);
    this.permitPool.acquire(permitsToAcquire);
    return new PoolPermitCloseable(this.permitPool, permitsToAcquire);
  }

  @Override
  public void stop() {
    // Nothing to do
  }

  private static class PoolPermitCloseable implements Closeable {

    private final Semaphore permitPool;
    private final int permits;

    public PoolPermitCloseable(Semaphore permitPool, int permits) {
      this.permitPool = permitPool;
      this.permits = permits;
    }

    @Override
    public void close()
        throws IOException {
      this.permitPool.release(this.permits);
    }
  }
}
