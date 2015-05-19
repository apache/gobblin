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

import java.util.concurrent.Semaphore;


/**
 * An implementation of {@link Limiter} that ony allows permits to be acquired from a pool.
 *
 * <p>
 *   This implementation uses a {@link Semaphore} as a permit pool. {@link #acquirePermits(int)}
 *   is blocking and will always return {@link true} after the permits are successfully acquired
 *   (probably after being blocked for some amount of time). Permit refills are supported by
 *   {@link #releasePermits(int)}.
 * </p>
 *
 * @author ynli
 */
public class PoolBasedLimiter implements Limiter {

  private final Semaphore permits;

  public PoolBasedLimiter(int poolSize) {
    this.permits = new Semaphore(poolSize);
  }

  @Override
  public void start() {
    // Nothing to do
  }

  @Override
  public boolean acquirePermits(int permits)
      throws InterruptedException {
    this.permits.acquire(permits);
    return true;
  }

  @Override
  public void releasePermits(int permits) {
    this.permits.release(permits);
  }

  @Override
  public void stop() {
    // Nothing to do
  }
}
