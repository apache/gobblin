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
import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.google.common.primitives.Ints;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;


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
 * @author Yinan Li
 */
public class PoolBasedLimiter implements Limiter {

  @Alias(value = "PoolBasedLimiter")
  public static class Factory implements LimiterFactory {
    public static final String POOL_SIZE_KEY = "poolSize";

    @Override
    public Limiter buildLimiter(Config config) {
      if (!config.hasPath(POOL_SIZE_KEY)) {
        throw new IllegalArgumentException("Missing key " + POOL_SIZE_KEY);
      }
      return new PoolBasedLimiter(config.getInt(POOL_SIZE_KEY));
    }
  }

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
