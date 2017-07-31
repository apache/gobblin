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

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;

import lombok.AccessLevel;
import lombok.Getter;


/**
 * An implementation of {@link Limiter} that limits the number of permits allowed to be issued.
 *
 * <p>
 *   {@link #acquirePermits(long)} will return {@code false} once if there's not enough permits
 *   available to satisfy the request. Permit refills are not supported in this implementation.
 * </p>
 * </p>
 *
 * @author Yinan Li
 */
public class CountBasedLimiter extends NonRefillableLimiter {

  public static final String FACTORY_ALIAS = "CountBasedLimiter";

  @Alias(value = FACTORY_ALIAS)
  public static class Factory implements LimiterFactory {
    public static final String COUNT_KEY = "maxPermits";

    @Override
    public Limiter buildLimiter(Config config) {
      if (!config.hasPath(COUNT_KEY)) {
        throw new IllegalArgumentException("Missing key " + COUNT_KEY);
      }
      return new CountBasedLimiter(config.getLong(COUNT_KEY));
    }
  }

  @Getter
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
