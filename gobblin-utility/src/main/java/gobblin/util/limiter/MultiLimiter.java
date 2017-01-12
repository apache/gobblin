/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.util.limiter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import lombok.Getter;


/**
 * A {@link Limiter} that contains multiple underlying {@link Limiter}s and requests permits from all those limiters.
 *
 * <p>
 *   Permits are requested serially from the underlying brokers in the order they appear in the constructor. If an underlying
 *   limiter is unable to provide permits, permits from previous underlying limiters are closed. However, for
 *   {@link NonRefillableLimiter}s, the permits are lost permanently.
 * </p>
 *
 * <p>
 *   Note: {@link MultiLimiter} does some optimization of the underlying limiters:
 *      * underlying {@link MultiLimiter}s are opened, and their underlying limiters are used instead.
 *      * {@link NoopLimiter}s are ignored.
 *      * underlying {@link Limiter}s are deduplicated to avoid requesting permits from the same limiter twice.
 *        Deduplication is done based on the {@link #equals(Object)} method of the underlying limiters.
 * </p>
 */
public class MultiLimiter implements Limiter {

  @Getter
  private final ImmutableList<Limiter> underlyingLimiters;

  public MultiLimiter(Limiter... underlyingLimiters) {
    ImmutableList.Builder<Limiter> builder = ImmutableList.builder();
    Set<Limiter> seenLimiters = Sets.newHashSet();
    for (Limiter limiter : underlyingLimiters) {
      if (limiter instanceof MultiLimiter) {
        for (Limiter innerLimiter : ((MultiLimiter) limiter).underlyingLimiters) {
          addLimiterIfNotSeen(innerLimiter, builder, seenLimiters);
        }
      } else if (!(limiter instanceof NoopLimiter)) {
        addLimiterIfNotSeen(limiter, builder, seenLimiters);
      }
    }
    this.underlyingLimiters = builder.build();
  }

  private void addLimiterIfNotSeen(Limiter limiter, ImmutableList.Builder<Limiter> builder, Set<Limiter> seenLimiters) {
    if (!seenLimiters.contains(limiter)) {
      builder.add(limiter);
      seenLimiters.add(limiter);
    }
  }

  @Override
  public void start() {
    for (Limiter limiter : this.underlyingLimiters) {
      limiter.start();
    }
  }

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {
    Closer closer = Closer.create();
    for (Limiter limiter : this.underlyingLimiters) {
      Closeable permit = limiter.acquirePermits(permits);
      if (permit == null) {
        try {
          closer.close();
        } catch (IOException ioe) {
          throw new RuntimeException("Could not return intermediate permits.");
        }
        return null;
      }
      closer.register(permit);
    }
    return closer;
  }

  @Override
  public void stop() {
    for (Limiter limiter : this.underlyingLimiters) {
      limiter.stop();
    }
  }
}
