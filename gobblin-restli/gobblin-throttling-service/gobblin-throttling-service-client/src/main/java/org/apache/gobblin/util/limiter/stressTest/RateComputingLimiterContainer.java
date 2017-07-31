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

package org.apache.gobblin.util.limiter.stressTest;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.util.Decorator;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.RestliServiceBasedLimiter;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Used to compute statistics on a set of {@link Limiter}s used in a throttling service stress test.
 */
@Slf4j
@RequiredArgsConstructor
public class RateComputingLimiterContainer {
  private final List<AtomicLong> subLimiterPermitCounts = Lists.newArrayList();
  private final Queue<Long> unusedPermitsCounts = new LinkedList<>();

  private Map<String, Long> lastReportTimes = Maps.newHashMap();

  /**
   * Decorate a {@link Limiter} to measure its permit rate for statistics computation.
   */
  public Limiter decorateLimiter(Limiter limiter) {
    AtomicLong localCount = new AtomicLong();
    return new RateComputingLimiterDecorator(limiter, localCount);
  }

  /**
   * A {@link Limiter} decorator that records all permits granted.
   */
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public class RateComputingLimiterDecorator implements Limiter, Decorator {

    private final Limiter underlying;
    private final AtomicLong localPermitCount;

    @Override
    public Object getDecoratedObject() {
      return this.underlying;
    }

    @Override
    public void start() {
      this.underlying.start();
      RateComputingLimiterContainer.this.subLimiterPermitCounts.add(this.localPermitCount);
    }

    @Override
    public Closeable acquirePermits(long permits) throws InterruptedException {
      Closeable closeable = this.underlying.acquirePermits(permits);
      this.localPermitCount.addAndGet(permits);
      return closeable;
    }

    @Override
    public void stop() {
      this.underlying.stop();
      if (this.underlying instanceof RestliServiceBasedLimiter) {
        RestliServiceBasedLimiter restliLimiter = (RestliServiceBasedLimiter) this.underlying;
        RateComputingLimiterContainer.this.unusedPermitsCounts.add(restliLimiter.getUnusedPermits());
        log.info("Unused permits: " + restliLimiter.getUnusedPermits());
      }
      RateComputingLimiterContainer.this.subLimiterPermitCounts.remove(this.localPermitCount);
    }
  }

  /**
   * Get a {@link DescriptiveStatistics} object with the rate of permit granting for all {@link Limiter}s decorated
   * with this {@link RateComputingLimiterContainer}.
   */
  public @Nullable DescriptiveStatistics getRateStatsSinceLastReport() {
    return getNormalizedStatistics("seenQPS", Lists.transform(this.subLimiterPermitCounts, new Function<AtomicLong, Double>() {
      @Override
      public Double apply(AtomicLong atomicLong) {
        return (double) atomicLong.getAndSet(0);
      }
    }));
  }

  public @Nullable DescriptiveStatistics getUnusedPermitsSinceLastReport() {
    DescriptiveStatistics stats = getNormalizedStatistics("unusedPermits", this.unusedPermitsCounts);
    this.unusedPermitsCounts.clear();
    return stats;
  }

  private @Nullable DescriptiveStatistics getNormalizedStatistics(String key, Collection<? extends Number> values) {
    long now = System.currentTimeMillis();

    long deltaTime = 0;
    if (this.lastReportTimes.containsKey(key)) {
      deltaTime = now - this.lastReportTimes.get(key);
    }
    this.lastReportTimes.put(key, now);

    if (deltaTime == 0) {
      return null;
    }

    double[] normalizedValues = new double[values.size()];
    int i = 0;
    for (Number value : values) {
      normalizedValues[i++] = 1000 * value.doubleValue() / deltaTime;
    }

    return new DescriptiveStatistics(normalizedValues);
  }
}
