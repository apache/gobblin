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

package gobblin.metrics;

import java.util.Arrays;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * Class that holds {@link Counter}s for each value of an enum. The metric generated with have the name of the enum.
 *
 * @see Counters#initialize(MetricContext, Class, Class)
 * @param <E>
 */
public class Counters<E extends Enum<E>> {

  private ImmutableMap<E, Counter> counters;

  /**
   * Creates a {@link Counter} for every value of the enumClass.
   * Use {@link #inc(Enum, long)} to increment the counter associated with a enum value
   *
   * @param metricContext that {@link Counter}s will be registered
   * @param enumClass that define the names of {@link Counter}s. One counter is created per value
   * @param instrumentedClass name that will be prefixed in the metric name
   */
  public void initialize(final MetricContext metricContext, final Class<E> enumClass, final Class<?> instrumentedClass) {

    Builder<E, Counter> builder = ImmutableMap.builder();

    for (E e : Arrays.asList(enumClass.getEnumConstants())) {
      builder.put(e, metricContext.counter(MetricRegistry.name(instrumentedClass, e.name())));
    }

    counters = builder.build();
  }

  /**
   * Increment the counter associated with enum value passed.
   *
   * @param e Counter to increment.
   * @param n the value to increment
   */
  public void inc(E e, long n) {
    if (counters != null && counters.containsKey(e)) {
      counters.get(e).inc(n);
    }
  }

  /**
   * Get count for counter associated with enum value passed.
   * @param e Counter to query.
   * @return the count for this counter.
   */
  public long getCount(E e) {
    if (counters.containsKey(e)) {
      return counters.get(e).getCount();
    } else {
      return 0l;
    }
  }
}
