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

package org.apache.gobblin.instrumented;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;


/**
 * Interface for classes instrumenting their execution into a {@link org.apache.gobblin.metrics.MetricContext}.
 */
public interface Instrumentable {

  /**
   * Get {@link org.apache.gobblin.metrics.MetricContext} containing metrics related to this Instrumentable.
   * @return an instance of {@link org.apache.gobblin.metrics.MetricContext}.
   */
  @Nonnull
  public MetricContext getMetricContext();

  /**
   * Returns true if instrumentation is activated.
   * @return true if instrumentation is enabled, false otherwise.
   */
  public boolean isInstrumentationEnabled();

  /**
   * Generate tags that should be added to the {@link org.apache.gobblin.metrics.MetricContext}.
   * @return List of tags to add to {@link org.apache.gobblin.metrics.MetricContext}.
   */
  public List<Tag<?>> generateTags(State state);

  /**
   * Generate a new {@link org.apache.gobblin.metrics.MetricContext} replacing old {@link org.apache.gobblin.metrics.Tag} with input
   * {@link org.apache.gobblin.metrics.Tag} (only tags with the same keys will be replaced),
   * and recreate all metrics in this new context.
   *
   * <p>
   *   This method is useful when the state of the {@link org.apache.gobblin.instrumented.Instrumentable} changes and the user
   *   wants that state change to be reflected in the tags of the instrumentation.
   * </p>
   *
   * <p>
   *   Notice that this method creates a brand new {@link org.apache.gobblin.metrics.MetricContext} and {@link org.apache.gobblin.metrics.Metric}
   *   every time it is called,
   *   with the associated processing and memory overhead. Use sparingly only for state changes that must be visible in
   *   emitted metrics.
   * </p>
   *
   * @param tags additional {@link org.apache.gobblin.metrics.Tag}.
   */
  public void switchMetricContext(List<Tag<?>> tags);

  /**
   * Switches the existing {@link org.apache.gobblin.metrics.MetricContext} with the supplied metric context and regenerates
   * {@link org.apache.gobblin.metrics.Metric}.
   *
   * <p>
   *   This method is useful when the state of the {@link org.apache.gobblin.instrumented.Instrumentable} changes and the user
   *   wants that state change to be reflected in the set of {@link org.apache.gobblin.metrics.Tag} of the instrumentation.
   * </p>
   *
   * <p>
   *   This method is an alternative to {@link #switchMetricContext(List)} when metric context switching is done
   *   often between a small set of contexts. The subclass should keep a list of previously generated contexts as a cache,
   *   and call this method to simply switch between the different contexts,
   *   avoiding the overhead of generating a brand new context every time a metric context switch is required.
   * </p>
   *
   * @param context new {@link org.apache.gobblin.metrics.MetricContext}.
   */
  public void switchMetricContext(MetricContext context);
}
