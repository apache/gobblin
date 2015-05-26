/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented;

import java.util.List;

import javax.annotation.Nonnull;

import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;


/**
 * Interface for classes instrumenting their execution into a {@link gobblin.metrics.MetricContext}.
 */
public interface Instrumentable {

  /**
   * Get {@link gobblin.metrics.MetricContext} containing metrics related to this Instrumentable.
   * @return an instance of {@link gobblin.metrics.MetricContext}.
   */
  @Nonnull
  public MetricContext getMetricContext();

  /**
   * Returns true if instrumentation is activated.
   * @return true if instrumentation is enabled, false otherwise.
   */
  public boolean isInstrumentationEnabled();

  /**
   * Generate tags that should be added to the {@link gobblin.metrics.MetricContext}.
   * @return List of tags to add to Metric Context.
   */
  public List<Tag<?>> generateTags(State state);

  /**
   * Generate a new metric context replacing old tags with input tags (only tags with the same keys will be replaced),
   * and recreate all metrics in this new context.
   *
   * <p>
   *   This method is useful when the state of the extractor changes and the user wants that state change to
   *   be reflected in the tags of the extractor instrumentation.
   * </p>
   *
   * <p>
   *   Notice that this method creates a brand new metric context and metrics every time it is called, with the
   *   associated processing and memory overhead. Use sparingly only for state changes that MUST be visible in
   *   emitted metrics.
   * </p>
   *
   * @param tags additional tags.
   */
  public void switchMetricContext(List<Tag<?>> tags);

  /**
   * Switches the existing metric context with the supplied metric context and regenerates metrics.
   *
   * <p>
   *   This method is useful when the state of the extractor changes and the user wants that state change to
   *   be reflected in the tags of the extractor instrumentation.
   * </p>
   *
   * <p>
   *   This method is an alternative to {@link #switchMetricContext(List)} when metric context switching is done
   *   often between a small set of contexts. The subclass should cache contexts, and call this method instead, saving
   *   the overhead of generating a brand new context every time a metric context switch is required.
   * </p>
   *
   * @param context new context.
   */
  public void switchMetricContext(MetricContext context);
}
