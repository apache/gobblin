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

package gobblin.metrics;

import com.codahale.metrics.Metric;


/**
 * An interface for a type of {@link com.codahale.metrics.Metric}s that are aware of their
 * {@link MetricContext} and can have associated {@link Tag}s.
 *
 * @author ynli
 */
public interface ContextAwareMetric extends Metric, Taggable {

  /**
   * Get the name of the metric.
   *
   * @return the name of the metric
   */
  public String getName();

  /**
   * Get the fully-qualified name of the metric.
   *
   * <p>
   *   See {@link Taggable#metricNamePrefix(boolean)} for the semantic of {@code includeTagKeys}.
   * </p>
   *
   * @param includeTagKeys whether to include tag keys in the metric name prefix
   * @return the fully-qualified name of the metric
   */
  public String getFullyQualifiedName(boolean includeTagKeys);

  /**
   * Get the {@link MetricContext} the metric is registered in.
   */
  public MetricContext getContext();
}
