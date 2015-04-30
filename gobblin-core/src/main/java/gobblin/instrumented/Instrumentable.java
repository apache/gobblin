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

import gobblin.metrics.MetricContext;


/**
 * Interface for classes instrumenting their execution into a {@link gobblin.metrics.MetricContext}.
 */
public interface Instrumentable {

  /**
   * Get {@link gobblin.metrics.MetricContext} containing metrics related to this Instrumentable.
   * @return an instance of {@link gobblin.metrics.MetricContext}.
   */
  public MetricContext getMetricContext();

  /**
   * Returns true if instrumentation is activated.
   * @return
   */
  public boolean isInstrumentationEnabled();
}
