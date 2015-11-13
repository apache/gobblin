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

package gobblin.metrics.metric;

import com.codahale.metrics.Metric;

import gobblin.metrics.ContextAwareMetric;


/**
 * Inner {@link Metric} used for reporting metrics one last time even after the user-facing wrapper has been
 * garbage collected.
 */
public interface InnerMetric extends Metric {

  /**
   * @return the associated {@link ContextAwareMetric}.
   */
  public ContextAwareMetric getContextAwareMetric();

}
