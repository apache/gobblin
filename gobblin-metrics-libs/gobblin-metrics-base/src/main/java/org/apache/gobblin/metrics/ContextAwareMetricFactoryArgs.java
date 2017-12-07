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

package org.apache.gobblin.metrics;

import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A class which wraps all arguments required by {@link ContextAwareMetricFactory}s.
 *
 * A concrete {@link ContextAwareMetricFactory} knows how to interpret this class into its corresponding sub-type.
 */
@AllArgsConstructor
@Getter
public class ContextAwareMetricFactoryArgs {
  protected final MetricContext context;
  protected final String name;

  @Getter
  public static class SlidingTimeWindowArgs extends  ContextAwareMetricFactoryArgs {
    protected final long windowSize;
    protected final TimeUnit unit;
    public SlidingTimeWindowArgs(MetricContext context, String name, long windowSize, TimeUnit unit) {
      super(context, name);
      this.windowSize = windowSize;
      this.unit = unit;
    }
  }
}