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
import com.codahale.metrics.Counter;

import org.apache.gobblin.metrics.metric.InnerMetric;
import org.apache.gobblin.metrics.metric.ProxyMetric;

import lombok.experimental.Delegate;


/**
 * A type of {@link Counter}s that are aware of their {@link MetricContext} and can have associated
 * {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareCounter} will be applied automatically to the
 *   {@link ContextAwareCounter} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Counter} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Counter} to the wrapped {@link com.codahale.metrics.Counter}.
 * </p>
 *
 * @author Yinan Li
 */
public class ContextAwareCounter extends Counter implements ProxyMetric, ContextAwareMetric {

  private final MetricContext metricContext;
  @Delegate
  private final InnerCounter innerCounter;

  ContextAwareCounter(MetricContext context, String name) {
    this.innerCounter = new InnerCounter(context, name, this);
    this.metricContext = context;
  }

  @Override
  public MetricContext getContext() {
    return this.metricContext;
  }

  @Override public InnerMetric getInnerMetric() {
    return this.innerCounter;
  }
}
