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

package gobblin.metrics.context;

import lombok.Getter;

import java.lang.ref.WeakReference;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;


/**
 * {@link WeakReference} to a {@link MetricContext} used to notify the {@link RootMetricContext} of garbage collection
 * of {@link MetricContext}s.
 */
@Getter
public class ContextWeakReference extends WeakReference<MetricContext> {

  private final InnerMetricContext innerContext;

  public ContextWeakReference(MetricContext referent, InnerMetricContext innerContext) {
    super(referent, RootMetricContext.get().getReferenceQueue());
    this.innerContext = innerContext;
  }

}
