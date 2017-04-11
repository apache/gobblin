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

package gobblin.metrics.context.filter;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;


/**
 * Accepts all {@link MetricContext} in the tree.
 */
public class AllContextFilter implements ContextFilter {

  @Override public Set<MetricContext> getMatchingContexts() {
    ImmutableSet.Builder<MetricContext> builder = ImmutableSet.builder();
    addContextsRecursively(builder, RootMetricContext.get());
    return builder.build();
  }

  @Override public boolean matches(MetricContext metricContext) {
    return true;
  }

  @Override public boolean shouldReplaceByParent(InnerMetricContext removedMetricContext) {
    return false;
  }

  private void addContextsRecursively(ImmutableSet.Builder<MetricContext> builder, MetricContext metricContext) {
    builder.add(metricContext);
    for(MetricContext context : metricContext.getChildContextsAsMap().values()) {
      addContextsRecursively(builder, context);
    }
  }
}
