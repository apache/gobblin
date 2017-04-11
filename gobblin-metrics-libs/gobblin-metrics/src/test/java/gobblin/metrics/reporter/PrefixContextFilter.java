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

package gobblin.metrics.reporter;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;
import gobblin.metrics.context.filter.ContextFilter;


/**
 * {@link ContextFilter} that accepts {@link MetricContext} based on whether the name has the correct prefix.
 */
public class PrefixContextFilter implements ContextFilter {

  public static final String PREFIX_FILTER = "text.context.filter.prefix.string";

  public static Config setPrefixString(Config config, String prefix) {
    return config.withValue(PREFIX_FILTER, ConfigValueFactory.fromAnyRef(prefix));
  }

  private final String prefix;

  public PrefixContextFilter(Config config) {
    this.prefix = config.getString(PREFIX_FILTER);
  }

  @Override public Set<MetricContext> getMatchingContexts() {
    Set<MetricContext> contexts = Sets.newHashSet();
    addContextsRecursively(contexts, RootMetricContext.get());
    return ImmutableSet.copyOf(contexts);
  }

  @Override public boolean matches(MetricContext metricContext) {
    return metricContext.getName().startsWith(this.prefix);
  }

  @Override public boolean shouldReplaceByParent(InnerMetricContext removedMetricContext) {
    return false;
  }

  private void addContextsRecursively(Set<MetricContext> matchingContexts, MetricContext context) {
    if(matches(context)) {
      matchingContexts.add(context);
    }
    for(MetricContext context1 : context.getChildContextsAsMap().values()) {
      addContextsRecursively(matchingContexts, context1);
    }
  }
}
