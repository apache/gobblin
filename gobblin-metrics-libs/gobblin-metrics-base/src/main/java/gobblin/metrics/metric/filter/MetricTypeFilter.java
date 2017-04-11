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

package gobblin.metrics.metric.filter;

import java.util.List;

import javax.annotation.Nullable;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.metrics.metric.Metrics;


/**
 * Implementation of {@link MetricFilter} that takes in a {@link String} containing a comma separated list of
 * {@link Metrics}. This {@link MetricFilter} {@link #matches(String, Metric)} a {@link Metric} if the given
 * {@link Metric} has a type in the given list of allowed {@link Metrics}.
 */
public class MetricTypeFilter implements MetricFilter {

  private List<Metrics> allowedMetrics;

  public MetricTypeFilter(String allowedMetrics) {
    if (Strings.isNullOrEmpty(allowedMetrics)) {
      this.allowedMetrics = Lists.newArrayList(Metrics.values());
    } else {
      List<String> allowedMetricsList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(allowedMetrics);
      this.allowedMetrics = Lists.transform(allowedMetricsList, new Function<String, Metrics>() {
        @Nullable @Override public Metrics apply(String input) {
          return input == null ? null : Metrics.valueOf(input);
        }
      });
    }
  }

  @Override
  public boolean matches(String name, Metric metric) {
    final Class<? extends Metric> metricClass = metric.getClass();

    return Iterables.any(this.allowedMetrics, new Predicate<Metrics>() {
      @Override public boolean apply(@Nullable Metrics input) {
        return input != null && input.getMetricClass().isAssignableFrom(metricClass);
      }
    });
  }
}
