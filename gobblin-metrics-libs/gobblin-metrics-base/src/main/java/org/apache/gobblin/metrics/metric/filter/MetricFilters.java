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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;


/**
 * A utility class for working with {@link MetricFilter}s.
 *
 * @see MetricFilter
 */
public class MetricFilters {

  public static MetricFilter and(final MetricFilter metricFilter1, final MetricFilter metricFilter2) {
    return new MetricFilter() {
      @Override public boolean matches(String name, Metric metric) {
        return metricFilter1.matches(name, metric) && metricFilter2.matches(name, metric);
      }
    };
  }
}
