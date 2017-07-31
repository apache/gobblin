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

package org.apache.gobblin.metrics.context;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;

/**
 * Interface for a context that can be reported (e.g. {@link org.apache.gobblin.metrics.InnerMetricContext},
 * {@link org.apache.gobblin.metrics.MetricContext}).
 */
public interface ReportableContext {

  /**
   * Get the name of this {@link MetricContext}.
   *
   * @return the name of this {@link MetricContext}
   */
  public String getName();

  /**
   * Get the parent {@link MetricContext} of this {@link MetricContext} wrapped in an
   * {@link com.google.common.base.Optional}, which may be absent if it has not parent
   * {@link MetricContext}.
   *
   * @return the parent {@link MetricContext} of this {@link MetricContext} wrapped in an
   *         {@link com.google.common.base.Optional}
   */
  public Optional<? extends ReportableContext> getParent();

  /**
   * Get a view of the child {@link org.apache.gobblin.metrics.MetricContext}s as a {@link com.google.common.collect.ImmutableMap}.
   * @return {@link com.google.common.collect.ImmutableMap} of
   *      child {@link org.apache.gobblin.metrics.MetricContext}s keyed by their names.
   */
  public Map<String, ? extends ReportableContext> getChildContextsAsMap();

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getNames()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedSet<String> getNames();

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getMetrics()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public Map<String, com.codahale.metrics.Metric> getMetrics();

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getGauges(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedMap<String, Gauge> getGauges(MetricFilter filter);

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getCounters(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedMap<String, Counter> getCounters(MetricFilter filter);

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getHistograms(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedMap<String, Histogram> getHistograms(MetricFilter filter);

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getMeters(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedMap<String, Meter> getMeters(MetricFilter filter);

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getTimers(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  public SortedMap<String, Timer> getTimers(MetricFilter filter);

  /**
   * Get all {@link Tag}s in a list.
   *
   * <p>
   *   This method guarantees no duplicated {@link Tag}s and the order of {@link Tag}s
   *   is the same as the one in which the {@link Tag}s were added.
   * </p>
   *
   * @return all {@link Tag}s in a list
   */
  public List<Tag<?>> getTags();

  /**
   * @return all {@link Tag}s in a map.
   */
  public Map<String, Object> getTagMap();

}
