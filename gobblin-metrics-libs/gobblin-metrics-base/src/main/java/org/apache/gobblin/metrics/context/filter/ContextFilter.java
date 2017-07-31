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

package org.apache.gobblin.metrics.context.filter;

import java.util.Set;

import org.apache.gobblin.metrics.InnerMetricContext;
import org.apache.gobblin.metrics.MetricContext;


/**
 * Filter for selecting {@link MetricContext}s to report by a {@link org.apache.gobblin.metrics.reporter.ContextAwareReporter}.
 */
public interface ContextFilter {

  /**
   * Get all {@link MetricContext}s in the {@link MetricContext} tree that should be reported.
   * @return Set of {@link MetricContext}s that should be reported.
   */
  public Set<MetricContext> getMatchingContexts();

  /**
   * Whether the input {@link MetricContext} should be reported.
   * @param metricContext {@link MetricContext} to check.
   * @return true if the input {@link MetricContext} should be reported.
   */
  public boolean matches(MetricContext metricContext);

  /**
   * This method is called by a {@link org.apache.gobblin.metrics.reporter.ContextAwareReporter} when a {@link MetricContext}
   * that it used to report is cleaned. Every cleaned {@link MetricContext} is a leaf of the tree. In some circumstances,
   * after removing the {@link MetricContext} it is necessary to start reporting the parent (for example, if we
   * are reporting leaves, and the parent is a new leaf). This method is called to determine if the
   * {@link org.apache.gobblin.metrics.reporter.ContextAwareReporter} should start reporting the parent of the input
   * {@link InnerMetricContext}.
   *
   * @param removedMetricContext {@link InnerMetricContext} backing up the newly removed {@link MetricContext}.
   * @return true if the parent of the removed {@link MetricContext} should be reported.
   */
  public boolean shouldReplaceByParent(InnerMetricContext removedMetricContext);
}
