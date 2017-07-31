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

package org.apache.gobblin.metrics.metric.filter;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link MetricFilters}.
 */
@Test
public class MetricFiltersTest {

  @Test
  public void andTest() {
    MetricFilter trueMetricFilter = mock(MetricFilter.class);
    when(trueMetricFilter.matches(any(String.class), any(Metric.class))).thenReturn(true);

    MetricFilter falseMetricFilter = mock(MetricFilter.class);
    when(falseMetricFilter.matches(any(String.class), any(Metric.class))).thenReturn(false);

    Assert.assertTrue(MetricFilters.and(trueMetricFilter, trueMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(trueMetricFilter, falseMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(falseMetricFilter, trueMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(falseMetricFilter, falseMetricFilter).matches("", mock(Metric.class)));
  }
}
