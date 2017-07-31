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

import static org.mockito.Mockito.mock;

import com.codahale.metrics.Metric;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link MetricNameRegexFilter}.
 */
@Test
public class MetricNameRegexFilterTest {

  @Test
  public void matchesTest() {
    MetricNameRegexFilter metricNameRegexFilter1 = new MetricNameRegexFilter(".*");
    Assert.assertTrue(metricNameRegexFilter1.matches("test1", mock(Metric.class)));
    Assert.assertTrue(metricNameRegexFilter1.matches("test2", mock(Metric.class)));
    Assert.assertTrue(metricNameRegexFilter1.matches("test3", mock(Metric.class)));

    MetricNameRegexFilter metricNameRegexFilter2 = new MetricNameRegexFilter("test1");
    Assert.assertTrue(metricNameRegexFilter2.matches("test1", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexFilter2.matches("test2", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexFilter2.matches("test3", mock(Metric.class)));
  }
}
