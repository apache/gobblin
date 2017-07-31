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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link MetricTypeFilter}.
 */
@Test
public class MetricTypeFilterTest {

  @Test
  public void matchesTest() {
    Counter counter = mock(Counter.class);
    Gauge gauge = mock(Gauge.class);
    Histogram histogram = mock(Histogram.class);
    Meter meter = mock(Meter.class);
    Timer timer = mock(Timer.class);

    MetricTypeFilter noMetricTypeFilter = new MetricTypeFilter(null);
    Assert.assertTrue(noMetricTypeFilter.matches("", counter));
    Assert.assertTrue(noMetricTypeFilter.matches("", gauge));
    Assert.assertTrue(noMetricTypeFilter.matches("", histogram));
    Assert.assertTrue(noMetricTypeFilter.matches("", meter));
    Assert.assertTrue(noMetricTypeFilter.matches("", timer));

    MetricTypeFilter counterMetricTypeFilter = new MetricTypeFilter("COUNTER");
    Assert.assertTrue(counterMetricTypeFilter.matches("", counter));
    Assert.assertFalse(counterMetricTypeFilter.matches("", gauge));
    Assert.assertFalse(counterMetricTypeFilter.matches("", histogram));
    Assert.assertFalse(counterMetricTypeFilter.matches("", meter));
    Assert.assertFalse(counterMetricTypeFilter.matches("", timer));

    MetricTypeFilter allMetricTypeFilter = new MetricTypeFilter("COUNTER,GAUGE,HISTOGRAM,METER,TIMER");
    Assert.assertTrue(allMetricTypeFilter.matches("", counter));
    Assert.assertTrue(allMetricTypeFilter.matches("", gauge));
    Assert.assertTrue(allMetricTypeFilter.matches("", histogram));
    Assert.assertTrue(allMetricTypeFilter.matches("", meter));
    Assert.assertTrue(allMetricTypeFilter.matches("", timer));
  }
}
