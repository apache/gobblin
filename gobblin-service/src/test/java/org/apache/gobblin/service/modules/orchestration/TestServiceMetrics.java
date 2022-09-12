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

package org.apache.gobblin.service.modules.orchestration;

import com.codahale.metrics.Metric;
import org.apache.gobblin.metrics.metric.filter.MetricNameRegexFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

@Test
public class TestServiceMetrics {
  @Test
  public void matchesTest() {

    MetricNameRegexFilter metricNameRegexForDagManager = DagManagerMetrics.getMetricsFilterForDagManager();
    Assert.assertTrue(metricNameRegexForDagManager.matches("GobblinService.testGroup.testFlow.RunningStatus", mock(Metric.class)));
    Assert.assertTrue(metricNameRegexForDagManager.matches("GobblinService.test..RunningStatus", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexForDagManager.matches("test3.RunningStatus", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexForDagManager.matches("GobblinService.test4RunningStatus", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexForDagManager.matches("GobblinServicetest5.RunningStatus", mock(Metric.class)));
  }
}
