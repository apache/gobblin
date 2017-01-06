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

package gobblin.instrumented;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.Constructs;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.instrumented.extractor.InstrumentedExtractor;


@Test(groups = { "gobblin.core" })
public class InstrumentedTest {

  @Test
  public void testInstrumented() {
    GobblinMetrics gobblinMetrics = GobblinMetrics.get("parent.context");

    State state = new State();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    state.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, gobblinMetrics.getName());
    Instrumented instrumented = new Instrumented(state, InstrumentedExtractor.class);

    Assert.assertNotNull(instrumented.getMetricContext());
    Assert.assertTrue(instrumented.getMetricContext().getParent().isPresent());
    Assert.assertEquals(instrumented.getMetricContext().getParent().get(), gobblinMetrics.getMetricContext());

    Map<String, ?> tags = instrumented.getMetricContext().getTagMap();
    Map<String, String> expectedTags = new HashMap<>();
    expectedTags.put("construct", Constructs.EXTRACTOR.toString());
    expectedTags.put("class", InstrumentedExtractor.class.getCanonicalName());
    expectedTags.put(MetricContext.METRIC_CONTEXT_ID_TAG_NAME,
        tags.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME).toString());
    expectedTags.put(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME,
        tags.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME).toString());

    Assert.assertEquals(tags.size(), expectedTags.size());
    for (Map.Entry<String, ?> tag : tags.entrySet()) {
      Assert.assertTrue(expectedTags.containsKey(tag.getKey()));
      Assert.assertEquals(expectedTags.get(tag.getKey()), tag.getValue().toString());
    }
  }

}
