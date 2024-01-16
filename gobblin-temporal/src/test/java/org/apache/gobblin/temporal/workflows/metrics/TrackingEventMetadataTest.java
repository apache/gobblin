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
package org.apache.gobblin.temporal.workflows.metrics;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;

import static org.apache.gobblin.instrumented.GobblinMetricsKeys.CLASS_META;

public class TrackingEventMetadataTest{
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String NAMESPACE = "test-namespace";
  @Test
  public void testCreateEventSubmitter()
      throws IOException {
    List<Tag<?>> tags = Arrays.asList(new Tag<>("jobId", "stub"));
    State state = new State();
    state.setProp("someState", "stub");
    MetricContext metricContext = Instrumented.getMetricContext(state, getClass(), tags);
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(metricContext, NAMESPACE).build();
    TrackingEventMetadata trackingEventMetadata = new TrackingEventMetadata(eventSubmitter);
    byte[] asBytes = OBJECT_MAPPER.writeValueAsBytes(trackingEventMetadata);

    TrackingEventMetadata deserEventMetadata = OBJECT_MAPPER.readValue(asBytes, TrackingEventMetadata.class);
    EventSubmitter deserEventSubmitter = deserEventMetadata.createEventSubmitter();
    Assert.assertTrue(deserEventSubmitter.getTags().contains(tags.get(0)));
    Assert.assertEquals(deserEventSubmitter.getNamespace(), NAMESPACE);

    Map<? extends String, String> tagMap = Tag.toMap(Tag.tagValuesToString(eventSubmitter.getTags()));
    Assert.assertEquals(tagMap.get(CLASS_META), this.getClass().getName());
    Assert.assertTrue(tagMap.containsKey(MetricContext.METRIC_CONTEXT_ID_TAG_NAME));
    Assert.assertTrue(tagMap.containsKey(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME));
  }
}
