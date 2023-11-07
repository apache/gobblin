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
package org.apache.gobblin.temporal.workflows.trackingevent.activity;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;

import static org.apache.gobblin.instrumented.GobblinMetricsKeys.CLASS_META;


/**
 * Explicit test for testing deser of EventSubmitter that checks for edge cases where the tests don't exist. This test
 * Cannot exist in {@link org.apache.gobblin.metrics} because it tests integration with the gobblin-core-base module.
 *
 * The reason to place this in the temporal module is because Jackson serialization is only truly critical for
 * {@link io.temporal.workflow.Workflow} and {@link io.temporal.activity.Activity}
 */
@Slf4j
public class EventSubmitterTemporalSerializationTest {
  private final ObjectMapper objectMapper = new ObjectMapper();
  @Test
  public void testGet() throws Exception {
    List<Tag<?>> tags = Arrays.asList(new Tag<String>("jobId", "stub"));
    State state = new State();
    state.setProp("someState", "stub");

    EventSubmitter eventSubmitter = TestEventSubmitter.builder()
      .callerClass(EventSubmitterTemporalSerializationTest.class)
      .state(state)
      .namespace("test")
      .tags(tags)
      .build().get();

    String bytes = objectMapper.writeValueAsString(eventSubmitter);
    Assert.assertTrue(bytes != null && !bytes.isEmpty());
    eventSubmitter = objectMapper.readValue(bytes, EventSubmitter.class);
    Assert.assertTrue(eventSubmitter.getTags().contains(tags.get(0)));
    Assert.assertEquals(eventSubmitter.getNamespace(), "test");

    Map<? extends String, String> tagsAsMap = Tag.toMap(Tag.tagValuesToString(eventSubmitter.getTags()));
    Assert.assertEquals(tagsAsMap.get(CLASS_META), this.getClass().getName());
    Assert.assertTrue(tagsAsMap.containsKey(MetricContext.METRIC_CONTEXT_ID_TAG_NAME));
    Assert.assertTrue(tagsAsMap.containsKey(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME));
  }

  /**
   * A POJO that can be used to serialize and deserialize an {@link EventSubmitter} for use in a {@link io.temporal.workflow.Workflow}
   */
  @Builder
  public static class TestEventSubmitter {
    Class callerClass;
    String namespace;
    State state;
    List<Tag<?>> tags;

    public EventSubmitter get() {
      MetricContext metricContext = Instrumented.getMetricContext(state, callerClass, tags);
      return new EventSubmitter.Builder(metricContext, namespace).build();
    }
  }
}
