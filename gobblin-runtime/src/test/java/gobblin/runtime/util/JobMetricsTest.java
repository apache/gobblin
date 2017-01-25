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

package gobblin.runtime.util;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.metrics.event.JobEvent;
import gobblin.runtime.JobState;


@Test(groups = { "gobblin.runtime" })
public class JobMetricsTest {

  @Test
  public void testJobMetricsGet() {
    String jobName = "testJob";
    String jobId = "job_123";

    JobState jobState = new JobState(jobName, jobId);
    JobMetrics jobMetrics = JobMetrics.get(jobState);

    Assert.assertNotNull(jobMetrics.getMetricContext());

    List<Tag<?>> tags = jobMetrics.getMetricContext().getTags();
    Map<String, ?> tagMap = jobMetrics.getMetricContext().getTagMap();
    String contextId = tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME).toString();
    String contextName = tagMap.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME).toString();

    Assert.assertEquals(tagMap.size(), 4);
    Assert.assertEquals(tagMap.get(JobEvent.METADATA_JOB_ID), jobId);
    Assert.assertEquals(tagMap.get(JobEvent.METADATA_JOB_NAME), jobName);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME), contextName);

    // should get the original jobMetrics, can check by the id
    JobMetrics jobMetrics1 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics1.getMetricContext());

    tagMap = jobMetrics1.getMetricContext().getTagMap();
    Assert.assertEquals(tags.size(), 4);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME), contextName);

    // remove original jobMetrics, should create a new one
    GobblinMetricsRegistry.getInstance().remove(jobMetrics.getId());
    JobMetrics jobMetrics2 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics2.getMetricContext());

    tagMap = jobMetrics2.getMetricContext().getTagMap();
    Assert.assertEquals(tags.size(), 4);
    Assert.assertNotEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);
    Assert.assertNotEquals(tagMap.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME), contextName);
  }

  @Test
  public void testCustomTags() {

    Properties testProperties = new Properties();
    Tag<String> expectedPropertyTag = new Tag<>("key1", "value1");

    GobblinMetrics.addCustomTagToProperties(testProperties, expectedPropertyTag);
    State testState = new State(testProperties);
    List<Tag<?>> tags = GobblinMetrics.getCustomTagsFromState(testState);

    Assert.assertEquals(Iterables.getFirst(tags, null), expectedPropertyTag);

    Tag<String> expectedStateTag = new Tag<>("key2", "value2");
    GobblinMetrics.addCustomTagToState(testState, expectedStateTag);
    tags = GobblinMetrics.getCustomTagsFromState(testState);

    Assert.assertTrue(tags.containsAll(ImmutableList.of(expectedPropertyTag, expectedStateTag)));

  }

}
