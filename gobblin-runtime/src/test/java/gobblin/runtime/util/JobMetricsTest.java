/*
 *
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import gobblin.configuration.State;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.JobState;

@Test(groups = {"gobblin.runtime"})
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

    Assert.assertEquals(tagMap.size(), 4);
    Assert.assertEquals(tagMap.get("jobId"), jobId);
    Assert.assertEquals(tagMap.get("jobName"), jobName);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);

    // should get the original jobMetrics, can check by the id
    JobMetrics jobMetrics1 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics1.getMetricContext());

    tagMap = jobMetrics1.getMetricContext().getTagMap();
    Assert.assertEquals(tags.size(), 4);
    Assert.assertEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);

    // remove original jobMetrics, should create a new one
    GobblinMetricsRegistry.getInstance().remove(jobMetrics.getId());
    JobMetrics jobMetrics2 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics2.getMetricContext());

    tagMap = jobMetrics2.getMetricContext().getTagMap();
    Assert.assertEquals(tags.size(), 4);
    Assert.assertNotEquals(tagMap.get(MetricContext.METRIC_CONTEXT_ID_TAG_NAME), contextId);
  }

  @Test
  public void testClusterIdentifierTags() {
    try {

      String expectedClusterIdentifier  = InetAddress.getLocalHost().getHostName();
      JobMetrics jobMetrics = buildTestJobMetrics();

      Assert.assertNotNull(jobMetrics.getMetricContext());

      Map<String, ?> tagMap = jobMetrics.getMetricContext().getTagMap();
      Assert.assertEquals(tagMap.get("clusterIdentifier"), expectedClusterIdentifier);
    } catch (UnknownHostException e) {
      //Ignore test
    }
  }

  @Test
  public void testCustomTags() {

    Properties testProperties = new Properties();
    Tag<String> expectedPropertyTag = new Tag<String>("key1", "value1");

    JobMetrics.addCustomTagToProperties(testProperties, expectedPropertyTag);
    State testState = new State(testProperties);
    List<Tag<?>> tags = JobMetrics.getCustomTagsFromState(testState);

    Assert.assertEquals(Iterables.getFirst(tags, null), expectedPropertyTag);

    Tag<String> expectedStateTag = new Tag<String>("key2", "value2");
    JobMetrics.addCustomTagToState(testState, expectedStateTag);
    tags = JobMetrics.getCustomTagsFromState(testState);

    Assert.assertTrue(tags.containsAll(ImmutableList.of(expectedPropertyTag, expectedStateTag)));

  }

  private JobMetrics buildTestJobMetrics() {
    String jobName = "testJob";
    String jobId = "job_123";

    JobState jobState = new JobState(jobName, jobId);
    return JobMetrics.get(jobState);

  }
}
