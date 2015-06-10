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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.metrics.GobblinMetricsRegistry;
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

    Map<String, String> expectedTags = new HashMap<String, String>();
    expectedTags.put("jobName", jobName);
    expectedTags.put("jobId", jobId);

    Assert.assertNotNull(jobMetrics.getMetricContext());

    List<Tag<?>> tags = jobMetrics.getMetricContext().getTags();
    Assert.assertEquals(tags.size(), 2);
    Assert.assertTrue(expectedTags.containsKey(tags.get(0).getKey()));
    Assert.assertEquals(tags.get(0).getValue(), expectedTags.get(tags.get(0).getKey()));
    Assert.assertTrue(expectedTags.containsKey(tags.get(1).getKey()));
    Assert.assertEquals(tags.get(1).getValue(), expectedTags.get(tags.get(1).getKey()));

    // should get the original jobMetrics, can check by the name
    JobMetrics jobMetrics1 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics1.getMetricContext());

    tags = jobMetrics1.getMetricContext().getTags();
    Assert.assertEquals(tags.size(), 2);
    Assert.assertTrue(expectedTags.containsKey(tags.get(0).getKey()));
    Assert.assertEquals(tags.get(0).getValue(), expectedTags.get(tags.get(0).getKey()));
    Assert.assertTrue(expectedTags.containsKey(tags.get(1).getKey()));
    Assert.assertEquals(tags.get(1).getValue(), expectedTags.get(tags.get(1).getKey()));

    // remove original jobMetrics, should create a new one
    GobblinMetricsRegistry.getInstance().remove(jobMetrics.getId());
    JobMetrics jobMetrics2 = JobMetrics.get(jobName + "_", jobId);
    Assert.assertNotNull(jobMetrics2.getMetricContext());

    tags = jobMetrics2.getMetricContext().getTags();
    expectedTags.put("jobName", jobName + "_");
    Assert.assertEquals(tags.size(), 2);
    Assert.assertTrue(expectedTags.containsKey(tags.get(0).getKey()));
    Assert.assertEquals(tags.get(0).getValue(), expectedTags.get(tags.get(0).getKey()));
    Assert.assertTrue(expectedTags.containsKey(tags.get(1).getKey()));
    Assert.assertEquals(tags.get(1).getValue(), expectedTags.get(tags.get(1).getKey()));
  }
}
