/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;


/**
 * Unit tests for {@link Tagged}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics"})
public class TaggedTest {

  private static final String JOB_ID_KEY = "job.id";
  private static final String JOB_ID = "TestJob-0";
  private static final String PROJECT_VERSION_KEY = "project.version";
  private static final int PROJECT_VERSION = 1;

  private Tagged tagged;

  @BeforeClass
  public void setUp() {
    this.tagged = new Tagged();
  }

  @Test
  public void testAddTags() {
    this.tagged.addTag(new Tag<String>(JOB_ID_KEY, JOB_ID));
    this.tagged.addTag(new Tag<Integer>(PROJECT_VERSION_KEY, PROJECT_VERSION));
  }

  @Test(dependsOnMethods = "testAddTags")
  public void testGetTags() {
    List<Tag<?>> tags = this.tagged.getTags();
    Assert.assertEquals(tags.size(), 2);
    Assert.assertEquals(tags.get(0).getKey(), JOB_ID_KEY);
    Assert.assertEquals(tags.get(0).getValue(), JOB_ID);
    Assert.assertEquals(tags.get(1).getKey(), PROJECT_VERSION_KEY);
    Assert.assertEquals(tags.get(1).getValue(), PROJECT_VERSION);
  }

  @Test(dependsOnMethods = "testAddTags")
  public void testMetricNamePrefix() {
    Assert.assertEquals(
        this.tagged.metricNamePrefix(false), MetricRegistry.name(JOB_ID, Integer.toString(PROJECT_VERSION)));
    Assert.assertEquals(
        this.tagged.metricNamePrefix(true),
        MetricRegistry.name(this.tagged.getTags().get(0).toString(), this.tagged.getTags().get(1).toString()));
  }
}
