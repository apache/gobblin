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

package gobblin.util;

import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link JobLauncherUtils}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.util"})
public class JobLauncherUtilsTest {

  private static final String JOB_NAME = "foo";
  private static final Pattern PATTERN = Pattern.compile("job_" + JOB_NAME + "_\\d+");
  private String jobId;

  @Test
  public void testNewJobId() {
    this.jobId = JobLauncherUtils.newJobId(JOB_NAME);
    Assert.assertTrue(PATTERN.matcher(this.jobId).matches());
  }

  @Test(dependsOnMethods = "testNewJobId")
  public void testNewTaskId() {
    Assert.assertEquals(JobLauncherUtils.newTaskId(this.jobId, 0), this.jobId.replace("job", "task") + "_0");
    Assert.assertEquals(JobLauncherUtils.newTaskId(this.jobId, 1), this.jobId.replace("job", "task") + "_1");
  }

  @Test(dependsOnMethods = "testNewJobId")
  public void testNewMultiTaskId() {
    Assert.assertEquals(JobLauncherUtils.newMultiTaskId(this.jobId, 0), this.jobId.replace("job", "multitask") + "_0");
    Assert.assertEquals(JobLauncherUtils.newMultiTaskId(this.jobId, 1), this.jobId.replace("job", "multitask") + "_1");
  }
}
