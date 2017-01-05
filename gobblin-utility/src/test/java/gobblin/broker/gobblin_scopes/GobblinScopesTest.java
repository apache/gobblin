/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.broker.gobblin_scopes;

import org.testng.Assert;
import org.testng.annotations.Test;


public class GobblinScopesTest {

  @Test
  public void test() {

    GobblinScopeInstance containerScope = new GobblinScopeInstance(GobblinScopeTypes.CONTAINER, "myContainer");
    Assert.assertEquals(containerScope.getScopeId(), "myContainer");

    JobScopeInstance jobScope = new JobScopeInstance("myJob", "job123");
    Assert.assertEquals(jobScope.getJobId(), "job123");
    Assert.assertEquals(jobScope.getJobName(), "myJob");

    TaskScopeInstance taskScope = new TaskScopeInstance("myTask");
    Assert.assertEquals(taskScope.getTaskId(), "myTask");

    try {
      new GobblinScopeInstance(GobblinScopeTypes.JOB, "myJob");
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected because should use JobScopeInstance
    }

    try {
      new GobblinScopeInstance(GobblinScopeTypes.TASK, "myJob");
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected because should use TaskScopeInstance
    }
  }

}