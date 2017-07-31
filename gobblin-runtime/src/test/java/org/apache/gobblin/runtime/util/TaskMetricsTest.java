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

package org.apache.gobblin.runtime.util;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.WorkUnit;


@Test(groups = {"gobblin.runtime"})
public class TaskMetricsTest {

  @Test
  public void testTaskGetMetrics() {

    String jobId = "job_456";
    String taskId = "task_456";
    String jobName = "jobName";

    JobState jobState = new JobState(jobName, jobId);
    JobMetrics jobMetrics = JobMetrics.get(jobState);

    State props = new State();
    props.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
    props.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);

    SourceState sourceState = new SourceState(props, new ArrayList<WorkUnitState>());
    WorkUnit workUnit = new WorkUnit(sourceState, null);
    WorkUnitState workUnitState = new WorkUnitState(workUnit);
    TaskState taskState = new TaskState(workUnitState);

    TaskMetrics taskMetrics = new TaskMetrics(taskState);

    Assert.assertNotNull(taskMetrics.getMetricContext());
    Assert.assertTrue(taskMetrics.getMetricContext().getParent().isPresent());
    Assert.assertEquals(taskMetrics.getMetricContext().getParent().get(), jobMetrics.getMetricContext());

  }

}
