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

package org.apache.gobblin.azure.adf;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.task.TaskIFace;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.task.HttpExecutionTask;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
@PrepareForTest({TaskMetrics.class})
public class ADFExecutionTaskFactoryTest extends PowerMockTestCase {
  @Test
  public void testCreateTask() {
    WorkUnit wu = WorkUnit.createEmpty();
    WorkUnitState wuState = new WorkUnitState(wu, new State());
    TaskState taskState = new TaskState(wuState);
    taskState.setProp(HttpExecutionTask.CONF_HTTPTASK_CLASS, "org.apache.gobblin.azure.adf.ADFPipelineExecutionTask");

    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getTaskState()).thenReturn(taskState);

    PowerMockito.mockStatic(TaskMetrics.class);

    TaskMetrics taskMetrics = mock(TaskMetrics.class);
    when(taskMetrics.getMetricContext()).thenReturn(null);
    when(TaskMetrics.get(taskState)).thenReturn(taskMetrics);


    TaskIFace task = new ADFExecutionTaskFactory().createTask(taskContext);
    Assert.assertTrue(task instanceof ADFPipelineExecutionTask);
  }

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }
}
