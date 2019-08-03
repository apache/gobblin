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

package org.apache.gobblin.yarn;

import java.io.IOException;

import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;


/**
 * Unit tests for {@link YarnAutoScalingManager}
 */
@Test(groups = { "gobblin.yarn" })
public class YarnAutoScalingManagerTest {
  /**
   * Test for one workflow with one job
   */
  @Test
  public void testOneJob() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("worker1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 10);

    runnable.run();

    // 2 containers requested and one worker in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(2, ImmutableSet.of("worker1"));
  }

  /**
   * Test for one workflow with two jobs
   */
  @Test
  public void testTwoJobs() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1", "job2"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext1 = mock(JobContext.class);
    Mockito.when(mockJobContext1.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("worker1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("worker2");
    Mockito.when(mockTaskDriver.getJobContext("job2")).thenReturn(mockJobContext2);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 10);

    runnable.run();

    // 3 containers requested and 2 workers in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(3, ImmutableSet.of("worker1", "worker2"));
  }

  /**
   * Test for two workflows
   */
  @Test
  public void testTwoWorkflows() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    WorkflowConfig mockWorkflowConfig1 = mock(WorkflowConfig.class);
    JobDag mockJobDag1 = mock(JobDag.class);

    Mockito.when(mockJobDag1.getAllNodes()).thenReturn(ImmutableSet.of("job1", "job2"));
    Mockito.when(mockWorkflowConfig1.getJobDag()).thenReturn(mockJobDag1);

    WorkflowContext mockWorkflowContext1 = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext1.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext1);

    JobContext mockJobContext1 = mock(JobContext.class);
    Mockito.when(mockJobContext1.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("worker1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("worker2");
    Mockito.when(mockTaskDriver.getJobContext("job2")).thenReturn(mockJobContext2);

    WorkflowConfig mockWorkflowConfig2 = mock(WorkflowConfig.class);
    JobDag mockJobDag2 = mock(JobDag.class);

    Mockito.when(mockJobDag2.getAllNodes()).thenReturn(ImmutableSet.of("job3"));
    Mockito.when(mockWorkflowConfig2.getJobDag()).thenReturn(mockJobDag2);

    WorkflowContext mockWorkflowContext2 = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext2.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow2")).thenReturn(mockWorkflowContext2);

    JobContext mockJobContext3 = mock(JobContext.class);
    Mockito.when(mockJobContext3.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(4), Integer.valueOf(5)));
    Mockito.when(mockJobContext3.getAssignedParticipant(4)).thenReturn("worker3");
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 10);

    runnable.run();

    // 5 containers requested and 3 workers in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(5,
        ImmutableSet.of("worker1", "worker2", "worker3"));
  }

  /**
   * Test for two workflows with one not in progress.
   * The partitions for the workflow that is not in progress should not be counted.
   */
  @Test
  public void testNotInProgress() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    WorkflowConfig mockWorkflowConfig1 = mock(WorkflowConfig.class);
    JobDag mockJobDag1 = mock(JobDag.class);

    Mockito.when(mockJobDag1.getAllNodes()).thenReturn(ImmutableSet.of("job1", "job2"));
    Mockito.when(mockWorkflowConfig1.getJobDag()).thenReturn(mockJobDag1);

    WorkflowContext mockWorkflowContext1 = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext1.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext1);

    JobContext mockJobContext1 = mock(JobContext.class);
    Mockito.when(mockJobContext1.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("worker1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("worker2");
    Mockito.when(mockTaskDriver.getJobContext("job2")).thenReturn(mockJobContext2);

    WorkflowConfig mockWorkflowConfig2 = mock(WorkflowConfig.class);
    JobDag mockJobDag2 = mock(JobDag.class);

    Mockito.when(mockJobDag2.getAllNodes()).thenReturn(ImmutableSet.of("job3"));
    Mockito.when(mockWorkflowConfig2.getJobDag()).thenReturn(mockJobDag2);

    WorkflowContext mockWorkflowContext2 = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext2.getWorkflowState()).thenReturn(TaskState.COMPLETED);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow2")).thenReturn(mockWorkflowContext2);

    JobContext mockJobContext3 = mock(JobContext.class);
    Mockito.when(mockJobContext3.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(4), Integer.valueOf(5)));
    Mockito.when(mockJobContext3.getAssignedParticipant(4)).thenReturn("worker3");
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 10);

    runnable.run();

    // 3 containers requested and 2 workers in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(3,
        ImmutableSet.of("worker1", "worker2"));
  }

  /**
   * Test multiple partitions to one container
   */
  @Test
  public void testMultiplePartitionsPerContainer() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("worker1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 2, 1, 10);

    runnable.run();

    // 1 container requested since 2 partitions and limit is 2 partitions per container. One worker in use.
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(1, ImmutableSet.of("worker1"));
  }


  /**
   * Test min containers
   */
  @Test
  public void testMinContainers() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("worker1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 5, 10);

    runnable.run();

    // 5 containers requested due to min and one worker in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(5, ImmutableSet.of("worker1"));
  }


  /**
   * Test max containers
   */
  @Test
  public void testMaxContainers() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("worker1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 1);

    runnable.run();

    // 1 container requested to max and one worker in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(1, ImmutableSet.of("worker1"));
  }

  /**
   * Test suppressed exception
   */
  @Test
  public void testSuppressedException() throws IOException {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = mock(WorkflowConfig.class);
    JobDag mockJobDag = mock(JobDag.class);

    Mockito.when(mockJobDag.getAllNodes()).thenReturn(ImmutableSet.of("job1"));
    Mockito.when(mockWorkflowConfig.getJobDag()).thenReturn(mockJobDag);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    WorkflowContext mockWorkflowContext = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext.getWorkflowState()).thenReturn(TaskState.IN_PROGRESS);

    Mockito.when(mockTaskDriver.getWorkflowContext("workflow1")).thenReturn(mockWorkflowContext);

    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("worker1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    TestYarnAutoScalingRunnable runnable =
        new TestYarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, 1, 1);

    runnable.setRaiseException(true);
    runnable.run();
    Mockito.verify(mockYarnService, times(0)).requestTargetNumberOfContainers(1, ImmutableSet.of("worker1"));

    runnable.setRaiseException(false);
    runnable.run();
    // 1 container requested to max and one worker in use
    Mockito.verify(mockYarnService, times(1)).requestTargetNumberOfContainers(1, ImmutableSet.of("worker1"));
  }

  private static class TestYarnAutoScalingRunnable extends YarnAutoScalingManager.YarnAutoScalingRunnable {
    boolean raiseException = false;

    public TestYarnAutoScalingRunnable(TaskDriver taskDriver, YarnService yarnService, int partitionsPerContainer,
        int minContainers, int maxContainers) {
      super(taskDriver, yarnService, partitionsPerContainer, minContainers, maxContainers);
    }

    @Override
    void runInternal() {
      if (this.raiseException) {
        throw new RuntimeException("Test exception");
      } else {
        super.runInternal();
      }
    }

    void setRaiseException(boolean raiseException) {
      this.raiseException = raiseException;
    }
  }
}
