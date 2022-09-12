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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;


/**
 * Unit tests for {@link YarnAutoScalingManager}
 */
@Test(groups = { "gobblin.yarn" })
public class YarnAutoScalingManagerTest {
  // A queue within size == 1 and upperBound == "infinite" should not impact on the execution.
  private final static YarnAutoScalingManager.SlidingWindowReservoir noopQueue =
      new YarnAutoScalingManager.SlidingWindowReservoir(1, Integer.MAX_VALUE);
  private final static int defaultContainerMemory = 1024;
  private final static int defaultContainerCores = 2;
  private final static String defaultHelixTag = "DefaultHelixTag";
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
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    // 2 containers requested and one worker in use
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 2);
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
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("GobblinYarnTaskRunner-2");
    Mockito.when(mockTaskDriver.getJobContext("job2")).thenReturn(mockJobContext2);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty(""),
            "GobblinYarnTaskRunner-2", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 3 containers requested and 2 workers in use
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 3);
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
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("GobblinYarnTaskRunner-2");
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
    Mockito.when(mockJobContext3.getAssignedParticipant(4)).thenReturn("GobblinYarnTaskRunner-3");
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty(""),
            "GobblinYarnTaskRunner-2", new HelixProperty(""),
            "GobblinYarnTaskRunner-3", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 5 containers requested and 3 workers in use
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 5);
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
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("GobblinYarnTaskRunner-2");
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
    Mockito.when(mockJobContext3.getAssignedParticipant(4)).thenReturn("GobblinYarnTaskRunner-3");
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty(""),
            "GobblinYarnTaskRunner-2", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 3 containers requested and 2 workers in use
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 3);
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
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 2,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 1 container requested since 2 partitions and limit is 2 partitions per container. One worker in use.
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 1);
  }

  @Test
  public void testOverprovision() {
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
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable1 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.2, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable1.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 1.2
    // so targetNumContainers = Ceil((2/1) * 1.2)) = 3.
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 3);

    Mockito.reset(mockYarnService);
    YarnAutoScalingManager.YarnAutoScalingRunnable runnable2 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            0.1, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable2.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 1.2
    // so targetNumContainers = Ceil((2/1) * 0.1)) = 1.
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 1);

    Mockito.reset(mockYarnService);
    YarnAutoScalingManager.YarnAutoScalingRunnable runnable3 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            6.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable3.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 6.0,
    // so targetNumContainers = Ceil((2/1) * 6.0)) = 12.
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 12);
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
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty("")));

    TestYarnAutoScalingRunnable runnable =
        new TestYarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1, helixDataAccessor);

    runnable.setRaiseException(true);
    runnable.run();
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(0)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));

    Mockito.reset(mockYarnService);
    runnable.setRaiseException(false);
    runnable.run();
    // 2 container requested
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 2);
  }

  public void testMaxValueEvictingQueue() throws Exception {
    Resource resource = Resource.newInstance(16, 1);
    YarnAutoScalingManager.SlidingWindowReservoir window = new YarnAutoScalingManager.SlidingWindowReservoir(3, 10);
    // Normal insertion with eviction of originally largest value
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(3, resource));
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(1, resource));
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(2, resource));
    // Now it contains [3,1,2]
    Assert.assertEquals(window.getMax().getTotalContainers(), 3);
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(1, resource));
    // Now it contains [1,2,1]
    Assert.assertEquals(window.getMax().getTotalContainers(), 2);
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(5, resource));
    Assert.assertEquals(window.getMax().getTotalContainers(), 5);
    // Now it contains [2,1,5]
    window.add(GobblinYarnTestUtils.createYarnContainerRequest(11, resource));
    // Still [2,1,5] as 11 > 10 thereby being rejected.
    Assert.assertEquals(window.getMax().getTotalContainers(), 5);
  }

  /**
   * Test the scenarios when an instance in cluster has no participants assigned for too long and got tagged as the
   * candidate for scaling-down.
   */
  @Test
  public void testInstanceIdleBeyondTolerance() throws IOException {
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

    // Having both partition assigned to single instance initially, in this case, GobblinYarnTaskRunner-2
    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(1), Integer.valueOf(2)));
    Mockito.when(mockJobContext.getAssignedParticipant(1)).thenReturn("GobblinYarnTaskRunner-2");
    Mockito.when(mockJobContext.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-2");

    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext);

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty(""),
            "GobblinYarnTaskRunner-2", new HelixProperty("")));

    TestYarnAutoScalingRunnable runnable = new TestYarnAutoScalingRunnable(mockTaskDriver, mockYarnService,
        1, helixDataAccessor);

    runnable.run();

    // 2 containers requested and one worker in use, while the evaluation will hold for true if not set externally,
    // still tell YarnService there are two instances being used.
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 2);

    // Set failEvaluation which simulates the "beyond tolerance" case.
    Mockito.reset(mockYarnService);
    runnable.setAlwaysTagUnused(true);
    runnable.run();

    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-2")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 2);
  }

  @Test
  public void testFlowsWithHelixTags() {
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
    Mockito.when(mockJobContext1.getAssignedParticipant(2)).thenReturn("GobblinYarnTaskRunner-1");
    Mockito.when(mockTaskDriver.getJobContext("job1")).thenReturn(mockJobContext1);

    JobContext mockJobContext2 = mock(JobContext.class);
    Mockito.when(mockJobContext2.getPartitionSet())
        .thenReturn(ImmutableSet.of(Integer.valueOf(3)));
    Mockito.when(mockJobContext2.getAssignedParticipant(3)).thenReturn("GobblinYarnTaskRunner-2");
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
    Mockito.when(mockJobContext3.getAssignedParticipant(4)).thenReturn("GobblinYarnTaskRunner-3");
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);
    JobConfig mockJobConfig3 = mock(JobConfig.class);
    String helixTag = "test-Tag1";
    Map<String, String> resourceMap = new HashMap<>();
    resourceMap.put(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS, "512");
    resourceMap.put(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES, "8");
    Mockito.when(mockJobConfig3.getInstanceGroupTag()).thenReturn(helixTag);
    Mockito.when(mockJobConfig3.getJobCommandConfigMap()).thenReturn(resourceMap);
    Mockito.when(mockTaskDriver.getJobContext("job3")).thenReturn(mockJobContext3);
    Mockito.when(mockTaskDriver.getJobConfig("job3")).thenReturn(mockJobConfig3);
    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any()))
        .thenReturn(ImmutableMap.of("GobblinYarnTaskRunner-1", new HelixProperty(""),
            "GobblinYarnTaskRunner-2", new HelixProperty(""),
            "GobblinYarnTaskRunner-3", new HelixProperty("")));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 5 containers requested and 3 workers in use
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3")));
    Assert.assertEquals(argument.getValue().getTotalContainers(), 5);
    Map<String, Set<String>> resourceHelixTagMap = argument.getValue().getResourceHelixTagMap();
    Map<String, Resource> helixTagResourceMap = argument.getValue().getHelixTagResourceMap();
    Map<String, Integer> helixTagContainerCountMap = argument.getValue().getHelixTagContainerCountMap();

    // Verify that 3 containers requested with default tag and resource setting,
    // while 2 with specific helix tag and resource requirement
    Assert.assertEquals(resourceHelixTagMap.size(), 2);
    Assert.assertEquals(helixTagResourceMap.get(helixTag), Resource.newInstance(512, 8));
    Assert.assertEquals(helixTagResourceMap.get(defaultHelixTag), Resource.newInstance(defaultContainerMemory, defaultContainerCores));
    Assert.assertEquals((int) helixTagContainerCountMap.get(helixTag), 2);
    Assert.assertEquals((int) helixTagContainerCountMap.get(defaultHelixTag), 3);

  }

  private static class TestYarnAutoScalingRunnable extends YarnAutoScalingManager.YarnAutoScalingRunnable {
    boolean raiseException = false;
    boolean alwaysUnused = false;

    public TestYarnAutoScalingRunnable(TaskDriver taskDriver, YarnService yarnService, int partitionsPerContainer,
        HelixDataAccessor helixDataAccessor) {
      super(taskDriver, yarnService, partitionsPerContainer, 1.0,
          noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);
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

    void setAlwaysTagUnused(boolean alwaysUnused) {
      this.alwaysUnused = alwaysUnused;
    }

    @Override
    boolean isInstanceUnused(String participant) {
      return alwaysUnused || super.isInstanceUnused(participant);
    }
  }
}