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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TargetState;
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

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;

import static org.mockito.Mockito.eq;
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
  public void testOneJob() {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    Mockito.when(mockTaskDriver.getWorkflows()).thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinClusterManager", "GobblinYarnTaskRunner-1"));

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
  public void testTwoJobs() {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1", "job2"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));
    getJobContext(mockTaskDriver, ImmutableMap.of(3, "GobblinYarnTaskRunner-2"), "job2");

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2"));

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
  public void testTwoWorkflows()  {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    WorkflowConfig mockWorkflowConfig1 =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1", "job2"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    WorkflowConfig mockWorkflowConfig2 =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job3"), TaskState.IN_PROGRESS, TargetState.START, "workflow2");

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));
    getJobContext(mockTaskDriver, ImmutableMap.of(3, "GobblinYarnTaskRunner-2"), "job2");
    getJobContext(mockTaskDriver, ImmutableMap.of(4, "GobblinYarnTaskRunner-3"), "job3", ImmutableSet.of(4,5));

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList(
        "GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2","GobblinYarnTaskRunner-3"));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 5 containers requested and 3 workers in use
    assertContainerRequest(mockYarnService, 5,
        ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3"));
  }

  /**
   * Test for three workflows with one not in progress and one marked for delete.
   * The partitions for the workflow that is not in progress or is marked for delete should not be counted.
   */
  @Test
  public void testNotInProgressOrBeingDeleted()  {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    WorkflowConfig workflowInProgress = getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job-inProgress-1", "job-inProgress-2"), TaskState.IN_PROGRESS, TargetState.START, "workflowInProgress");
    WorkflowConfig workflowCompleted = getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job-complete-1"), TaskState.COMPLETED, TargetState.STOP, "workflowCompleted");
    WorkflowConfig workflowSetToBeDeleted = getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job-setToDelete-1"), TaskState.IN_PROGRESS, TargetState.DELETE, "workflowSetToBeDeleted");
    Mockito.when(mockTaskDriver.getWorkflows()).thenReturn(ImmutableMap.of(
        "workflowInProgress", workflowInProgress,
        "workflowCompleted", workflowCompleted,
        "workflowSetToBeDeleted", workflowSetToBeDeleted));

    getJobContext(mockTaskDriver, ImmutableMap.of(1, "GobblinYarnTaskRunner-1"), "job-inProgress-1",
        ImmutableSet.of(1,2));
    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-2"), "job-inProgress-2");
    getJobContext(mockTaskDriver, ImmutableMap.of(1, "GobblinYarnTaskRunner-3"), "job-setToDelete-1");
    getJobContext(mockTaskDriver, ImmutableMap.of(1, "GobblinYarnTaskRunner-4"), "job-complete-1",
        ImmutableSet.of(1, 5));

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(
        Arrays.asList("GobblinClusterManager",
            "GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3", "GobblinYarnTaskRunner-4"));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 3 containers requested and 4 workers in use
    assertContainerRequest(mockYarnService, 3,
        ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3", "GobblinYarnTaskRunner-4"));
  }

  /**
   * Test multiple partitions to one container
   */
  @Test
  public void testMultiplePartitionsPerContainer()  {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");

    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1");
    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinYarnTaskRunner-1"));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 2,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 1 container requested since 2 partitions and limit is 2 partitions per container. One worker in use.
    assertContainerRequest(mockYarnService, 1, ImmutableSet.of("GobblinYarnTaskRunner-1"));
  }

  @Test
  public void testOverprovision() {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinYarnTaskRunner-1"));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable1 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.2, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable1.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 1.2
    // so targetNumContainers = Ceil((2/1) * 1.2)) = 3.
    assertContainerRequest(mockYarnService, 3, ImmutableSet.of("GobblinYarnTaskRunner-1"));
    Mockito.reset(mockYarnService);

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable2 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            0.1, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable2.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 1.2
    // so targetNumContainers = Ceil((2/1) * 0.1)) = 1.
    assertContainerRequest(mockYarnService, 1, ImmutableSet.of("GobblinYarnTaskRunner-1"));

    Mockito.reset(mockYarnService);
    YarnAutoScalingManager.YarnAutoScalingRunnable runnable3 =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            6.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable3.run();

    // 3 containers requested to max and one worker in use
    // NumPartitions = 2, Partitions per container = 1 and overprovision = 6.0,
    // so targetNumContainers = Ceil((2/1) * 6.0)) = 12.
    assertContainerRequest(mockYarnService, 12, ImmutableSet.of("GobblinYarnTaskRunner-1"));
  }

  /**
   * Test suppressed exception
   */
  @Test
  public void testSuppressedException()  {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    Mockito.when(mockTaskDriver.getWorkflows()).thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinYarnTaskRunner-1"));

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
    assertContainerRequest(mockYarnService, 2, ImmutableSet.of("GobblinYarnTaskRunner-1"));
  }

  public void testMaxValueEvictingQueue()  {
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
  public void testInstanceIdleBeyondTolerance()  {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    WorkflowConfig mockWorkflowConfig = getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    Mockito.when(mockTaskDriver.getWorkflows()).thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig));

    // Having both partition assigned to single instance initially, in this case, GobblinYarnTaskRunner-2
    getJobContext(mockTaskDriver, ImmutableMap.of(1,"GobblinYarnTaskRunner-2", 2, "GobblinYarnTaskRunner-2"), "job1");

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(Arrays.asList("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2"));

    TestYarnAutoScalingRunnable runnable = new TestYarnAutoScalingRunnable(mockTaskDriver, mockYarnService,
        1, helixDataAccessor);

    runnable.run();

    // 2 containers requested and one worker in use, while the evaluation will hold for true if not set externally,
    // still tell YarnService there are two instances being used.
    assertContainerRequest(mockYarnService, 2, ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2"));

    // Set failEvaluation which simulates the "beyond tolerance" case.
    Mockito.reset(mockYarnService);
    runnable.setAlwaysTagUnused(true);
    runnable.run();

    assertContainerRequest(mockYarnService, 2, ImmutableSet.of("GobblinYarnTaskRunner-2"));
  }

  @Test
  public void testFlowsWithHelixTags() {
    YarnService mockYarnService = mock(YarnService.class);
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    WorkflowConfig mockWorkflowConfig1 =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job1", "job2"), TaskState.IN_PROGRESS, TargetState.START, "workflow1");
    WorkflowConfig mockWorkflowConfig2 =
        getWorkflowConfig(mockTaskDriver, ImmutableSet.of("job3"), TaskState.IN_PROGRESS, TargetState.START, "workflow2");
    Mockito.when(mockTaskDriver.getWorkflows())
        .thenReturn(ImmutableMap.of("workflow1", mockWorkflowConfig1, "workflow2", mockWorkflowConfig2));

    getJobContext(mockTaskDriver, ImmutableMap.of(2, "GobblinYarnTaskRunner-1"), "job1", ImmutableSet.of(1, 2));
    getJobContext(mockTaskDriver, ImmutableMap.of(3, "GobblinYarnTaskRunner-2"), "job2");
    getJobContext(mockTaskDriver, ImmutableMap.of(4, "GobblinYarnTaskRunner-3"), "job3", ImmutableSet.of(4, 5));

    JobConfig mockJobConfig3 = mock(JobConfig.class);
    Mockito.when(mockTaskDriver.getJobConfig("job3")).thenReturn(mockJobConfig3);
    String helixTag = "test-Tag1";
    Map<String, String> resourceMap = ImmutableMap.of(
        GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS, "512",
        GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES, "8"
    );
    Mockito.when(mockJobConfig3.getInstanceGroupTag()).thenReturn(helixTag);
    Mockito.when(mockJobConfig3.getJobCommandConfigMap()).thenReturn(resourceMap);

    HelixDataAccessor helixDataAccessor = getHelixDataAccessor(
        Arrays.asList("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3"));

    YarnAutoScalingManager.YarnAutoScalingRunnable runnable =
        new YarnAutoScalingManager.YarnAutoScalingRunnable(mockTaskDriver, mockYarnService, 1,
            1.0, noopQueue, helixDataAccessor, defaultHelixTag, defaultContainerMemory, defaultContainerCores);

    runnable.run();

    // 5 containers requested and 3 workers in use
    ArgumentCaptor<YarnContainerRequestBundle> argument = ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    assertContainerRequest(argument, mockYarnService, 5, ImmutableSet.of("GobblinYarnTaskRunner-1", "GobblinYarnTaskRunner-2", "GobblinYarnTaskRunner-3"));

    // Verify that 3 containers requested with default tag and resource setting,
    // while 2 with specific helix tag and resource requirement
    Map<String, Set<String>> resourceHelixTagMap = argument.getValue().getResourceHelixTagMap();
    Map<String, Resource> helixTagResourceMap = argument.getValue().getHelixTagResourceMap();
    Map<String, Integer> helixTagContainerCountMap = argument.getValue().getHelixTagContainerCountMap();

    Assert.assertEquals(resourceHelixTagMap.size(), 2);
    Assert.assertEquals(helixTagResourceMap.get(helixTag), Resource.newInstance(512, 8));
    Assert.assertEquals(helixTagResourceMap.get(defaultHelixTag), Resource.newInstance(defaultContainerMemory, defaultContainerCores));
    Assert.assertEquals((int) helixTagContainerCountMap.get(helixTag), 2);
    Assert.assertEquals((int) helixTagContainerCountMap.get(defaultHelixTag), 3);
  }

  private HelixDataAccessor getHelixDataAccessor(List<String> taskRunners) {
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));

    Mockito.when(helixDataAccessor.getChildValuesMap(Mockito.any())).thenReturn(
        taskRunners.stream().collect(Collectors.toMap((name) -> name, (name) -> new HelixProperty(""))));
    return helixDataAccessor;
  }

  private WorkflowConfig getWorkflowConfig(TaskDriver mockTaskDriver, ImmutableSet<String> jobNames,
      TaskState taskState, TargetState targetState, String workflowName) {
    WorkflowConfig mockWorkflowConfig1 = mock(WorkflowConfig.class);
    JobDag mockJobDag1 = mock(JobDag.class);

    Mockito.when(mockJobDag1.getAllNodes()).thenReturn(jobNames);
    Mockito.when(mockWorkflowConfig1.getJobDag()).thenReturn(mockJobDag1);
    Mockito.when(mockWorkflowConfig1.getTargetState()).thenReturn(targetState);

    WorkflowContext mockWorkflowContext1 = mock(WorkflowContext.class);
    Mockito.when(mockWorkflowContext1.getWorkflowState()).thenReturn(taskState);

    Mockito.when(mockTaskDriver.getWorkflowContext(workflowName)).thenReturn(mockWorkflowContext1);
    return mockWorkflowConfig1;
  }

  private JobContext getJobContext(TaskDriver mockTaskDriver, Map<Integer, String> assignedParticipantMap, String jobName) {
    return getJobContext(mockTaskDriver, assignedParticipantMap, jobName, assignedParticipantMap.keySet());
  }

  private JobContext getJobContext(
      TaskDriver mockTaskDriver,
      Map<Integer, String> assignedParticipantMap,
      String jobName,
      Set<Integer> partitionSet) {
    JobContext mockJobContext = mock(JobContext.class);
    Mockito.when(mockJobContext.getPartitionSet()).thenReturn(ImmutableSet.copyOf(partitionSet));
    for (Map.Entry<Integer, String> entry : assignedParticipantMap.entrySet()) {
      Mockito.when(mockJobContext.getAssignedParticipant(entry.getKey())).thenReturn(entry.getValue());
    }
    Mockito.when(mockTaskDriver.getJobContext(jobName)).thenReturn(mockJobContext);
    return mockJobContext;
  }

  private void assertContainerRequest(ArgumentCaptor<YarnContainerRequestBundle> argument, YarnService mockYarnService, int expectedNumberOfContainers,
      ImmutableSet<String> expectedInUseInstances) {
     ArgumentCaptor.forClass(YarnContainerRequestBundle.class);
    Mockito.verify(mockYarnService, times(1)).
        requestTargetNumberOfContainers(argument.capture(),
            eq(expectedInUseInstances));
    Assert.assertEquals(argument.getValue().getTotalContainers(), expectedNumberOfContainers);
  }

  private void assertContainerRequest(YarnService mockYarnService, int expectedNumberOfContainers,
      ImmutableSet<String> expectedInUseInstances) {
    assertContainerRequest(ArgumentCaptor.forClass(YarnContainerRequestBundle.class), mockYarnService, expectedNumberOfContainers, expectedInUseInstances);
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
