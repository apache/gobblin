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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.temporal.util.nesting.work.WorkflowAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.util.nesting.workflow.AbstractNestingExecWorkflowImpl;


@RunWith(PowerMockRunner.class)
@PrepareForTest(Workflow.class)
public class AbstractNestingExecWorkflowImplTest {

  @Mock
  private Workload<String> mockWorkload;

  @Mock
  private WorkflowAddr mockWorkflowAddr;

  @Mock
  private Workload.WorkSpan<String> mockWorkSpan;

  @Mock
  private Promise<Object> mockPromise;

  private AbstractNestingExecWorkflowImpl<String, Object> workflow;

  @BeforeClass
  public void setup() {
    // PowerMockito is required to mock static methods in the Workflow class
    Mockito.mockStatic(Workflow.class);
    Mockito.mockStatic(Async.class);
    Mockito.mockStatic(Promise.class);
    this.mockWorkload = Mockito.mock(Workload.class);
    this.mockWorkflowAddr = Mockito.mock(WorkflowAddr.class);
    this.mockWorkSpan = Mockito.mock(Workload.WorkSpan.class);
    this.mockPromise = Mockito.mock(Promise.class);

    workflow = new AbstractNestingExecWorkflowImpl<String, Object>() {
      @Override
      protected Promise<Object> launchAsyncActivity(String task) {
        return mockPromise;
      }
    };
  }

  @Test
  public void testPerformWorkload_NoWorkSpan() {
    // Arrange
    Mockito.when(mockWorkload.getSpan(Mockito.anyInt(), Mockito.anyInt())).thenReturn(Optional.empty());

    // Act
    int result = workflow.performWorkload(mockWorkflowAddr, mockWorkload, 0, 10, 5, Optional.empty());

    // Assert
    Assert.assertEquals(0, result);
    Mockito.verify(mockWorkload, Mockito.times(2)).getSpan(0, 5);
  }

  @Test
  public void testCalcPauseDurationBeforeCreatingSubTree_NoPause() {
    // Act
    Duration result = workflow.calcPauseDurationBeforeCreatingSubTree(50);

    // Assert
    Assert.assertEquals(Duration.ZERO, result);
  }

  @Test
  public void testCalcPauseDurationBeforeCreatingSubTree_PauseRequired() {
    // Act
    Duration result = workflow.calcPauseDurationBeforeCreatingSubTree(150);

    // Assert
    Assert.assertEquals(
        Duration.ofSeconds(AbstractNestingExecWorkflowImpl.NUM_SECONDS_TO_PAUSE_BEFORE_CREATING_SUB_TREE_DEFAULT),
        result);
  }

  @Test
  public void testConsolidateSubTreeGrandChildren() {
    // Act
    List<Integer> result = AbstractNestingExecWorkflowImpl.consolidateSubTreeGrandChildren(3, 10, 2);

    // Assert
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(Integer.valueOf(0), result.get(0));
    Assert.assertEquals(Integer.valueOf(0), result.get(1));
    Assert.assertEquals(Integer.valueOf(6), result.get(2));
  }

  @Test(expectedExceptions = AssertionError.class)
  public void testPerformWorkload_LaunchesChildWorkflows() {
    // Arrange
    Mockito.when(mockWorkload.getSpan(Mockito.anyInt(), Mockito.anyInt())).thenReturn(Optional.of(mockWorkSpan));
    Mockito.when(mockWorkSpan.getNumElems()).thenReturn(5);
    Mockito.when(mockWorkSpan.next()).thenReturn("task1");
    Mockito.when(mockWorkload.isIndexKnownToExceed(Mockito.anyInt())).thenReturn(false);

    // Mock the child workflow
    Mockito.when(Async.function(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.anyInt(), Mockito.any())).thenReturn(mockPromise);
    Mockito.when(mockPromise.get()).thenReturn(5);
    // Act
    int result = workflow.performWorkload(mockWorkflowAddr, mockWorkload, 0, 10, 5, Optional.empty());
  }
}
