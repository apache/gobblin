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

package org.apache.gobblin.runtime;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.mockito.Mockito;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.rest.LauncherTypeEnum;


@Test(groups = {"gobblin.runtime"})
public class JobExecutionEventSubmitterTest {

  private EventSubmitter mockEventSubmitter;
  private JobExecutionEventSubmitter jobExecutionEventSubmitter;

  @BeforeClass
  public void setUp() {
    this.mockEventSubmitter = mock(EventSubmitter.class);
    this.jobExecutionEventSubmitter = new JobExecutionEventSubmitter(this.mockEventSubmitter);
  }

  @Test
  public void testSubmitJobExecutionEvents() {
    JobState mockJobState = mock(JobState.class, Mockito.RETURNS_SMART_NULLS);
    when(mockJobState.getState()).thenReturn(JobState.RunningState.SUCCESSFUL);
    when(mockJobState.getLauncherType()).thenReturn(LauncherTypeEnum.$UNKNOWN);
    when(mockJobState.getTrackingURL()).thenReturn(Optional.<String> absent());

    TaskState mockTaskState1 = createMockTaskState();
    TaskState mockTaskState2 = createMockTaskState();

    when(mockJobState.getTaskStates()).thenReturn(Lists.newArrayList(mockTaskState1, mockTaskState2));

    this.jobExecutionEventSubmitter.submitJobExecutionEvents(mockJobState);
    verify(this.mockEventSubmitter, atLeastOnce()).submit(any(String.class), any(Map.class));
  }

  private TaskState createMockTaskState() {
    TaskState taskState = mock(TaskState.class, Mockito.RETURNS_SMART_NULLS);
    when(taskState.getWorkingState()).thenReturn(WorkUnitState.WorkingState.SUCCESSFUL);
    when(taskState.getTaskFailureException()).thenReturn(Optional.<String> absent());
    return taskState;
  }
}
