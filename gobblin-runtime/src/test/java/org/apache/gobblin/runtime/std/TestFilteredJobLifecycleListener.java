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
package org.apache.gobblin.runtime.std;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState.RunningState;
import org.apache.gobblin.runtime.api.JobExecutionState;
import org.apache.gobblin.runtime.api.JobExecutionStateListener;
import org.apache.gobblin.runtime.api.JobLifecycleListener;
import org.apache.gobblin.runtime.api.JobSpec;

/**
 * Unit tests for {@link FilteredJobLifecycleListener}
 */
public class TestFilteredJobLifecycleListener {

  @Test public void testSimple() {
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob"));
    JobSpec js1_1 = JobSpec.builder("gobblin:/testSimple/job1").withVersion("1").withConfig(config).build();
    JobSpec js1_2 = JobSpec.builder("gobblin:/testSimple/job1").withVersion("2").withConfig(config).build();

    JobLifecycleListener mockListener = mock(JobLifecycleListener.class);

    FilteredJobLifecycleListener testListener =
        new FilteredJobLifecycleListener(JobSpecFilter.builder()
            .eqURI("gobblin:/testSimple/job1").eqVersion("2").build(),
            mockListener);

    JobExecutionState jss1_1 = new JobExecutionState(js1_1,
        JobExecutionUpdatable.createFromJobSpec(js1_1),
        Optional.<JobExecutionStateListener>absent());
    JobExecutionState jss1_2 = new JobExecutionState(js1_2,
        JobExecutionUpdatable.createFromJobSpec(js1_2),
        Optional.<JobExecutionStateListener>absent());

    testListener.onAddJob(js1_1);
    testListener.onDeleteJob(js1_1.getUri(), js1_1.getVersion());
    testListener.onUpdateJob(js1_1);;
    testListener.onStatusChange(jss1_1, RunningState.PENDING, RunningState.RUNNING);
    testListener.onStageTransition(jss1_1, "Stage1", "Stage2");
    testListener.onMetadataChange(jss1_1, "metaKey", "value1", "value2");

    testListener.onAddJob(js1_2);
    testListener.onDeleteJob(js1_2.getUri(), js1_2.getVersion());
    testListener.onUpdateJob(js1_2);
    testListener.onStatusChange(jss1_2, RunningState.RUNNING, RunningState.SUCCESSFUL);
    testListener.onStageTransition(jss1_2, "Stage1", "Stage2");
    testListener.onMetadataChange(jss1_2, "metaKey", "value1", "value2");

    verify(mockListener).onAddJob(eq(js1_2));
    verify(mockListener).onDeleteJob(eq(js1_2.getUri()),
                                             eq(js1_2.getVersion()));
    verify(mockListener).onUpdateJob(eq(js1_2));
    verify(mockListener).onStatusChange(eq(jss1_2), eq(RunningState.RUNNING),
                                        eq(RunningState.SUCCESSFUL));
    verify(mockListener).onStageTransition(eq(jss1_2), eq("Stage1"), eq("Stage2"));
    verify(mockListener).onMetadataChange(eq(jss1_2), eq("metaKey"), eq("value1"), eq("value2"));

    verify(mockListener, never()).onAddJob(eq(js1_1));
    verify(mockListener, never()).onDeleteJob(eq(js1_1.getUri()), eq(js1_1.getVersion()));
    verify(mockListener, never()).onUpdateJob(eq(js1_1));
    verify(mockListener, never()).onStatusChange(eq(jss1_1), eq(RunningState.RUNNING),
                                                 eq(RunningState.SUCCESSFUL));
    verify(mockListener, never()).onStatusChange(eq(jss1_1), eq(RunningState.PENDING),
                                                 eq(RunningState.RUNNING));
    verify(mockListener, never()).onStageTransition(eq(jss1_1), eq("Stage1"), eq("Stage2"));
    verify(mockListener, never()).onMetadataChange(eq(jss1_1), eq("metaKey"), eq("value1"), eq("value2"));
  }
}
