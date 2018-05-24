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

package org.apache.gobblin.cluster;

import java.util.Iterator;

import com.google.common.base.Optional;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.source.workunit.WorkUnit;


public class TaskAttemptBuilder {
  private final TaskStateTracker _taskStateTracker;
  private final TaskExecutor _taskExecutor;
  private String _containerId;
  private StateStore<TaskState> _taskStateStore;

  public TaskAttemptBuilder(TaskStateTracker taskStateTracker, TaskExecutor taskExecutor) {
    _taskStateTracker = taskStateTracker;
    _taskExecutor = taskExecutor;
  }

  public TaskAttemptBuilder setContainerId(String containerId) {
    _containerId = containerId;
    return this;
  }

  public TaskAttemptBuilder setTaskStateStore(StateStore<TaskState> taskStateStore) {
    _taskStateStore = taskStateStore;
    return this;
  }

  public GobblinMultiTaskAttempt build(Iterator<WorkUnit> workUnits, String jobId, JobState jobState,
      SharedResourcesBroker<GobblinScopeTypes> jobBroker) {
    GobblinMultiTaskAttempt attemptInstance =
        new GobblinMultiTaskAttempt(workUnits, jobId, jobState, _taskStateTracker, _taskExecutor,
            Optional.fromNullable(_containerId), Optional.fromNullable(_taskStateStore), jobBroker);

    return attemptInstance;
  }
}
