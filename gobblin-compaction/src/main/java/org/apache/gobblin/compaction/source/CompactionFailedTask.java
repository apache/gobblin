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

package org.apache.gobblin.compaction.source;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.suite.CompactionSuite;
import org.apache.gobblin.compaction.suite.CompactionSuiteUtils;
import org.apache.gobblin.compaction.verify.CompactionVerifier;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.FailedTask;
import org.apache.gobblin.runtime.task.TaskIFace;

/**
 * A task which throws an exception when executed
 * The exception contains dataset information
 */
@Slf4j
public class CompactionFailedTask extends FailedTask {
  protected final CompactionSuite suite;
  protected final Dataset dataset;
  protected final String failedReason;

  public CompactionFailedTask (TaskContext taskContext) {
    super(taskContext);
    this.suite = CompactionSuiteUtils.getCompactionSuiteFactory (taskContext.getTaskState()).
        createSuite(taskContext.getTaskState());
    this.dataset = this.suite.load(taskContext.getTaskState());
    this.failedReason = taskContext.getTaskState().getProp(CompactionVerifier.COMPACTION_VERIFICATION_FAIL_REASON);
  }

  @Override
  public void run() {
    log.error ("Compaction job for " + dataset.datasetURN() + " is failed because of {}", failedReason);
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  public static class CompactionFailedTaskFactory extends FailedTaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new CompactionFailedTask (taskContext);
    }
  }
}
