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

package gobblin.compaction.source;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.suite.CompactionSuite;
import gobblin.compaction.suite.CompactionSuiteUtils;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.Dataset;
import gobblin.runtime.TaskContext;
import gobblin.runtime.task.FailedTask;
import gobblin.runtime.task.TaskIFace;

/**
 * A task which throws an exception when executed
 * The exception contains dataset information
 */
@Slf4j
public class CompactionFailedTask extends FailedTask {
  protected final CompactionSuite suite;
  protected final Dataset dataset;

  public CompactionFailedTask (TaskContext taskContext) {
    super(taskContext);
    this.suite = CompactionSuiteUtils.getCompactionSuiteFactory (taskContext.getTaskState()).
        createSuite(taskContext.getTaskState());
    this.dataset = this.suite.load(taskContext.getTaskState());
  }

  @Override
  public void run() {
    log.error ("Compaction job for " + dataset.datasetURN() + " is failed. Please take a look");
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  public static class CompactionFailedTaskFactory extends FailedTaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new CompactionFailedTask (taskContext);
    }
  }
}
