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
package org.apache.gobblin.runtime.task;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * A task which raise an exception when run
 */
public class FailedTask extends BaseAbstractTask {
  public FailedTask (TaskContext taskContext) {
    super(taskContext);
  }

  public void commit() {
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  @Override
  public void run() {
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  public static class FailedWorkUnit extends WorkUnit {

    public FailedWorkUnit () {
      super ();
      TaskUtils.setTaskFactoryClass(this, FailedTaskFactory.class);
    }
  }

  public static class FailedTaskFactory implements TaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new FailedTask(taskContext);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      return new NoopPublisher(datasetState);
    }
  }
}
