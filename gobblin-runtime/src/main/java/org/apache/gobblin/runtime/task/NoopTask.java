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

import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * A task that does nothing. Usually used for transferring state from one job to the next.
 */
public class NoopTask extends BaseAbstractTask {

  /**
   * @return A {@link WorkUnit} that will run a {@link NoopTask}.
   */
  public static WorkUnit noopWorkunit() {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, Factory.class);
    return workUnit;
  }

  /**
   * The factory for a {@link NoopTask}.
   */
  public static class Factory implements TaskFactory {
    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new NoopTask(taskContext);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      return new NoopPublisher(datasetState);
    }
  }

  private NoopTask(TaskContext taskContext) {
    super(taskContext);
  }

}
