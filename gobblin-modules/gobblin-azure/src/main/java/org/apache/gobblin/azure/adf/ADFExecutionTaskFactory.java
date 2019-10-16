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

package org.apache.gobblin.azure.adf;

import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;
import org.apache.gobblin.task.HttpExecutionTask;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

/**
 * A {@link TaskFactory} that creates an implementation of {@link AbstractADFPipelineExecutionTask}.
 * There is no data publish phase for this task, so this factory uses a {@link NoopPublisher}.
 */
public class ADFExecutionTaskFactory implements TaskFactory {
  @Override
  public TaskIFace createTask(TaskContext taskContext) {
    String className = taskContext.getTaskState().getProp(HttpExecutionTask.CONF_HTTPTASK_CLASS);
    return GobblinConstructorUtils.invokeConstructor(HttpExecutionTask.class, className, taskContext);
  }

  @Override
  public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
    return new NoopPublisher(datasetState);
  }
}
