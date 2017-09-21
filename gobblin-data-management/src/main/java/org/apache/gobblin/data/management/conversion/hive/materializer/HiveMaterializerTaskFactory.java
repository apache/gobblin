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

package org.apache.gobblin.data.management.conversion.hive.materializer;

import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;

/**
 * A {@link TaskFactory} that runs a {@link HiveMaterializer} task.
 * This factory is intended to publish data in the task directly, and
 * uses a {@link NoopPublisher}.
 */
public class HiveMaterializerTaskFactory implements TaskFactory {
  @Override
  public TaskIFace createTask(TaskContext taskContext) {
    try {
      return new HiveMaterializer(taskContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
    return new NoopPublisher(datasetState);
  }
}
