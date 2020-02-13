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

import org.apache.gobblin.runtime.util.StateStores;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * An taskRunner for in-memory {@link SingleTask} that can be switched to run a meant-to-failed task.
 */
public class InMemorySingleTaskRunner extends SingleTaskRunner {
  public InMemorySingleTaskRunner(String clusterConfigFilePath, String jobId, String workUnitFilePath) {
    super(clusterConfigFilePath, jobId, workUnitFilePath);
  }

  @Override
  protected SingleTask createTaskAttempt(TaskAttemptBuilder taskAttemptBuilder, FileSystem fs, StateStores stateStores,
      Path jobStateFilePath, boolean fail) {
    return !fail ? new InMemoryWuSingleTask(this.jobId, new Path(this.workUnitFilePath), jobStateFilePath, fs,
        taskAttemptBuilder, stateStores, GobblinClusterUtils.getDynamicConfig(this.clusterConfig))
        : new InMemoryWuFailedSingleTask(this.jobId, new Path(this.workUnitFilePath), jobStateFilePath, fs,
            taskAttemptBuilder, stateStores, GobblinClusterUtils.getDynamicConfig(this.clusterConfig));
  }
}
