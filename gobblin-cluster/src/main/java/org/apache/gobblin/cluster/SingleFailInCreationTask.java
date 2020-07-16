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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import org.apache.gobblin.runtime.TaskCreationException;
import org.apache.gobblin.runtime.util.StateStores;


/**
 * A simple extension for {@link SingleTask} to directly throw exception for the case of task-creation failure.
 * We need this since Helix couldn't handle failure before startTask call as part of state transition and trigger
 * task reassignment.
 */
public class SingleFailInCreationTask extends SingleTask {
  public SingleFailInCreationTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores, Config dynamicConfig) {
    //Since this is a dummy task that is designed to fail immediately on run(), we skip fetching the job state.
    super(jobId, workUnitFilePath, jobStateFilePath, fs, taskAttemptBuilder, stateStores, dynamicConfig, true);
  }

  @Override
  public void run()
      throws IOException, InterruptedException {
    throw new TaskCreationException("Failing task directly due to fatal issue in task-creation");
  }
}
