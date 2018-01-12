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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import org.apache.gobblin.configuration.ConfigurationKeys;


public class SingleHelixTask implements Task {

  private static final Logger logger = LoggerFactory.getLogger(SingleHelixTask.class);

  private final String jobId;
  private final String jobName;

  private final Process taskProcess;

  SingleHelixTask(final SingleTaskLauncher launcher, final Map<String, String> configMap)
      throws IOException {
    this.jobName = configMap.get(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = configMap.get(ConfigurationKeys.JOB_ID_KEY);
    final Path workUnitFilePath =
        Paths.get(configMap.get(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH));
    logger.info(String
        .format("Launching a single task process. job name: %s. job id: %s", this.jobName,
            this.jobId));
    this.taskProcess = launcher.launch(this.jobId, workUnitFilePath);
  }

  @Override
  public TaskResult run() {
    try {
      logger.info(String
          .format("Waiting for a single task process to finish. job name: %s. job id: %s",
              this.jobName, this.jobId));
      int exitCode = this.taskProcess.waitFor();
      if (exitCode == 0) {
        logger.info(String
            .format("Task process finished. job name: %s. job id: %s", this.jobName, this.jobId));
        return new TaskResult(TaskResult.Status.COMPLETED, "");
      } else {
        logger.warn(String
            .format("Task process failed with exitcode (%d). job name: %s. job id: %s", exitCode,
                this.jobName, this.jobId));
        return new TaskResult(TaskResult.Status.FAILED, "Exit code: " + exitCode);
      }
    } catch (final Throwable t) {
      logger.error("SingleHelixTask failed due to " + t.getMessage(), t);
      return new TaskResult(TaskResult.Status.FAILED, Throwables.getStackTraceAsString(t));
    }
  }

  @Override
  public void cancel() {
    logger.info(String
        .format("Canceling a single task process. job name: %s. job id: %s", this.jobName,
            this.jobId));
    this.taskProcess.destroyForcibly();
  }
}
