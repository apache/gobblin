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
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.Id;


/**
 * An implementation of Helix's {@link org.apache.helix.task.Task} that wraps and runs one or more Gobblin
 * {@link org.apache.gobblin.runtime.Task}s.
 *
 * <p>
 *   Upon startup, a {@link GobblinHelixTask} reads the property
 *   {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH} for the path of the file storing a serialized
 *   {@link WorkUnit} on the {@link FileSystem} of choice and de-serializes the {@link WorkUnit}. Depending on
 *   if the serialized {@link WorkUnit} is a {@link MultiWorkUnit}, it then creates one or more Gobblin
 *   {@link org.apache.gobblin.runtime.Task}s to run the {@link WorkUnit}(s) (possibly wrapped in the {@link MultiWorkUnit})
 *   and waits for the Gobblin {@link org.apache.gobblin.runtime.Task}(s) to finish. Upon completion of the Gobblin
 *   {@link org.apache.gobblin.runtime.Task}(s), it persists the {@link TaskState} of each {@link org.apache.gobblin.runtime.Task} to
 *   a file that will be collected by the {@link GobblinHelixJobLauncher} later upon completion of the job.
 * </p>
 */
@Alpha
public class GobblinHelixTask implements Task {

  private static final Logger _logger = LoggerFactory.getLogger(GobblinHelixTask.class);

  private final TaskConfig taskConfig;
  private String jobName;
  private String jobId;
  private String jobKey;
  private Path workUnitFilePath;

  private SingleTask task;

  public GobblinHelixTask(TaskCallbackContext taskCallbackContext, FileSystem fs, Path appWorkDir,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores)
      throws IOException {

    this.taskConfig = taskCallbackContext.getTaskConfig();
    getInfoFromTaskConfig();

    Path jobStateFilePath = constructJobStateFilePath(appWorkDir);

    this.task =
        new SingleTask(this.jobId, workUnitFilePath, jobStateFilePath, fs, taskAttemptBuilder,
            stateStores);
  }

  private Path constructJobStateFilePath(Path appWorkDir) {
    return new Path(appWorkDir, this.jobId + "." + AbstractJobLauncher.JOB_STATE_FILE_NAME);
  }

  private void getInfoFromTaskConfig() {
    Map<String, String> configMap = this.taskConfig.getConfigMap();
    this.jobName = configMap.get(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = configMap.get(ConfigurationKeys.JOB_ID_KEY);
    this.jobKey = Long.toString(Id.parse(this.jobId).getSequence());
    this.workUnitFilePath =
        new Path(configMap.get(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH));
  }

  @Override
  public TaskResult run() {
    try (Closer closer = Closer.create()) {
      closer.register(MDC.putCloseable(ConfigurationKeys.JOB_NAME_KEY, this.jobName));
      closer.register(MDC.putCloseable(ConfigurationKeys.JOB_KEY_KEY, this.jobKey));
      this.task.run();
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return new TaskResult(TaskResult.Status.CANCELED, "");
    } catch (Throwable t) {
      _logger.error("GobblinHelixTask failed due to " + t.getMessage(), t);
      return new TaskResult(TaskResult.Status.FAILED, Throwables.getStackTraceAsString(t));
    }
  }

  @Override
  public void cancel() {
    this.task.cancel();
  }
}
