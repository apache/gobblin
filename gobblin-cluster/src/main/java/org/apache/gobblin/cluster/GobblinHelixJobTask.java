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
import java.util.Properties;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.util.ConfigUtils;

/**
 * An implementation of Helix's {@link org.apache.helix.task.Task} that runs original {@link GobblinHelixJobLauncher}
 */
@Slf4j
public class GobblinHelixJobTask implements Task {

  private final TaskConfig taskConfig;
  private Config sysConfig;
  private Properties jobConfig;
  private StateStores stateStores;
  private String planningJobId;

  public GobblinHelixJobTask(TaskCallbackContext context,
      Config sysConfig,
      StateStores stateStores) {
    this.taskConfig = context.getTaskConfig();
    this.sysConfig = sysConfig;
    this.jobConfig = ConfigUtils.configToProperties(sysConfig);
    Map<String, String> configMap = this.taskConfig.getConfigMap();
    for (Map.Entry<String, String> entry: configMap.entrySet()) {
      if (entry.getKey().startsWith(GobblinHelixDistributeJobExecutionLauncher.JOB_PROPS_PREFIX)) {
          String key = entry.getKey().replaceFirst(GobblinHelixDistributeJobExecutionLauncher.JOB_PROPS_PREFIX, "");
          jobConfig.put(key, entry.getValue());
      }
    }

    if (!jobConfig.containsKey(GobblinClusterConfigurationKeys.PLANNING_ID_KEY)) {
      throw new RuntimeException("Job doesn't have plannning ID");
    }

    this.planningJobId = jobConfig.getProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
    this.stateStores = stateStores;
  }

  @Override
  public TaskResult run() {
    log.info("We will run planning job " + this.planningJobId);

    // TODO: We should run GobblinHelixJobLauncher#launchJob() here

    try {
      setResultToUserContent(ImmutableMap.of(Partitioner.IS_EARLY_STOPPED, "false"));
    } catch (IOException e) {
      return new TaskResult(TaskResult.Status.FAILED, "State store cannot be persisted for job " + planningJobId);
    }
    return new TaskResult(TaskResult.Status.COMPLETED, "");
  }

  //TODO: change below to Helix UserConentStore
  @VisibleForTesting
  protected void setResultToUserContent(Map<String, String> keyValues) throws IOException {
    WorkUnitState wus = new WorkUnitState();
    wus.setProp(ConfigurationKeys.JOB_ID_KEY, this.planningJobId);
    wus.setProp(ConfigurationKeys.TASK_ID_KEY, this.planningJobId);
    wus.setProp(ConfigurationKeys.TASK_KEY_KEY, this.planningJobId);
    keyValues.forEach((key, value)->wus.setProp(key, value));
    TaskState taskState = new TaskState(wus);

    this.stateStores.getTaskStateStore().put(this.planningJobId, this.planningJobId, taskState);
  }

  @Override
  public void cancel() {
    // TODO: We should delete the real job.
  }
}
