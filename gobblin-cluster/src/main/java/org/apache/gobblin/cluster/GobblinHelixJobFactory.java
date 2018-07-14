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

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.util.PathUtils;


/**
 * An implementation of Helix's {@link TaskFactory} for {@link GobblinHelixJobTask}s.
 */
@Slf4j
public class GobblinHelixJobFactory implements TaskFactory {
  protected StateStores stateStores;

  protected TaskRunnerSuiteBase.Builder builder;

  private void initializeStateStore(TaskRunnerSuiteBase.Builder builder) {
    Config sysConfig = builder.getConfig();
    Path appWorkDir = builder.getAppWorkPath();
    URI rootPathUri = PathUtils.getRootPath(appWorkDir).toUri();
    Config stateStoreJobConfig = sysConfig
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
            ConfigValueFactory.fromAnyRef(rootPathUri.toString()));

    this.stateStores = new StateStores(stateStoreJobConfig,
        appWorkDir, GobblinHelixDistributeJobExecutionLauncher.PLANNING_TASK_STATE_DIR_NAME,
        appWorkDir, GobblinHelixDistributeJobExecutionLauncher.PLANNING_WORK_UNIT_DIR_NAME,
        appWorkDir, GobblinHelixDistributeJobExecutionLauncher.PLANNING_JOB_STATE_DIR_NAME);
  }

  public GobblinHelixJobFactory(TaskRunnerSuiteBase.Builder builder) {
    this.builder = builder;
    // TODO: We can remove below initialization once Helix allow us to persist job resut in userContentStore
    initializeStateStore(this.builder);
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    return new GobblinHelixJobTask(context,
        this.stateStores,
        this.builder);
  }
}
