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

package org.apache.gobblin.cluster.temporal;

import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinHelixTaskStateTracker;
import org.apache.gobblin.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.cluster.SingleTask;
import org.apache.gobblin.cluster.TaskAttemptBuilder;
import org.apache.gobblin.cluster.TaskRunnerSuiteBase;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.util.ConfigUtils;

public class GobblinTemporalActivityImpl implements GobblinTemporalActivity {

    private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalActivityImpl.class);
    private TaskRunnerSuiteBase.Builder builder;
    private StateStores stateStores;
    private TaskAttemptBuilder taskAttemptBuilder;


    @Override
    public String composeGreeting(String name) {
        LOGGER.info("Activity triggered");
        return "Hello " + name + "!";
    }

    private TaskAttemptBuilder createTaskAttemptBuilder() {
        Properties properties = ConfigUtils.configToProperties(builder.getConfig());
        TaskStateTracker taskStateTracker = new GobblinHelixTaskStateTracker(properties);
        TaskExecutor taskExecutor = new TaskExecutor(ConfigUtils.configToProperties(builder.getConfig()));
        TaskAttemptBuilder taskAttemptBuilder = new TaskAttemptBuilder(taskStateTracker, taskExecutor);
        taskAttemptBuilder.setTaskStateStore(this.stateStores.getTaskStateStore());
        return taskAttemptBuilder;
    }

    @Override
    public void run(Properties jobProps, String appWorkDirStr, String jobId, String workUnitFilePath, String jobStateFilePath, String workflowId)
        throws Exception {
        Path appWorkDir = new Path(appWorkDirStr);
        this.builder = GobblinTemporalTaskRunner.getBuilder();

        Config stateStoreJobConfig = ConfigUtils.propertiesToConfig(jobProps)
            .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(
                new URI(appWorkDir.toUri().getScheme(), null, appWorkDir.toUri().getHost(), appWorkDir.toUri().getPort(),
                    "/", null, null).toString()));

        this.stateStores =
            new StateStores(stateStoreJobConfig, appWorkDir, GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME,
                appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME, appWorkDir,
                GobblinClusterConfigurationKeys.JOB_STATE_DIR_NAME);

        this.taskAttemptBuilder = createTaskAttemptBuilder();

        // Dynamic config is considered as part of JobState in SingleTask
        // Important to distinguish between dynamicConfig and Config
        final Config dynamicConfig = builder.getDynamicConfig()
            .withValue(GobblinClusterConfigurationKeys.TASK_RUNNER_HOST_NAME_KEY, ConfigValueFactory.fromAnyRef(builder.getHostName()))
            .withValue(GobblinClusterConfigurationKeys.CONTAINER_ID_KEY, ConfigValueFactory.fromAnyRef(builder.getContainerId()))
            .withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY, ConfigValueFactory.fromAnyRef(builder.getInstanceName()));

        SingleTask singleTask = new SingleTask(jobId, new Path(workUnitFilePath), new Path(jobStateFilePath), builder.getFs(), this.taskAttemptBuilder, this.stateStores, dynamicConfig, workflowId);
        singleTask.run();
    }
}
