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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.util.GobblinProcessBuilder;
import org.apache.gobblin.util.SystemPropertiesWrapper;

import com.typesafe.config.Config;

import static org.apache.gobblin.cluster.SingleTaskRunnerMainOptions.CLUSTER_CONFIG_FILE_PATH;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainOptions.JOB_ID;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainOptions.WORK_UNIT_FILE_PATH;


class SingleTaskLauncher {
  private static final Logger logger = LoggerFactory.getLogger(SingleTaskLauncher.class);

  private final GobblinProcessBuilder processBuilder;
  private final SystemPropertiesWrapper propertiesWrapper;
  private final Path clusterConfigFilePath;
  private final Config sysConfig;

  SingleTaskLauncher(final GobblinProcessBuilder processBuilder,
      final SystemPropertiesWrapper propertiesWrapper, final Path clusterConfigFilePath, Config sysConfig) {
    this.processBuilder = processBuilder;
    this.propertiesWrapper = propertiesWrapper;
    this.clusterConfigFilePath = clusterConfigFilePath;
    this.sysConfig = sysConfig;
  }

  Process launch(final String jobId, final Path workUnitFilePath)
      throws IOException {
    final SingleTaskLauncher.CmdBuilder cmdBuilder = this.new CmdBuilder(jobId, workUnitFilePath);
    final List<String> command = cmdBuilder.build();
    logger.info("Launching a task process.");

    // The -cp parameter list can be very long.
    final String completeCmdLine = String.join(" ", command);
    logger.debug("cmd line:\n{}", completeCmdLine);

    final Process taskProcess = this.processBuilder.start(command);

    return taskProcess;
  }

  private class CmdBuilder {
    private final String jobId;
    private final Path workUnitFilePath;
    private final List<String> cmd = new ArrayList<>();

    private CmdBuilder(final String jobId, final Path workUnitFilePath) {
      this.jobId = jobId;
      this.workUnitFilePath = workUnitFilePath;
    }

    List<String> build() {
      addJavaBin();
      addClassPath();
      addLogConfig();
      addClassName();
      addOptions();
      return this.cmd;
    }

    private void addClassName() {
      final String runnerClassName = SingleTaskRunnerMain.class.getCanonicalName();
      this.cmd.add(runnerClassName);
    }

    private void addJavaBin() {
      final String javaHomeDir = SingleTaskLauncher.this.propertiesWrapper.getJavaHome();
      final Path javaBinPath = Paths.get(javaHomeDir, "bin", "java");
      this.cmd.add(javaBinPath.toString());
    }

    private void addLogConfig() {
      if (sysConfig.hasPath(GobblinClusterConfigurationKeys.TASK_LOG_CONFIG)) {
        String logConfig = sysConfig.getString(GobblinClusterConfigurationKeys.TASK_LOG_CONFIG);
        this.cmd.add(logConfig);
      }
    }

    private void addClassPath() {
      this.cmd.add("-cp");
      String classPath;
      if (sysConfig.hasPath(GobblinClusterConfigurationKeys.TASK_CLASSPATH)) {
        classPath = sysConfig.getString(GobblinClusterConfigurationKeys.TASK_CLASSPATH);
      } else {
        classPath = SingleTaskLauncher.this.propertiesWrapper.getJavaClassPath();
      }
      this.cmd.add(classPath);
    }

    private void addOptions() {
      addClusterConfigPath();
      addJobId();
      addWorkUnitPath();
    }

    private void addClusterConfigPath() {
      addOneOption(CLUSTER_CONFIG_FILE_PATH,
          SingleTaskLauncher.this.clusterConfigFilePath.toString());
    }

    private void addWorkUnitPath() {
      addOneOption(WORK_UNIT_FILE_PATH, this.workUnitFilePath.toString());
    }

    private void addJobId() {
      addOneOption(JOB_ID, this.jobId);
    }

    private void addOneOption(final String key, final String value) {
      this.cmd.add("--" + key);
      this.cmd.add(value);
    }
  }
}
