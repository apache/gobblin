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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;


class SingleTaskRunner {
  private static final Logger logger = LoggerFactory.getLogger(SingleTaskRunner.class);

  protected final String jobId;
  protected final String workUnitFilePath;
  protected final Config clusterConfig;
  private final Path appWorkPath;
  @VisibleForTesting
  SingleTask task;
  private TaskExecutor taskExecutor;
  private GobblinHelixTaskStateTracker taskStateTracker;
  private ServiceManager serviceManager;

  SingleTaskRunner(final String clusterConfigFilePath, final String jobId,
      final String workUnitFilePath) {
    this.jobId = jobId;
    this.workUnitFilePath = workUnitFilePath;
    this.clusterConfig = ConfigFactory.parseFile(new File(clusterConfigFilePath));
    final String workDir = this.clusterConfig.getString(GobblinTaskRunner.CLUSTER_APP_WORK_DIR);
    this.appWorkPath = new Path(workDir);
  }

  void run()
      throws IOException, InterruptedException {
    this.run(false);
  }

  /**
   *
   * @param fail set to false in normal cases, when set to true, the underlying task will fail.
   */
  void run(boolean fail) throws IOException, InterruptedException{
    logger.info("SingleTaskRunner running.");
    startServices();
    runTask(fail);
    shutdownServices();
  }

  @VisibleForTesting
  void startServices() {
    logger.info("SingleTaskRunner start services.");
    initServices();
    this.serviceManager.startAsync();
    try {
      this.serviceManager.awaitHealthy(10, TimeUnit.SECONDS);
    } catch (final TimeoutException e) {
      throw new GobblinClusterException("Timeout waiting for services to start.", e);
    }
  }

  private void shutdownServices() {
    logger.info("SingleTaskRunner shutting down services.");
    this.serviceManager.stopAsync();
    try {
      this.serviceManager.awaitStopped(1, TimeUnit.MINUTES);
    } catch (final TimeoutException e) {
      logger.error("Timeout waiting for services to shutdown.", e);
    }
  }

  private void runTask(boolean fail)
      throws IOException, InterruptedException {
    logger.info("SingleTaskRunner running task.");
    initClusterSingleTask(fail);
    this.task.run();
  }

  void initClusterSingleTask(boolean fail)
      throws IOException {
    final FileSystem fs = getFileSystem();
    final StateStores stateStores = new StateStores(this.clusterConfig, this.appWorkPath,
        GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME, this.appWorkPath,
        GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME, this.appWorkPath,
        GobblinClusterConfigurationKeys.JOB_STATE_DIR_NAME);
    final Path jobStateFilePath =
        GobblinClusterUtils.getJobStateFilePath(stateStores.haveJobStateStore(), this.appWorkPath, this.jobId);

    final TaskAttemptBuilder taskAttemptBuilder = getTaskAttemptBuilder(stateStores);

    this.task = createSingleTaskHelper(taskAttemptBuilder, fs, stateStores, jobStateFilePath, fail);
  }

  protected SingleTask createSingleTaskHelper(TaskAttemptBuilder taskAttemptBuilder, FileSystem fs,
      StateStores stateStores, Path jobStateFilePath, boolean fail) throws IOException {
    return new SingleTask(this.jobId, new Path(this.workUnitFilePath), jobStateFilePath, fs,
        taskAttemptBuilder, stateStores, GobblinClusterUtils.getDynamicConfig(this.clusterConfig));
  }

  private TaskAttemptBuilder getTaskAttemptBuilder(final StateStores stateStores) {
    final TaskAttemptBuilder taskAttemptBuilder =
        new TaskAttemptBuilder(this.taskStateTracker, this.taskExecutor);
    // No container id is set. Use the default.
    taskAttemptBuilder.setTaskStateStore(stateStores.getTaskStateStore());
    return taskAttemptBuilder;
  }

  private void initServices() {
    final Properties properties = ConfigUtils.configToProperties(this.clusterConfig);
    this.taskExecutor = new TaskExecutor(properties);
    this.taskStateTracker = new GobblinHelixTaskStateTracker(properties);

    final List<Service> services = Lists.newArrayList(this.taskExecutor, this.taskStateTracker);
    this.serviceManager = new ServiceManager(services);
  }

  private FileSystem getFileSystem()
      throws IOException {
    final Configuration conf = HadoopUtils.newConfiguration();

    final FileSystem fs = this.clusterConfig.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(this.clusterConfig.getString(ConfigurationKeys.FS_URI_KEY)), conf)
        : FileSystem.get(conf);

    return fs;
  }
}
