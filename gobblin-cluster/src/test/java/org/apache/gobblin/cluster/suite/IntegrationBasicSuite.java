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

package org.apache.gobblin.cluster.suite;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.ClusterIntegrationTest;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinTaskRunner;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.cluster.TestHelper;
import org.apache.gobblin.testing.AssertWithBackoff;

/**
 * A test suite used for {@link ClusterIntegrationTest#testJobShouldComplete()}
 *
 * This basic suite class provides utilities to launch one manager and multiple workers (participants).
 * User can override {@link IntegrationBasicSuite#getWorkerConfigs()} for worker customization.
 * User can also override {@link IntegrationBasicSuite#waitForAndVerifyOutputFiles()} to check different successful condition.
 */
@Slf4j
public class IntegrationBasicSuite {
  public static final String JOB_CONF_NAME = "HelloWorldJob.conf";
  public static final String WORKER_INSTANCE_0 = "WorkerInstance_0";
  public static final String TEST_INSTANCE_NAME_KEY = "worker.instance.name";

  // manager and workers
  protected Config managerConfig;
  protected Collection<Config> taskDriverConfigs = Lists.newArrayList();
  protected Collection<Config> workerConfigs = Lists.newArrayList();
  protected Collection<GobblinTaskRunner> workers = Lists.newArrayList();
  protected Collection<GobblinTaskRunner> taskDrivers = Lists.newArrayList();
  protected GobblinClusterManager manager;

  protected Path workPath;
  protected Path jobConfigPath;
  protected Path jobOutputBasePath;
  protected URL jobConfResourceUrl;
  protected TestingServer testingZKServer;

  public IntegrationBasicSuite() {
    try {
      initWorkDir();
      initJobOutputDir();
      initZooKeeper();
      initConfig();
      initJobConfDir();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initConfig() {
    this.managerConfig = this.getManagerConfig();
    this.taskDriverConfigs = this.getTaskDriverConfigs();
    this.workerConfigs = this.getWorkerConfigs();
  }

  private void initZooKeeper() throws Exception {
    this.testingZKServer = new TestingServer(false);
    log.info(
        "Created testing ZK Server. Connection string : " + testingZKServer.getConnectString());
  }

  private void initJobConfDir() throws IOException {
    String jobConfigDir = this.managerConfig.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY);
    this.jobConfigPath = Paths.get(jobConfigDir);
    Files.createDirectories(this.jobConfigPath);
    this.jobConfResourceUrl = Resources.getResource(JOB_CONF_NAME);
    copyJobConfFromResource();
  }

  private void initJobOutputDir() throws IOException {
    this.jobOutputBasePath = Paths.get(this.workPath + "/job-output");
    Files.createDirectory(this.jobOutputBasePath);
  }

  private void initWorkDir() throws IOException {
    // Relative to the current directory
    this.workPath = Paths.get("gobblin-integration-test-work-dir");
    log.info("Created a new work directory: " + this.workPath.toAbsolutePath());

    // Delete the working directory in case the previous test fails to delete the directory
    // e.g. when the test was killed forcefully under a debugger.
    deleteWorkDir();
    Files.createDirectory(this.workPath);
  }

  public void deleteWorkDir() throws IOException {
    if ((this.workPath != null) && Files.exists(this.workPath)) {
      FileUtils.deleteDirectory(this.workPath.toFile());
    }
  }

  private void copyJobConfFromResource() throws IOException {
    Map<String, Config> jobConfigs;
    try (InputStream resourceStream = this.jobConfResourceUrl.openStream()) {
      Reader reader = new InputStreamReader(resourceStream);
      Config rawJobConfig = ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
      jobConfigs = overrideJobConfigs(rawJobConfig);
      jobConfigs.forEach((jobName, jobConfig)-> {
        try {
          writeJobConf(jobName, jobConfig);
        } catch (IOException e) {
          log.error("Job " + jobName + " config cannot be written.");
        }
      });
    }
  }

  protected Map<String, Config> overrideJobConfigs(Config rawJobConfig) {
    return ImmutableMap.of("HelloWorldJob", rawJobConfig);
  }

  private void writeJobConf(String jobName, Config jobConfig) throws IOException {
    String targetPath = this.jobConfigPath + "/" + jobName + ".conf";
    String renderedConfig = jobConfig.root().render(ConfigRenderOptions.defaults());
    try (DataOutputStream os = new DataOutputStream(new FileOutputStream(targetPath));
        Writer writer = new OutputStreamWriter(os, Charsets.UTF_8)) {
      writer.write(renderedConfig);
    }
  }

  protected Config getClusterConfig() {
    URL url = Resources.getResource("BasicCluster.conf");
    Config config = ConfigFactory.parseURL(url);

    Map<String, String> configMap = new HashMap<>();
    String zkConnectionString = this.testingZKServer.getConnectString();
    configMap.put(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY, zkConnectionString);
    configMap.put(GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR, this.workPath.toString());
    Config overrideConfig = ConfigFactory.parseMap(configMap);

    return overrideConfig.withFallback(config);
  }

  protected Config getManagerConfig() {
    // manager config initialization
    URL url = Resources.getResource("BasicManager.conf");
    Config managerConfig = ConfigFactory.parseURL(url);
    managerConfig = managerConfig.withFallback(getClusterConfig());
    return managerConfig.resolve();
  }

  protected Collection<Config> getTaskDriverConfigs() {
    return new ArrayList<>();
  }

  protected Collection<Config> getWorkerConfigs() {
    // worker config initialization
    URL url = Resources.getResource("BasicWorker.conf");
    Config workerConfig = ConfigFactory.parseURL(url);
    workerConfig = workerConfig.withFallback(getClusterConfig());
    return Lists.newArrayList(workerConfig.resolve());
  }

  protected Config addInstanceName(Config baseConfig, String instanceName) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(IntegrationBasicSuite.TEST_INSTANCE_NAME_KEY, instanceName);
    Config instanceConfig = ConfigFactory.parseMap(configMap);
    return instanceConfig.withFallback(baseConfig).resolve();
  }

  public void waitForAndVerifyOutputFiles() throws Exception {
    AssertWithBackoff asserter = AssertWithBackoff.create().logger(log).timeoutMs(120_000)
        .maxSleepMs(100).backoffFactor(1.5);

    asserter.assertTrue(this::hasExpectedFilesBeenCreated, "Waiting for job-completion");
  }

  protected boolean hasExpectedFilesBeenCreated(Void input) {
    int numOfFiles = getNumOfOutputFiles(this.jobOutputBasePath);
    return numOfFiles == 1;
  }

  protected int getNumOfOutputFiles(Path jobOutputDir) {
    Collection<File> outputFiles = FileUtils
        .listFiles(jobOutputDir.toFile(), new String[]{"txt"}, true);
    return outputFiles.size();
  }

  public void startCluster() throws Exception {
    this.testingZKServer.start();
    createHelixCluster();
    startWorker();
    startTaskDriver();
    startManager();
  }

  private void startManager() throws Exception {
    this.manager = new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME,
        TestHelper.TEST_APPLICATION_ID,
        this.managerConfig, Optional.absent());

    this.manager.start();
  }

  private void startTaskDriver() throws Exception {
    for (Config taskDriverConfig: this.taskDriverConfigs) {
      GobblinTaskRunner runner = new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME,
          taskDriverConfig.getString(TEST_INSTANCE_NAME_KEY),
          TestHelper.TEST_APPLICATION_ID, "1",
          taskDriverConfig, Optional.absent());
      this.taskDrivers.add(runner);

      // Need to run in another thread since the start call will not return until the stop method
      // is called.
      Thread workerThread = new Thread(runner::start);
      workerThread.start();
    }
  }

  private void startWorker() throws Exception {
    if (workerConfigs.size() == 1) {
      this.workers.add(new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, WORKER_INSTANCE_0,
          TestHelper.TEST_APPLICATION_ID, "1",
          this.workerConfigs.iterator().next(), Optional.absent()));

      // Need to run in another thread since the start call will not return until the stop method
      // is called.
      Thread workerThread = new Thread(this.workers.iterator().next()::start);
      workerThread.start();
    } else {
      // Each workerConfig corresponds to a worker instance
      for (Config workerConfig: this.workerConfigs) {
        GobblinTaskRunner runner = new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME,
            workerConfig.getString(TEST_INSTANCE_NAME_KEY),
            TestHelper.TEST_APPLICATION_ID, "1",
            workerConfig, Optional.absent());
        this.workers.add(runner);

        // Need to run in another thread since the start call will not return until the stop method
        // is called.
        Thread workerThread = new Thread(runner::start);
        workerThread.start();
      }
    }
  }

  public void shutdownCluster() throws InterruptedException, IOException {
    this.workers.forEach(runner->runner.stop());
    this.taskDrivers.forEach(runner->runner.stop());
    this.manager.stop();
    this.testingZKServer.close();
  }

  protected void createHelixCluster() throws Exception {
    String zkConnectionString = this.managerConfig
        .getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helix_cluster_name = this.managerConfig
        .getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, helix_cluster_name);
  }
}
