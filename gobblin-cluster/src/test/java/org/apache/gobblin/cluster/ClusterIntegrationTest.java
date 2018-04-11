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
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.testing.AssertWithBackoff;


public class ClusterIntegrationTest {

  public final static Logger _logger = LoggerFactory.getLogger(ClusterIntegrationTest.class);
  public static final String JOB_CONF_NAME = "HelloWorldJob.conf";
  Config _config;
  private Path _workPath;
  private Path _jobConfigPath;
  private Path _jobOutputBasePath;
  private URL _jobConfResourceUrl;
  private TestingServer _testingZKServer;
  private GobblinTaskRunner _worker;
  private GobblinClusterManager _manager;
  private boolean _runTaskInSeparateProcess;


  @Test
  public void simpleJobShouldComplete() throws Exception {
    runSimpleJobAndVerifyResult();
  }

  @Test
  public void simpleJobShouldCompleteInTaskIsolationMode()
      throws Exception {
    _runTaskInSeparateProcess = true;
    runSimpleJobAndVerifyResult();
  }

  private void runSimpleJobAndVerifyResult()
      throws Exception {
    init();
    startCluster();
    waitForAndVerifyOutputFiles();
    shutdownCluster();
  }

  private void init() throws Exception {
    initWorkDir();
    initZooKeeper();
    initConfig();
    initJobConfDir();
    initJobOutputDir();
  }

  private void initWorkDir() throws IOException {
    // Relative to the current directory
    _workPath = Paths.get("gobblin-integration-test-work-dir");
    _logger.info("Created a new work directory: " + _workPath.toAbsolutePath());

    // Delete the working directory in case the previous test fails to delete the directory
    // e.g. when the test was killed forcefully under a debugger.
    deleteWorkDir();
    Files.createDirectory(_workPath);
  }

  private void initJobConfDir() throws IOException {
    String jobConfigDir = _config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY);
    _jobConfigPath = Paths.get(jobConfigDir);
    Files.createDirectories(_jobConfigPath);
    _jobConfResourceUrl = Resources.getResource(JOB_CONF_NAME);
    copyJobConfFromResource();
  }

  private void initJobOutputDir() throws IOException {
    _jobOutputBasePath = Paths.get(_workPath + "/job-output");
    Files.createDirectory(_jobOutputBasePath);
  }

  private void copyJobConfFromResource() throws IOException {
    try (InputStream resourceStream = _jobConfResourceUrl.openStream()) {
      File targetFile = new File(_jobConfigPath + "/" + JOB_CONF_NAME);
      FileUtils.copyInputStreamToFile(resourceStream, targetFile);
    }
  }

  private void initZooKeeper() throws Exception {
    _testingZKServer = new TestingServer(false);
    _logger.info(
        "Created testing ZK Server. Connection string : " + _testingZKServer.getConnectString());
  }

  private void initConfig() {
    Config configFromResource = getConfigFromResource();
    Config configOverride = getConfigOverride();
    _config = configOverride.withFallback(configFromResource).resolve();
  }

  private Config getConfigOverride() {
    Map<String, String> configMap = new HashMap<>();
    String zkConnectionString = _testingZKServer.getConnectString();
    configMap.put(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY, zkConnectionString);
    configMap.put(GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR, _workPath.toString());
    if (_runTaskInSeparateProcess) {
      configMap.put(GobblinClusterConfigurationKeys.ENABLE_TASK_IN_SEPARATE_PROCESS, "true");
    }
    Config config = ConfigFactory.parseMap(configMap);
    return config;
  }

  private Config getConfigFromResource() {
    URL url = Resources.getResource("BasicCluster.conf");
    Config config = ConfigFactory.parseURL(url);
    return config;
  }

  @AfterMethod
  public void tearDown() throws IOException {
    deleteWorkDir();
  }

  private void deleteWorkDir() throws IOException {
    if ((_workPath != null) && Files.exists(_workPath)) {
      FileUtils.deleteDirectory(_workPath.toFile());
    }
  }

  private void createHelixCluster() {
    String zkConnectionString = _config
        .getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helix_cluster_name = _config
        .getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, helix_cluster_name);
  }

  private void startCluster() throws Exception {
    _testingZKServer.start();
    createHelixCluster();
    startWorker();
    startManager();
  }

  private void startWorker() throws Exception {
    _worker = new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, "Worker",
        TestHelper.TEST_APPLICATION_ID, "1",
        _config, Optional.absent());

    // Need to run in another thread since the start call will not return until the stop method
    // is called.
    Thread workerThread = new Thread(_worker::start);
    workerThread.start();
  }

  private void startManager() throws Exception {
    _manager = new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME,
        TestHelper.TEST_APPLICATION_ID,
        _config, Optional.absent());

    _manager.start();
  }

  private void shutdownCluster() throws InterruptedException, IOException {
    _worker.stop();
    _manager.stop();
    _testingZKServer.close();
  }

  private void waitForAndVerifyOutputFiles() throws Exception {

    AssertWithBackoff asserter = AssertWithBackoff.create().logger(_logger).timeoutMs(60_000)
        .maxSleepMs(100).backoffFactor(1.5);

    asserter.assertTrue(this::hasExpectedFilesBeenCreated, "Waiting for job-completion");
  }

  private boolean hasExpectedFilesBeenCreated(Void input) {
    int numOfFiles = getNumOfOutputFiles(_jobOutputBasePath);
    return numOfFiles == 1;
  }

  private int getNumOfOutputFiles(Path jobOutputDir) {
    Collection<File> outputFiles = FileUtils
        .listFiles(jobOutputDir.toFile(), new String[]{"txt"}, true);
    return outputFiles.size();
  }
}
