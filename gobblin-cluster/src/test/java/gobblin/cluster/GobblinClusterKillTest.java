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

package gobblin.cluster;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import gobblin.testing.AssertWithBackoff;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for killing {@link GobblinClusterManager}s and {@link GobblinTaskRunner}s
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses the {@link HelixManager} to
 *   act as a testing Helix participant to receive the container (running the {@link GobblinTaskRunner})
 *   shutdown request message.
 * </p>
 */
@Test(groups = { "gobblin.cluster" }, singleThreaded = true)
public class GobblinClusterKillTest {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinClusterKillTest.class);

  private TestingServer _testingZKServer;

  private final static int ASSERT_TIMEOUT = 60000;
  private final static int ASSERT_MAX_SLEEP = 2000;
  private final static int NUM_MANAGERS = 3;
  private final static int NUM_WORKERS = 2;
  private GobblinClusterManager[] _clusterManagers;
  private GobblinTaskRunner[] _clusterWorkers;
  private Thread[] _workerStartThreads;

  private String _testDirPath;
  Config _config;

  /**
   * clean up and set up test directory
   */
  private void setupTestDir() throws IOException {
    _testDirPath = _config.getString("gobblin.cluster.work.dir");
    String jobDirPath = _config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY);

    // clean up test directory and create job dir
    File testDir = new File(_testDirPath);
    File jobDir = new File(jobDirPath);

    if (testDir.exists()) {
      FileUtils.deleteDirectory(testDir);
    }

    jobDir.mkdirs();

    // copy job file from resource
    String jobFileName = GobblinClusterKillTest.class.getSimpleName() + ".job";
    try (InputStream resourceStream = this.getClass().getClassLoader().getResourceAsStream(jobFileName)) {
      if (resourceStream == null) {
        throw new RuntimeException("Could not find job resource " + jobFileName);
      }
      File targetFile = new File(jobDirPath + "/" + jobFileName);

      FileUtils.copyInputStreamToFile(resourceStream, targetFile);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load job resource " + jobFileName, e);
    }
  }

  /**
   * Create and start a cluster manager
   * @param id - array offset
   * @throws Exception
   */
  private void setupManager(int id) throws Exception {
    _clusterManagers[id] =
        new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_APPLICATION_ID,
            _config.withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
                ConfigValueFactory.fromAnyRef("Manager_" + id)),
            Optional.of(new Path(_config.getString("gobblin.cluster.work.dir"))));

    _clusterManagers[id].start();
  }

  /**
   * Create and start a cluster worker
   * @param id - array offset
   * @throws Exception
   */
  private void setupWorker(int id) throws Exception {
    final GobblinTaskRunner fworker =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, "Worker_" + id, TestHelper.TEST_APPLICATION_ID, "1",
            _config, Optional.of(new Path(_config.getString("gobblin.cluster.work.dir"))));

    _clusterWorkers[id] = fworker;
    _workerStartThreads[id] = new Thread(new Runnable() {
      @Override
      public void run() {
        fworker.start();
      }
    });
    _workerStartThreads[id].start();
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Use a random ZK port
    _testingZKServer = new TestingServer(-1);
    LOG.info("Testing ZK Server listening on: " + _testingZKServer.getConnectString());

    URL url = GobblinClusterKillTest.class.getClassLoader().getResource(
        GobblinClusterKillTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    _config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(_testingZKServer.getConnectString()))
        .resolve();

    String zkConnectionString = _config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString,
        _config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    setupTestDir();

    _clusterManagers = new GobblinClusterManager[NUM_MANAGERS];
    _clusterWorkers = new GobblinTaskRunner[NUM_WORKERS];
    _workerStartThreads = new Thread[NUM_WORKERS];

    for (int i = 0; i < NUM_MANAGERS; i++) {
      setupManager(i);
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
      setupWorker(i);
    }
  }

  // The kill tests are unreliable on Travis
  @Test(groups = { "disabledOnTravis" })
  public void testKillWorker() throws TimeoutException, InterruptedException {
    Collection<File> matches = Collections.EMPTY_LIST;

    final File writerOutputDir = new File(_testDirPath + "/writer-output/gobblin/util/test/HelloWorldSource/");
    final File jobOutputDir = new File(_testDirPath + "/job-output/gobblin/util/test/HelloWorldSource/");

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        if (writerOutputDir.exists()) {
          return FileUtils.listFiles(writerOutputDir, new String[]{"txt"}, true).size() >= 25;
        } else {
          return false;
        }
      }
    }, "Waiting for writer output");

    LOG.info("{} matches found before disconnecting worker",
        FileUtils.listFiles(writerOutputDir, new String[]{"txt"}, true).size());

    _clusterWorkers[0].disconnectHelixManager();

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        if (jobOutputDir.exists()) {
          return FileUtils.listFiles(jobOutputDir, new String[]{"txt"}, true).size() >= 100;
        } else {
          return false;
        }
      }
    }, "Waiting for job-completion");
  }

  // The kill tests are unreliable on Travis
  @Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testKillWorker")
  public void testKillManager() throws IOException, TimeoutException, InterruptedException {
    Collection<File> matches = Collections.EMPTY_LIST;
    final File writerOutputDir = new File(_testDirPath + "/writer-output/gobblin/util/test/HelloWorldSource/");
    final File jobOutputDir = new File(_testDirPath + "/job-output/gobblin/util/test/HelloWorldSource/");

    // reinitialize test directory
    setupTestDir();

    // kill a manager to cause leader election. New leader will schedule a new job.
    _clusterManagers[0].disconnectHelixManager();

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        if (writerOutputDir.exists()) {
          return FileUtils.listFiles(writerOutputDir, new String[]{"txt"}, true).size() >= 25;
        } else {
          return false;
        }
      }
    }, "Waiting for writer output");

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        if (jobOutputDir.exists()) {
          return FileUtils.listFiles(jobOutputDir, new String[]{"txt"}, true).size() >= 100;
        } else {
          return false;
        }
      }
    }, "Waiting for job-completion");
  }

  @AfterClass
  public void tearDown() throws IOException, InterruptedException {

    for (int i = 0; i < NUM_MANAGERS; i++) {
      _clusterManagers[i].connectHelixManager();
      if (!_clusterManagers[i].isHelixManagerConnected()) {
        _clusterManagers[i].connectHelixManager();
      }
      _clusterManagers[i].stop();
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
      _clusterWorkers[i].stop();
    }

    for (int i = 0; i < NUM_WORKERS; i++) {
      _workerStartThreads[i].join();
    }

    _testingZKServer.close();
  }
}
