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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
  private final static int NUM_MANAGERS = 2;
  private final static int NUM_WORKERS = 2;
  private GobblinClusterManager[] _clusterManagers;
  private GobblinTaskRunner[] _clusterWorkers;
  private Thread[] _workerStartThreads;

  private String _testDirPath;
  private String _jobDirPath;
  Config _config;

  /**
   * clean up and set up test directory
   */
  private void setupTestDir() throws IOException {
    _testDirPath = _config.getString("gobblin.cluster.work.dir");
    _jobDirPath = _config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY);

    // clean up test directory and create job dir
    File testDir = new File(_testDirPath);
    File jobDir = new File(_jobDirPath);

    if (testDir.exists()) {
      FileUtils.deleteDirectory(testDir);
    }

    jobDir.mkdirs();

    // copy job file from resource
    String jobFileName = GobblinClusterKillTest.class.getSimpleName() + "Job.conf";
    try (InputStream resourceStream = this.getClass().getClassLoader().getResourceAsStream(jobFileName)) {
      if (resourceStream == null) {
        throw new RuntimeException("Could not find job resource " + jobFileName);
      }
      File targetFile = new File(_jobDirPath + "/" + jobFileName);

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
        .withValue("gobblin.cluster.jobconf.fullyQualifiedPath",
            ConfigValueFactory.fromAnyRef("/tmp/gobblinClusterKillTest/job-conf"))
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

    final File testJobFile = new File(_jobDirPath + "/GobblinClusterKillTestJob.conf");

    // Job file should exist
    Assert.assertTrue(testJobFile.exists());

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        File writerOutputDir = getWriterOutputDir();

        if (writerOutputDir != null && writerOutputDir.exists()) {
          return FileUtils.listFiles(writerOutputDir, new String[]{"txt"}, true).size() >= 25;
        } else {
          return false;
        }
      }
    }, "Waiting for writer output");

    File writerOutputDir = getWriterOutputDir();
    LOG.info("{} matches found before disconnecting worker",
        FileUtils.listFiles(writerOutputDir, new String[]{"txt"}, true).size());

    _clusterWorkers[0].disconnectHelixManager();

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        File jobOutputDir = getJobOutputDir();

        if (jobOutputDir != null && jobOutputDir.exists()) {
          return FileUtils.listFiles(jobOutputDir, new String[]{"txt"}, true).size() >= 100;
        } else {
          return false;
        }
      }
    }, "Waiting for job-completion");

    // Job file should have been deleted
    Thread.sleep(5000);
    Assert.assertFalse(testJobFile.exists());
  }

  // The kill tests are unreliable on Travis
  @Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testKillWorker")
  public void testKillManager() throws IOException, TimeoutException, InterruptedException {
    Collection<File> matches = Collections.EMPTY_LIST;

    // reinitialize test directory
    setupTestDir();

    // kill a manager to cause leader election. New leader will schedule a new job.
    _clusterManagers[0].disconnectHelixManager();

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        File writerOutputDir = getWriterOutputDir();

        if (writerOutputDir != null && writerOutputDir.exists()) {
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
        File jobOutputDir = getJobOutputDir();

        if (jobOutputDir != null && jobOutputDir.exists()) {
          return FileUtils.listFiles(jobOutputDir, new String[]{"txt"}, true).size() >= 100;
        } else {
          return false;
        }
      }
    }, "Waiting for job-completion");

    // Job file should have been deleted
    Thread.sleep(5000);
    final File testJobFile = new File(_jobDirPath + "/GobblinClusterKillTestJob.conf");
    Assert.assertFalse(testJobFile.exists());
  }

  // The kill tests are unreliable on Travis
  @Test(groups = { "disabledOnTravis" }, enabled=true, dependsOnMethods = "testKillManager")
  public void testRestartManager() throws IOException, TimeoutException, InterruptedException {
    Collection<File> matches = Collections.EMPTY_LIST;
    // reinitialize test directory
    setupTestDir();

    // At this point there is one connected manager. Disconnect it and reconnect the other one to confirm that a manager
    // can continue to function after regaining leadership.
    _clusterManagers[1].disconnectHelixManager();

    // Should function after regaining leadership
    // need to reinitialize the heap manager and call handleLeadershipChange to shut down services in the test
    // since the leadership change is simulated
    _clusterManagers[0].initializeHelixManager();
    _clusterManagers[0].handleLeadershipChange(null);

    // reconnect to get leadership role
    _clusterManagers[0].connectHelixManager();

    AssertWithBackoff.create().logger(LOG).timeoutMs(ASSERT_TIMEOUT).maxSleepMs(ASSERT_MAX_SLEEP).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
          @Override
          public boolean apply(Void input) {
            File writerOutputDir = getWriterOutputDir();

            if (writerOutputDir != null && writerOutputDir.exists()) {
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
            File jobOutputDir = getJobOutputDir();

            if (jobOutputDir != null && jobOutputDir.exists()) {
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

  /**
   * Get a file that matches the glob pattern in the base directory
   * @param base directory to check
   * @param glob the glob pattern to match
   * @return a {@link File} if found, otherwise null
   */
  private File getFileFromGlob(String base, String glob) {
    try (DirectoryStream<java.nio.file.Path> dirStream = Files.newDirectoryStream(Paths.get(base), glob)) {

      Iterator<java.nio.file.Path> iter = dirStream.iterator();
      if (iter.hasNext()) {
        java.nio.file.Path path = iter.next();
        return path.toFile();
      } else {
        return null;
      }
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Find the writer output directory
   * @return a {@link File} if directory found, otherwise null
   */
  private File getWriterOutputDir() {
    File writerOutputJobDir = getFileFromGlob(_testDirPath + "/writer-output", "job*");
    File writerOutputDir = null;

    if (writerOutputJobDir != null) {
      writerOutputDir = new File(writerOutputJobDir, "gobblin/util/test/HelloWorldSource");
    }

    return writerOutputDir;
  }

  /**
   * Find the job output directory
   * @return a {@link File} if directory found, otherwise null
   */
  private File getJobOutputDir() {
    return getFileFromGlob(_testDirPath + "/job-output/gobblin/util/test/HelloWorldSource/",
        "*_append");
  }
}

