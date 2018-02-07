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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.curator.test.TestingServer;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.listeners.AbstractJobListener;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.FsDatasetStateStore;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Getter;


/**
 * Unit tests for {@link GobblinHelixJobLauncher}.
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. This class
 *   also uses the {@link HelixManager} to act as a testing Helix controller to be passed into the
 *   {@link GobblinHelixJobLauncher} instance. A {@link GobblinTaskRunner} is also used to run
 *   the single task of the test job. A {@link FsDatasetStateStore} is used to check the state store
 *   after the job is done. The test job writes everything to the local file system as returned by
 *   {@link FileSystem#getLocal(Configuration)}.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class GobblinHelixJobLauncherTest {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinHelixJobLauncherTest.class);

  private HelixManager helixManager;

  private FileSystem localFs;

  private Path appWorkDir;

  private GobblinTaskRunner gobblinTaskRunner;

  private DatasetStateStore datasetStateStore;

  private Thread thread;

  private final Closer closer = Closer.create();

  private Config baseConfig;

  @BeforeClass
  public void setUp() throws Exception {
    TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    URL url = GobblinHelixJobLauncherTest.class.getClassLoader().getResource(
        GobblinHelixJobLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.appWorkDir = new Path(GobblinHelixJobLauncherTest.class.getSimpleName());

    // Prepare the source Json file
    File sourceJsonFile = new File(this.appWorkDir.toString(), TestHelper.TEST_JOB_NAME + ".json");
    TestHelper.createSourceJsonFile(sourceJsonFile);

    baseConfig = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .withValue(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
            ConfigValueFactory.fromAnyRef(sourceJsonFile.getAbsolutePath()))
        .withValue(ConfigurationKeys.JOB_STATE_IN_STATE_STORE, ConfigValueFactory.fromAnyRef("true"))
        .resolve();

    String zkConnectingString = baseConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = baseConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    HelixUtils.createGobblinHelixCluster(zkConnectingString, helixClusterName);

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER,
            zkConnectingString);
    this.closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        helixManager.disconnect();
      }
    });
    this.helixManager.connect();

    this.localFs = FileSystem.getLocal(new Configuration());

    this.closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        if (localFs.exists(appWorkDir)) {
          localFs.delete(appWorkDir, true);
        }
      }
    });

    this.gobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_HELIX_INSTANCE_NAME,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, baseConfig, Optional.of(appWorkDir));

    String stateStoreType = ConfigUtils.getString(baseConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    DatasetStateStore.Factory stateStoreFactory =
        resolver.resolveClass(stateStoreType).newInstance();

    this.datasetStateStore = stateStoreFactory.createStateStore(baseConfig);

    this.thread = new Thread(new Runnable() {
      @Override
      public void run() {
        gobblinTaskRunner.start();
      }
    });
    this.thread.start();
  }

  private Properties generateJobProperties(Config baseConfig, String jobNameSuffix, String jobIdSuffix) {
    Properties properties = ConfigUtils.configToProperties(baseConfig);

    String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY) + jobNameSuffix;

    properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, jobName);

    properties.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_" + jobName + jobIdSuffix);

    properties.setProperty(ConfigurationKeys.WRITER_FILE_PATH, jobName);

    return properties;
  }

  private File getJobOutputFile(Properties properties) {
    return new File(properties.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
        properties.getProperty(ConfigurationKeys.WRITER_FILE_PATH) + File.separator + properties
            .getProperty(ConfigurationKeys.WRITER_FILE_NAME));
  }

  public void testLaunchJob() throws Exception {
    final ConcurrentHashMap<String, Boolean> runningMap = new ConcurrentHashMap<>();

    // Normal job launcher
    final Properties properties = generateJobProperties(this.baseConfig, "1", "_1504201348470");
    final GobblinHelixJobLauncher gobblinHelixJobLauncher = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    gobblinHelixJobLauncher.launchJob(null);

    final File jobOutputFile = getJobOutputFile(properties);
    Assert.assertTrue(jobOutputFile.exists());

    Schema schema = new Schema.Parser().parse(TestHelper.SOURCE_SCHEMA);
    TestHelper.assertGenericRecords(jobOutputFile, schema);

    List<JobState.DatasetState> datasetStates = this.datasetStateStore.getAll(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
        FsDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + FsDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX);
    Assert.assertEquals(datasetStates.size(), 1);
    JobState.DatasetState datasetState = datasetStates.get(0);
    Assert.assertEquals(datasetState.getCompletedTasks(), 1);
    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);

    Assert.assertEquals(datasetState.getTaskStates().size(), 1);
    Assert.assertEquals(datasetState.getTaskStates().get(0).getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
  }

  private static class SuspendJobListener extends AbstractJobListener {
    @Getter
    private AtomicInteger completes = new AtomicInteger();
    private CountDownLatch stg1;
    private CountDownLatch stg2;
    public SuspendJobListener (CountDownLatch stg1, CountDownLatch stg2) {
      this.stg1 = stg1;
      this.stg2 = stg2;
    }

    @Override
    public void onJobStart (JobContext jobContext) throws Exception {
      stg1.countDown();
      stg2.await();
    }

    @Override
    public void onJobCompletion(JobContext jobContext) throws Exception {
      completes.addAndGet(1);
    }
  }

  public void testLaunchMultipleJobs() throws Exception {
    final ConcurrentHashMap<String, Boolean> runningMap = new ConcurrentHashMap<>();

    // Job launcher(1) to test parallel job running
    final Properties properties1 = generateJobProperties(this.baseConfig, "2", "_1504201348471");
    final GobblinHelixJobLauncher gobblinHelixJobLauncher1 = this.closer.register(
        new GobblinHelixJobLauncher(properties1, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    // Job launcher(2) to test parallel job running
    final Properties properties2 = generateJobProperties(this.baseConfig, "2", "_1504201348472");
    final GobblinHelixJobLauncher gobblinHelixJobLauncher2 = this.closer.register(
        new GobblinHelixJobLauncher(properties2, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    CountDownLatch stg1 = new CountDownLatch(1);
    CountDownLatch stg2 = new CountDownLatch(1);
    CountDownLatch stg3 = new CountDownLatch(1);
    SuspendJobListener testListener = new SuspendJobListener(stg1, stg2);
    (new Thread(() -> {
      try {
        gobblinHelixJobLauncher1.launchJob(testListener);
        stg3.countDown();
      } catch (JobException e) {
      }
    })).start();

    // Wait for the first job to start
    stg1.await();
    // When first job is in the middle of running, launch the second job (which should do NOOP because previous job is still running)
    gobblinHelixJobLauncher2.launchJob(testListener);
    stg2.countDown();
    // Wait for the first job to finish
    stg3.await();
    Assert.assertEquals(testListener.getCompletes().get() == 1, true);
  }

  public void testJobCleanup() throws Exception {
    final ConcurrentHashMap<String, Boolean> runningMap = new ConcurrentHashMap<>();

    final Properties properties = generateJobProperties(this.baseConfig, "3", "_1504201348473");
    final GobblinHelixJobLauncher gobblinHelixJobLauncher =
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap);

    final Properties properties2 = generateJobProperties(this.baseConfig, "33", "_1504201348474");
    final GobblinHelixJobLauncher gobblinHelixJobLauncher2 =
        new GobblinHelixJobLauncher(properties2, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap);

    gobblinHelixJobLauncher.launchJob(null);
    gobblinHelixJobLauncher2.launchJob(null);

    final TaskDriver taskDriver = new TaskDriver(this.helixManager);

    final String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    final String jobIdKey = properties.getProperty(ConfigurationKeys.JOB_ID_KEY);
    final String jobContextName = jobName + "_" + jobIdKey;
    final String jobName2 = properties2.getProperty(ConfigurationKeys.JOB_NAME_KEY);

    org.apache.helix.task.JobContext jobContext = taskDriver.getJobContext(jobContextName);

    // job context should be present until close
    Assert.assertNotNull(jobContext);

    gobblinHelixJobLauncher.close();

    // job queue deleted asynchronously after close
    waitForQueueCleanup(taskDriver, jobName);

    jobContext = taskDriver.getJobContext(jobContextName);

    // job context should have been deleted
    Assert.assertNull(jobContext);

    // job queue should have been deleted
    WorkflowConfig workflowConfig  = taskDriver.getWorkflowConfig(jobName);
    Assert.assertNull(workflowConfig);

    WorkflowContext workflowContext = taskDriver.getWorkflowContext(jobName);
    Assert.assertNull(workflowContext);

    // second job queue with shared prefix should not be deleted when the first job queue is cleaned up
    workflowConfig  = taskDriver.getWorkflowConfig(jobName2);
    Assert.assertNotNull(workflowConfig);

    gobblinHelixJobLauncher2.close();

    // job queue deleted asynchronously after close
    waitForQueueCleanup(taskDriver, jobName2);

    workflowConfig  = taskDriver.getWorkflowConfig(jobName2);
    Assert.assertNull(workflowConfig);

    // check that workunit and taskstate directory for the job are cleaned up
    final File workunitsDir =
        new File(this.appWorkDir + File.separator + GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME
        + File.separator + jobIdKey);

    final File taskstatesDir =
        new File(this.appWorkDir + File.separator + GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME
            + File.separator + jobIdKey);

    Assert.assertFalse(workunitsDir.exists());
    Assert.assertFalse(taskstatesDir.exists());

    // check that job.state file is cleaned up
    final File jobStateFile = new File(GobblinClusterUtils.getJobStateFilePath(true, this.appWorkDir, jobIdKey).toString());

    Assert.assertFalse(jobStateFile.exists());
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      this.gobblinTaskRunner.stop();
      this.thread.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      this.closer.close();
    }
  }

   private void waitForQueueCleanup(TaskDriver taskDriver, String queueName) {
     for (int i = 0; i < 60; i++) {
       WorkflowConfig workflowConfig  = taskDriver.getWorkflowConfig(queueName);

       if (workflowConfig == null) {
         break;
       }

       try {
         Thread.sleep(1000);
       } catch (InterruptedException e) {
       }
     }
  }
}
