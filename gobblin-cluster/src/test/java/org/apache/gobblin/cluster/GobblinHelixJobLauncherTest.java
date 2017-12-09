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

  private String jobName;

  private File jobOutputFile;

  private GobblinHelixJobLauncher gobblinHelixJobLauncher;

  private GobblinHelixJobLauncher gobblinHelixJobLauncher1;

  private GobblinHelixJobLauncher gobblinHelixJobLauncher2;

  private GobblinTaskRunner gobblinTaskRunner;

  private DatasetStateStore datasetStateStore;

  private Thread thread;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    URL url = GobblinHelixJobLauncherTest.class.getClassLoader().getResource(
        GobblinHelixJobLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .resolve();

    String zkConnectingString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

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

    Properties properties = ConfigUtils.configToProperties(config);

    this.localFs = FileSystem.getLocal(new Configuration());

    this.appWorkDir = new Path(GobblinHelixJobLauncherTest.class.getSimpleName());
    this.closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        if (localFs.exists(appWorkDir)) {
          localFs.delete(appWorkDir, true);
        }
      }
    });

    this.jobName = config.getString(ConfigurationKeys.JOB_NAME_KEY);

    this.jobOutputFile = new File(config.getString(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
        config.getString(ConfigurationKeys.WRITER_FILE_PATH) + File.separator + config
            .getString(ConfigurationKeys.WRITER_FILE_NAME));

    // Prepare the source Json file
    File sourceJsonFile = new File(this.appWorkDir.toString(), TestHelper.TEST_JOB_NAME + ".json");
    TestHelper.createSourceJsonFile(sourceJsonFile);
    properties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, sourceJsonFile.getAbsolutePath());

    ConcurrentHashMap<String, Boolean> runningMap = new ConcurrentHashMap<>();

    // Normal job launcher
    properties.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_" + this.jobName + "_1504201348470");
    this.gobblinHelixJobLauncher = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    // Job launcher(1) to test parallel job running
    properties.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_" + this.jobName + "_1504201348471");
    this.gobblinHelixJobLauncher1 = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    // Job launcher(2) to test parallel job running
    properties.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_" + this.jobName + "_1504201348472");
    this.gobblinHelixJobLauncher2 = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of(), runningMap));

    this.gobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_HELIX_INSTANCE_NAME,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, config, Optional.of(appWorkDir));

    String stateStoreType = properties.getProperty(ConfigurationKeys.STATE_STORE_TYPE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    DatasetStateStore.Factory stateStoreFactory =
        resolver.resolveClass(stateStoreType).newInstance();

    this.datasetStateStore = stateStoreFactory.createStateStore(config);

    this.thread = new Thread(new Runnable() {
      @Override
      public void run() {
        gobblinTaskRunner.start();
      }
    });
    this.thread.start();
  }

  public void testLaunchJob() throws JobException, IOException {
    this.gobblinHelixJobLauncher.launchJob(null);

    Assert.assertTrue(this.jobOutputFile.exists());

    Schema schema = new Schema.Parser().parse(TestHelper.SOURCE_SCHEMA);
    TestHelper.assertGenericRecords(this.jobOutputFile, schema);

    List<JobState.DatasetState> datasetStates = this.datasetStateStore.getAll(this.jobName,
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

  public void testLaunchMultipleJobs() throws JobException, IOException, InterruptedException {
    CountDownLatch stg1 = new CountDownLatch(1);
    CountDownLatch stg2 = new CountDownLatch(1);
    CountDownLatch stg3 = new CountDownLatch(1);
    SuspendJobListener testListener = new SuspendJobListener(stg1, stg2);
    (new Thread(() -> {
      try {
        GobblinHelixJobLauncherTest.this.gobblinHelixJobLauncher1.launchJob(testListener);
        stg3.countDown();
      } catch (JobException e) {
      }
    })).start();

    // Wait for the first job to start
    stg1.await();
    // When first job is in the middle of running, launch the second job (which should do NOOP because previous job is still running)
    this.gobblinHelixJobLauncher2.launchJob(testListener);
    stg2.countDown();
    // Wait for the first job to finish
    stg3.await();
    Assert.assertEquals(testListener.getCompletes().get() == 1, true);
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
}
