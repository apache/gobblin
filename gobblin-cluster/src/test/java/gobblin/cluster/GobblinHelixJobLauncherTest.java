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

import gobblin.metastore.DatasetStateStore;
import gobblin.util.ClassAliasResolver;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.curator.test.TestingServer;
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.Tag;
import gobblin.runtime.FsDatasetStateStore;
import gobblin.runtime.JobException;
import gobblin.runtime.JobState;
import gobblin.util.ConfigUtils;


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

    this.gobblinHelixJobLauncher = this.closer.register(
        new GobblinHelixJobLauncher(properties, this.helixManager, this.appWorkDir, ImmutableList.<Tag<?>>of()));

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
