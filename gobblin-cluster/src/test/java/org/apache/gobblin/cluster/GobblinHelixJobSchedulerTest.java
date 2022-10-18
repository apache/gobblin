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
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.event.NewJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.UpdateJobConfigArrivalEvent;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import org.apache.gobblin.scheduler.SchedulerService;


/**
 * Unit tests for {@link org.apache.gobblin.cluster.GobblinHelixJobScheduler}.
 *
 */
@Test(groups = {"gobblin.cluster"})
public class GobblinHelixJobSchedulerTest {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinHelixJobSchedulerTest.class);

  private HelixManager helixManager;
  private FileSystem localFs;
  private Path appWorkDir;
  private final Closer closer = Closer.create();
  private Config baseConfig;

  private GobblinTaskRunner gobblinTaskRunner;

  private Thread thread;

  private final String workflowIdSuffix1 = "_1504201348471";
  private final String workflowIdSuffix2 = "_1504201348472";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    URL url = GobblinHelixJobSchedulerTest.class.getClassLoader()
        .getResource(GobblinHelixJobSchedulerTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.appWorkDir = new Path(GobblinHelixJobSchedulerTest.class.getSimpleName());

    // Prepare the source Json file
    File sourceJsonFile = new File(this.appWorkDir.toString(), TestHelper.TEST_JOB_NAME + ".json");
    TestHelper.createSourceJsonFile(sourceJsonFile);

    baseConfig = ConfigFactory.parseURL(url).withValue("gobblin.cluster.zk.connection.string",
        ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .withValue(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
            ConfigValueFactory.fromAnyRef(sourceJsonFile.getAbsolutePath()))
        .withValue(ConfigurationKeys.JOB_STATE_IN_STATE_STORE, ConfigValueFactory.fromAnyRef("true")).resolve();

    String zkConnectingString = baseConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = baseConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    HelixUtils.createGobblinHelixCluster(zkConnectingString, helixClusterName);

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER,
            zkConnectingString);
    this.closer.register(() -> helixManager.disconnect());
    this.helixManager.connect();

    this.localFs = FileSystem.getLocal(new Configuration());

    this.closer.register(() -> {
      if (localFs.exists(appWorkDir)) {
        localFs.delete(appWorkDir, true);
      }
    });

    this.closer.register(() -> {
      if (localFs.exists(appWorkDir)) {
        localFs.delete(appWorkDir, true);
      }
    });

    this.gobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_HELIX_INSTANCE_NAME,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, baseConfig, Optional.of(appWorkDir));

    this.thread = new Thread(() -> gobblinTaskRunner.start());
    this.thread.start();
  }

  @Test
  public void testNewJobAndUpdate()
      throws Exception {
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        ConfigValueFactory.fromAnyRef("/tmp/" + GobblinHelixJobScheduler.class.getSimpleName()));
    SchedulerService schedulerService = new SchedulerService(new Properties());
    NonObservingFSJobCatalog jobCatalog = new NonObservingFSJobCatalog(config);
    jobCatalog.startAsync();
    GobblinHelixJobScheduler jobScheduler =
        new GobblinHelixJobScheduler(ConfigFactory.empty(), this.helixManager, java.util.Optional.empty(),
            new EventBus(), appWorkDir, Lists.emptyList(), schedulerService, jobCatalog);

    final Properties properties1 =
        GobblinHelixJobLauncherTest.generateJobProperties(this.baseConfig, "1", workflowIdSuffix1);
    properties1.setProperty(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, "true");

    NewJobConfigArrivalEvent newJobConfigArrivalEvent =
        new NewJobConfigArrivalEvent(properties1.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties1);
    jobScheduler.handleNewJobConfigArrival(newJobConfigArrivalEvent);
    properties1.setProperty(ConfigurationKeys.JOB_ID_KEY,
        "job_" + properties1.getProperty(ConfigurationKeys.JOB_NAME_KEY) + workflowIdSuffix2);
    Map<String, String> workflowIdMap;
    this.helixManager.connect();

    String workFlowId = null;
    long endTime = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < endTime) {
      workflowIdMap = HelixUtils.getWorkflowIdsFromJobNames(this.helixManager,
          Collections.singletonList(newJobConfigArrivalEvent.getJobName()));
      if (workflowIdMap.containsKey(newJobConfigArrivalEvent.getJobName())) {
        workFlowId = workflowIdMap.get(newJobConfigArrivalEvent.getJobName());
        break;
      }
      Thread.sleep(100);
    }
    Assert.assertNotNull(workFlowId);
    Assert.assertTrue(workFlowId.endsWith(workflowIdSuffix1));

    jobScheduler.handleUpdateJobConfigArrival(
        new UpdateJobConfigArrivalEvent(properties1.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties1));
    this.helixManager.connect();
    endTime = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < endTime) {
      workflowIdMap = HelixUtils.getWorkflowIdsFromJobNames(this.helixManager,
          Collections.singletonList(newJobConfigArrivalEvent.getJobName()));
      if (workflowIdMap.containsKey(newJobConfigArrivalEvent.getJobName())) {
        workFlowId = workflowIdMap.get(newJobConfigArrivalEvent.getJobName());
        break;
      }
      Thread.sleep(100);
    }
    Assert.assertTrue(workFlowId.endsWith(workflowIdSuffix2));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
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
