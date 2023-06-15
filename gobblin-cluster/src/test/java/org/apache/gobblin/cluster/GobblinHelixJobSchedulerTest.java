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
import java.nio.file.Files;
import java.time.Instant;
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
import org.mockito.MockedStatic;
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

import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link org.apache.gobblin.cluster.GobblinHelixJobScheduler}.
 *
 */
@Test(groups = {"gobblin.cluster"})
public class GobblinHelixJobSchedulerTest {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinHelixJobSchedulerTest.class);

  private FileSystem localFs;
  private Path appWorkDir;
  private final Closer closer = Closer.create();
  private Config baseConfig;

  private GobblinTaskRunner gobblinTaskRunner;

  private Thread thread;
  private final String workflowIdSuffix1 = "_1504201348471";
  private final String workflowIdSuffix2 = "_1504201348472";
  private final String workflowIdSuffix3 = "_1504201348473";

  private Instant beginTime = Instant.ofEpochMilli(0);
  private Instant withinThrottlePeriod = Instant.ofEpochMilli(1);
  private Instant exceedsThrottlePeriod = Instant.ofEpochMilli(3600001);

  private String zkConnectingString;
  private String helixClusterName;

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

    this.zkConnectingString = baseConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    this.helixClusterName = baseConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    HelixUtils.createGobblinHelixCluster(zkConnectingString, helixClusterName);

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

  // Time span exceeds throttle timeout, within same workflow, throttle is enabled
  // Job will be updated
  @Test
  public void testNewJobAndUpdate()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateSameWorkflowLongPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2, workflowIdSuffix2,
        true, true);
  }

  // Time span is within throttle timeout, within same workflow, throttle is enabled
  // Job will not be updated
  @Test
  public void testUpdateSameWorkflowShortPeriodThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateSameWorkflowShortPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2, workflowIdSuffix1,
        true, true);
  }

  // Time span exceeds throttle timeout, within same workflow, throttle is not enabled
  // Job will be updated
  @Test
  public void testUpdateSameWorkflowLongPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateSameWorkflowLongPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2, workflowIdSuffix2,
        false, true);
  }

  // Time span is within throttle timeout, within same workflow, throttle is not enabled
  // Job will be updated
  @Test
  public void testUpdateSameWorkflowShortPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateSameWorkflowShortPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2, workflowIdSuffix2,
        false, true);
  }

  // Time span is within throttle timeout, within different workflow, throttle is enabled
  // Job will be updated
  public void testUpdateDiffWorkflowShortPeriodThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateDiffWorkflowShortPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix3, workflowIdSuffix3,
        true, false);
  }

  // Time span is within throttle timeout, within different workflow, throttle is not enabled
  // Job will be updated
  @Test
  public void testUpdateDiffWorkflowShortPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateDiffWorkflowShortPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix3, workflowIdSuffix3,
        false, false);
  }

  // Time span exceeds throttle timeout, within different workflow, throttle is enabled
  // Job will be updated
  @Test
  public void testUpdateDiffWorkflowLongPeriodThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateDiffWorkflowLongPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix3, workflowIdSuffix3,
        true, false);
  }

  // Time span exceeds throttle timeout, within different workflow, throttle is not enabled
  // Job will be updated
  @Test
  public void testUpdateDiffWorkflowLongPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateDiffWorkflowLongPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix3, workflowIdSuffix3,
        false, false);
  }

  private GobblinHelixJobScheduler createJobScheduler(HelixManager helixManager) throws Exception {
    java.nio.file.Path p = Files.createTempDirectory(GobblinHelixJobScheduler.class.getSimpleName());
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        ConfigValueFactory.fromAnyRef(p.toString()));
    SchedulerService schedulerService = new SchedulerService(new Properties());
    NonObservingFSJobCatalog jobCatalog = new NonObservingFSJobCatalog(config);
    jobCatalog.startAsync();
    return new GobblinHelixJobScheduler(ConfigFactory.empty(), helixManager, java.util.Optional.empty(),
        new EventBus(), appWorkDir, Lists.emptyList(), schedulerService, jobCatalog);
  }

  private NewJobConfigArrivalEvent createJobConfigArrivalEvent(Properties properties, String suffix) {
    properties.setProperty(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, "true");
    NewJobConfigArrivalEvent newJobConfigArrivalEvent =
        new NewJobConfigArrivalEvent(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties);
    properties.setProperty(ConfigurationKeys.JOB_ID_KEY,
        "job_" + properties.getProperty(ConfigurationKeys.JOB_NAME_KEY) + suffix);
    return newJobConfigArrivalEvent;
  }

  private void connectAndAssertWorkflowId(String expectedSuffix, NewJobConfigArrivalEvent newJobConfigArrivalEvent, HelixManager helixManager ) throws Exception {
    helixManager.connect();
    String workFlowId = getWorkflowID(newJobConfigArrivalEvent, helixManager);
    Assert.assertNotNull(workFlowId);
    Assert.assertTrue(workFlowId.endsWith(expectedSuffix));
  }

  private String getWorkflowID (NewJobConfigArrivalEvent newJobConfigArrivalEvent, HelixManager helixManager )
      throws Exception {
    // endTime is manually set time period that we allow HelixUtils to fetch workflowIdMap before timeout
    long endTime = System.currentTimeMillis() + 30000;
    Map<String, String> workflowIdMap;
    while (System.currentTimeMillis() < endTime) {
      try{
        workflowIdMap = HelixUtils.getWorkflowIdsFromJobNames(helixManager,
            Collections.singletonList(newJobConfigArrivalEvent.getJobName()));
      } catch(GobblinHelixUnexpectedStateException e){
        continue;
      }
      if (workflowIdMap.containsKey(newJobConfigArrivalEvent.getJobName())) {
        return workflowIdMap.get(newJobConfigArrivalEvent.getJobName());
      }
      Thread.sleep(100);
    }
    return null;
  }

  private void runWorkflowTest(Instant mockedTime, String jobSuffix, String newJobWorkflowIdSuffix,
      String updateWorkflowIdSuffix1, String updateWorkflowIdSuffix2,
      String assertUpdateWorkflowIdSuffix, boolean isThrottleEnabled, boolean isSameWorkflow) throws Exception {
    try (MockedStatic<Instant> mocked = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
      mocked.when(Instant::now).thenReturn(beginTime, mockedTime);

      // helixManager is set to local variable to avoid the HelixManager (ZkClient) is not connected error across tests
      HelixManager helixManager = HelixManagerFactory
          .getZKHelixManager(helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER,
              zkConnectingString);
      GobblinHelixJobScheduler jobScheduler = createJobScheduler(helixManager);
      final Properties properties =
          GobblinHelixJobLauncherTest.generateJobProperties(
              this.baseConfig, jobSuffix, newJobWorkflowIdSuffix);
      NewJobConfigArrivalEvent newJobConfigArrivalEvent = createJobConfigArrivalEvent(properties, updateWorkflowIdSuffix1);
      jobScheduler.handleNewJobConfigArrival(newJobConfigArrivalEvent);
      connectAndAssertWorkflowId(newJobWorkflowIdSuffix, newJobConfigArrivalEvent, helixManager);

      if (isSameWorkflow) {
        properties.setProperty(GobblinClusterConfigurationKeys.HELIX_JOB_SCHEDULING_THROTTLE_ENABLED_KEY, String.valueOf(isThrottleEnabled));
        jobScheduler.handleUpdateJobConfigArrival(
            new UpdateJobConfigArrivalEvent(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties));
        connectAndAssertWorkflowId(assertUpdateWorkflowIdSuffix, newJobConfigArrivalEvent, helixManager);
      }
      else {
        final Properties properties2 =
            GobblinHelixJobLauncherTest.generateJobProperties(
                this.baseConfig, jobSuffix + '2', updateWorkflowIdSuffix2);
        NewJobConfigArrivalEvent newJobConfigArrivalEvent2 =
            new NewJobConfigArrivalEvent(properties2.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties2);
        properties2.setProperty(GobblinClusterConfigurationKeys.HELIX_JOB_SCHEDULING_THROTTLE_ENABLED_KEY, String.valueOf(isThrottleEnabled));
        jobScheduler.handleUpdateJobConfigArrival(
            new UpdateJobConfigArrivalEvent(properties2.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties2));
        connectAndAssertWorkflowId(assertUpdateWorkflowIdSuffix, newJobConfigArrivalEvent2, helixManager);
      }
    }
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
