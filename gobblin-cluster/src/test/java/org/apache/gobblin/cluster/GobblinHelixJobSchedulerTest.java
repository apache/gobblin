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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.assertj.core.util.Lists;
import org.mockito.Mockito;
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

import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link org.apache.gobblin.cluster.GobblinHelixJobScheduler}.
 *
 * In all test cases, we use GobblinHelixManagerFactory instead of
 * HelixManagerFactory, and instantiate a local HelixManager per test to
 * provide isolation and prevent errors caused by the ZKClient being shared
 * (e.g. ZKClient is not connected exceptions).
 */
@Test(groups = {"gobblin.cluster"}, singleThreaded = true)
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

  private final Instant beginTime = Instant.EPOCH;
  private final Duration withinThrottlePeriod = Duration.of(1, ChronoUnit.SECONDS);
  private final Duration exceedsThrottlePeriod = Duration.of(
      GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_SCHEDULING_THROTTLE_TIMEOUT_SECONDS_KEY + 1, ChronoUnit.SECONDS);

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

  /***
   * Time span exceeds throttle timeout, within same workflow, throttle is enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateSameWorkflowLongPeriodThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateSameWorkflowLongPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        true, true);
  }

  /***
   * Time span is within throttle timeout, within same workflow, throttle is enabled
   * Job will not be updated
   * @throws Exception
   */
  @Test
  public void testUpdateSameWorkflowShortPeriodThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateSameWorkflowShortPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix1,
        true, true);
  }

  /***
   * Time span exceeds throttle timeout, within same workflow, throttle is not enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateSameWorkflowLongPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateSameWorkflowLongPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        false, true);
  }

  /***
   * Time span is within throttle timeout, within same workflow, throttle is not enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateSameWorkflowShortPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateSameWorkflowShortPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        false, true);
  }

  /***
   * Time span is within throttle timeout, within different workflow, throttle is enabled
   * Job will be updated
   * @throws Exception
   */
  public void testUpdateDiffWorkflowShortPeriodThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateDiffWorkflowShortPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        true, false);
  }

  /***
   * Time span is within throttle timeout, within different workflow, throttle is not enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateDiffWorkflowShortPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(withinThrottlePeriod, "UpdateDiffWorkflowShortPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        false, false);
  }

  /***
   * Time span exceeds throttle timeout, within different workflow, throttle is enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateDiffWorkflowLongPeriodThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateDiffWorkflowLongPeriodThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        true, false);
  }

  /***
   * Time span exceeds throttle timeout, within different workflow, throttle is not enabled
   * Job will be updated
   * @throws Exception
   */
  @Test
  public void testUpdateDiffWorkflowLongPeriodNoThrottle()
      throws Exception {
    runWorkflowTest(exceedsThrottlePeriod, "UpdateDiffWorkflowLongPeriodNoThrottle",
        workflowIdSuffix1, workflowIdSuffix2, workflowIdSuffix2,
        false, false);
  }

  private GobblinHelixJobScheduler createJobScheduler(HelixManager helixManager, boolean isThrottleEnabled, Clock clock) throws Exception {
    java.nio.file.Path p = Files.createTempDirectory(GobblinHelixJobScheduler.class.getSimpleName());
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
        ConfigValueFactory.fromAnyRef(p.toString()));
    SchedulerService schedulerService = new SchedulerService(new Properties());
    NonObservingFSJobCatalog jobCatalog = new NonObservingFSJobCatalog(config);
    jobCatalog.startAsync();
    Config helixJobSchedulerConfig = ConfigFactory.empty().withValue(GobblinClusterConfigurationKeys.HELIX_JOB_SCHEDULING_THROTTLE_ENABLED_KEY,
        ConfigValueFactory.fromAnyRef(isThrottleEnabled));
    GobblinHelixJobScheduler gobblinHelixJobScheduler = new GobblinHelixJobScheduler(helixJobSchedulerConfig, helixManager, java.util.Optional.empty(),
          new EventBus(), appWorkDir, Lists.emptyList(), schedulerService, jobCatalog, clock);
    return gobblinHelixJobScheduler;
  }

  private NewJobConfigArrivalEvent createJobConfigArrivalEvent(Properties properties) {
    properties.setProperty(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, "true");
    NewJobConfigArrivalEvent newJobConfigArrivalEvent =
        new NewJobConfigArrivalEvent(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties);
    return newJobConfigArrivalEvent;
  }

  private void connectAndAssertWorkflowId(String expectedSuffix, String jobName, HelixManager helixManager) throws Exception {
    helixManager.connect();
    String workFlowId = getWorkflowID(jobName, helixManager);
    Assert.assertNotNull(workFlowId);
    Assert.assertTrue(workFlowId.endsWith(expectedSuffix));
  }

  private String getWorkflowID (String jobName, HelixManager helixManager)
      throws Exception {
    // Poll helix for up to 30 seconds to fetch until a workflow with a matching job name exists in Helix and then return that workflowID
    long endTime = System.currentTimeMillis() + 30000;
    Map<String, String> workflowIdMap;
    while (System.currentTimeMillis() < endTime) {
      try{
        workflowIdMap = HelixUtils.getWorkflowIdsFromJobNames(helixManager,
            Collections.singletonList(jobName));
      } catch(GobblinHelixUnexpectedStateException e){
        continue;
      }
      if (workflowIdMap.containsKey(jobName)) {
        return workflowIdMap.get(jobName);
      }
      Thread.sleep(100);
    }
    return null;
  }

  private void runWorkflowTest(Duration mockStepAmountTime, String jobSuffix,
    String newJobWorkflowIdSuffix, String updateWorkflowIdSuffix,
    String assertUpdateWorkflowIdSuffix, boolean isThrottleEnabled, boolean isSameWorkflow) throws Exception {
    Clock mockClock = Mockito.mock(Clock.class);
    AtomicReference<Instant> nextInstant = new AtomicReference<>(beginTime);
    when(mockClock.instant()).thenAnswer(invocation -> nextInstant.getAndAccumulate(null, (currentInstant, x) -> currentInstant.plus(mockStepAmountTime)));

    // Use GobblinHelixManagerFactory instead of HelixManagerFactory to avoid the connection error
    // helixManager is set to local variable to avoid the HelixManager (ZkClient) is not connected error across tests
    HelixManager helixManager = GobblinHelixManagerFactory
        .getZKHelixManager(helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER,
            zkConnectingString);
    GobblinHelixJobScheduler jobScheduler = createJobScheduler(helixManager, isThrottleEnabled, mockClock);
    final Properties properties = GobblinHelixJobLauncherTest.generateJobProperties(this.baseConfig, jobSuffix, newJobWorkflowIdSuffix);
    NewJobConfigArrivalEvent newJobConfigArrivalEvent = createJobConfigArrivalEvent(properties);
    jobScheduler.handleNewJobConfigArrival(newJobConfigArrivalEvent);
    connectAndAssertWorkflowId(newJobWorkflowIdSuffix, newJobConfigArrivalEvent.getJobName(), helixManager);

    if (isSameWorkflow) {
      properties.setProperty(ConfigurationKeys.JOB_ID_KEY,
          "job_" + properties.getProperty(ConfigurationKeys.JOB_NAME_KEY) + updateWorkflowIdSuffix);
      jobScheduler.handleUpdateJobConfigArrival(
          new UpdateJobConfigArrivalEvent(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties));
      connectAndAssertWorkflowId(assertUpdateWorkflowIdSuffix, newJobConfigArrivalEvent.getJobName(), helixManager);
    }
    else {
      final Properties properties2 =
          GobblinHelixJobLauncherTest.generateJobProperties(
              this.baseConfig, jobSuffix + '2', updateWorkflowIdSuffix);
      NewJobConfigArrivalEvent newJobConfigArrivalEvent2 =
          new NewJobConfigArrivalEvent(properties2.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties2);
      jobScheduler.handleUpdateJobConfigArrival(
          new UpdateJobConfigArrivalEvent(properties2.getProperty(ConfigurationKeys.JOB_NAME_KEY), properties2));
      connectAndAssertWorkflowId(assertUpdateWorkflowIdSuffix, newJobConfigArrivalEvent2.getJobName(), helixManager);
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
