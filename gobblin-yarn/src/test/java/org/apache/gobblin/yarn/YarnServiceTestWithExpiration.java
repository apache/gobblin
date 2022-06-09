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

package org.apache.gobblin.yarn;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.testing.AssertWithBackoff;


/**
 * Tests for {@link YarnService}.
 */
@Test(groups = {"gobblin.yarn", "disabledOnCI"})
public class YarnServiceTestWithExpiration {
  final Logger LOG = LoggerFactory.getLogger(YarnServiceTest.class);

  private YarnClient yarnClient;
  private MiniYARNCluster yarnCluster;
  private TestExpiredYarnService expiredYarnService;
  private Config config;
  private YarnConfiguration clusterConf;
  private ApplicationId applicationId;
  private ApplicationAttemptId applicationAttemptId;
  private final EventBus eventBus = new EventBus("YarnServiceTest");

  private final Closer closer = Closer.create();

  private static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Set java home in environment since it isn't set on some systems
    String javaHome = System.getProperty("java.home");
    setEnv("JAVA_HOME", javaHome);

    this.clusterConf = new YarnConfiguration();
    this.clusterConf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "100");
    this.clusterConf.set(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, "10000");
    this.clusterConf.set(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS, "60000");
    this.clusterConf.set(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS, "1000");

    this.yarnCluster =
        this.closer.register(new MiniYARNCluster("YarnServiceTestCluster", 4, 1,
            1));
    this.yarnCluster.init(this.clusterConf);
    this.yarnCluster.start();

    // YARN client should not be started before the Resource Manager is up
    AssertWithBackoff.create().logger(LOG).timeoutMs(10000)
        .assertTrue(new Predicate<Void>() {
          @Override public boolean apply(Void input) {
            return !clusterConf.get(YarnConfiguration.RM_ADDRESS).contains(":0");
          }
        }, "Waiting for RM");

    this.yarnClient = this.closer.register(YarnClient.createYarnClient());
    this.yarnClient.init(this.clusterConf);
    this.yarnClient.start();

    URL url = YarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url).resolve();

    // Start a dummy application manager so that the YarnService can use the AM-RM token.
    startApp();

    // create and start the test yarn service
    this.expiredYarnService = new TestExpiredYarnService(this.config, "testApp", "appId",
        this.clusterConf,
        FileSystem.getLocal(new Configuration()), this.eventBus);

    this.expiredYarnService.startUp();
  }

  private void startApp() throws Exception {
    // submit a dummy app
    ApplicationSubmissionContext appSubmissionContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    this.applicationId = appSubmissionContext.getApplicationId();

    ContainerLaunchContext containerLaunchContext =
        BuilderUtils.newContainerLaunchContext(Collections.emptyMap(), Collections.emptyMap(),
            Arrays.asList("sleep", "100"), Collections.emptyMap(), null, Collections.emptyMap());

    // Setup the application submission context
    appSubmissionContext.setApplicationName("TestApp");
    appSubmissionContext.setResource(Resource.newInstance(128, 1));
    appSubmissionContext.setPriority(Priority.newInstance(0));
    appSubmissionContext.setAMContainerSpec(containerLaunchContext);

    this.yarnClient.submitApplication(appSubmissionContext);

    // wait for application to be accepted
    int i;
    RMAppAttempt attempt = null;
    for (i = 0; i < 120; i++) {
      ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);

      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        this.applicationAttemptId = appReport.getCurrentApplicationAttemptId();
        attempt = yarnCluster.getResourceManager().getRMContext().getRMApps()
            .get(appReport.getCurrentApplicationAttemptId().getApplicationId()).getCurrentAppAttempt();
        break;
      }
      Thread.sleep(1000);
    }

    Assert.assertTrue(i < 120, "timed out waiting for ACCEPTED state");

    // Set the AM-RM token in the UGI for access during testing
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(UserGroupInformation.getCurrentUser()
        .getUserName()));
    UserGroupInformation.getCurrentUser().addToken(attempt.getAMRMToken());
  }

  @AfterClass
  public void tearDown() throws IOException, TimeoutException, YarnException {
    try {
      this.yarnClient.killApplication(this.applicationAttemptId.getApplicationId());
      this.expiredYarnService.shutDown();
      Assert.assertEquals(this.expiredYarnService.getContainerMap().size(), 0);
    } finally {
      this.closer.close();
    }
  }

  /**
   * Test that the yarn service can handle onStartContainerError right
   */

  @Test(groups = {"gobblin.yarn", "disabledOnCI"})
  public void testStartError() throws Exception{
    Resource resource = Resource.newInstance(16, 1);
    this.expiredYarnService.requestTargetNumberOfContainers(
        GobblinYarnTestUtils.createYarnContainerRequest(10, resource), Collections.EMPTY_SET);

    Assert.assertFalse(this.expiredYarnService.getMatchingRequestsList(resource).isEmpty());

    AssertWithBackoff.create().logger(LOG).timeoutMs(60000).maxSleepMs(2000).backoffFactor(1.5)
        .assertTrue(new Predicate<Void>() {
          @Override
          public boolean apply(Void input) {
            //Since it may retry to request the container and start again, so the number may lager than 10
            return expiredYarnService.completedContainers.size() >= 10
                && expiredYarnService.startErrorContainers.size() >= 10;
          }
        }, "Waiting for container completed");

  }

  private static class TestExpiredYarnService extends YarnServiceTest.TestYarnService {
    public HashSet<ContainerId> startErrorContainers = new HashSet<>();
    public HashSet<ContainerStatus> completedContainers = new HashSet<>();
    public TestExpiredYarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
        FileSystem fs, EventBus eventBus) throws Exception {
      super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus);
    }

    @Override
    protected NMClientCallbackHandler getNMClientCallbackHandler() {
      return new TestNMClientCallbackHandler();
    }

    @Override
    protected void handleContainerCompletion(ContainerStatus containerStatus){
      super.handleContainerCompletion(containerStatus);
      completedContainers.add(containerStatus);
    }

    protected ContainerLaunchContext newContainerLaunchContext(ContainerInfo containerInfo)
        throws IOException {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return BuilderUtils.newContainerLaunchContext(Collections.emptyMap(), Collections.emptyMap(),
          Arrays.asList("sleep", "600"), Collections.emptyMap(), null, Collections.emptyMap());
    }
    private class TestNMClientCallbackHandler extends YarnService.NMClientCallbackHandler {
      @Override
      public void onStartContainerError(ContainerId containerId, Throwable t) {
        startErrorContainers.add(containerId);
      }
    }
  }
}