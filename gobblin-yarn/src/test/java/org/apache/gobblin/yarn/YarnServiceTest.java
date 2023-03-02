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
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;

import static org.mockito.Mockito.*;


/**
 * Tests for {@link YarnService}.
 */
public class YarnServiceTest {
  final Logger LOG = LoggerFactory.getLogger(YarnServiceTest.class);
  private TestYarnService yarnService;
  private Config config;
  private YarnConfiguration clusterConf = new YarnConfiguration();
  private final EventBus eventBus = new EventBus("YarnServiceTest");

  AMRMClientAsync mockAMRMClient;
  RegisterApplicationMasterResponse mockRegisterApplicationMasterResponse;
  Resource mockResource;
  FileSystem mockFs;

  @BeforeClass
  public void setUp() throws Exception {
    mockAMRMClient = mock(AMRMClientAsync.class);
    mockRegisterApplicationMasterResponse = mock(RegisterApplicationMasterResponse.class);
    mockResource = mock(Resource.class);
    mockFs = mock(FileSystem.class);

    URL url = YarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url).resolve();

    MockedStatic<AMRMClientAsync> amrmClientAsyncMockStatic = mockStatic(AMRMClientAsync.class);
    MockedStatic<AMRMClientAsyncImpl> amrmClientAsyncImplMockStatic = mockStatic(AMRMClientAsyncImpl.class);

    amrmClientAsyncMockStatic.when(() -> AMRMClientAsync.createAMRMClientAsync(anyInt(), any(AMRMClientAsync.CallbackHandler.class)))
        .thenReturn(mockAMRMClient);
    doNothing().when(mockAMRMClient).init(any(YarnConfiguration.class));

    when(mockAMRMClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegisterApplicationMasterResponse);
    when(mockRegisterApplicationMasterResponse.getMaximumResourceCapability())
        .thenReturn(mockResource);

    // Create the test yarn service, but don't start yet
    this.yarnService = new TestYarnService(this.config, "testApp", "appId",
        this.clusterConf, mockFs, this.eventBus);
  }

  /**
   * Testing the race condition between the yarn start up and creating yarn container request
   * Block on creating new yarn containers until start up of the yarn service and purging is complete
   */
  @Test(groups = {"gobblin.yarn"})
  public void testYarnStartUpFirst() throws Exception{
    // Not allowed to request target number of containers since yarnService hasn't started up yet.
    Assert.assertFalse(this.yarnService.requestTargetNumberOfContainers(new YarnContainerRequestBundle(), Collections.EMPTY_SET));

    // Start the yarn service
    this.yarnService.startUp();

    // Allowed to request target number of containers after yarnService is started up.
    Assert.assertTrue(this.yarnService.requestTargetNumberOfContainers(new YarnContainerRequestBundle(), Collections.EMPTY_SET));
  }

  static class TestYarnService extends YarnService {
    public TestYarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
        FileSystem fs, EventBus eventBus) throws Exception {
      super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus, getMockHelixManager(config), getMockHelixAdmin());
    }

    private static HelixManager getMockHelixManager(Config config) {
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getClusterName()).thenReturn(config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));
      when(helixManager.getMetadataStoreConnectionString()).thenReturn("stub");
      return helixManager;
    }

    private static HelixAdmin getMockHelixAdmin() { return mock(HelixAdmin.class); }

    protected ContainerLaunchContext newContainerLaunchContext(ContainerInfo containerInfo)
        throws IOException {
      return BuilderUtils.newContainerLaunchContext(Collections.emptyMap(), Collections.emptyMap(),
          Arrays.asList("sleep", "60000"), Collections.emptyMap(), null, Collections.emptyMap());
    }

    @Override
    protected ByteBuffer getSecurityTokens() throws IOException { return mock(ByteBuffer.class); }
  }
}
