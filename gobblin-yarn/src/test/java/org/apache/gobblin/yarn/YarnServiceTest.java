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

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockObjectFactory;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;

import static org.mockito.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;


/**
 * Tests for {@link YarnService}.
 */
@PrepareForTest({AMRMClientAsync.class, RegisterApplicationMasterResponse.class})
@PowerMockIgnore({"javax.management.*"})
public class YarnServiceTest extends PowerMockTestCase{
  final Logger LOG = LoggerFactory.getLogger(YarnServiceTest.class);
  private TestYarnService yarnService;
  private Config config;
  private YarnConfiguration clusterConf = new YarnConfiguration();
  private final EventBus eventBus = new EventBus("YarnServiceTest");
  @Mock
  AMRMClientAsync mockAMRMClient;
  @Mock
  RegisterApplicationMasterResponse mockRegisterApplicationMasterResponse;
  @Mock
  Resource mockResource;

  @BeforeClass
  public void setUp() throws Exception {
    mockAMRMClient = Mockito.mock(AMRMClientAsync.class);
    mockRegisterApplicationMasterResponse = Mockito.mock(RegisterApplicationMasterResponse.class);
    mockResource = Mockito.mock(Resource.class);

    URL url = YarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url).resolve();

    PowerMockito.mockStatic(AMRMClientAsync.class);
    PowerMockito.mockStatic(AMRMClientAsyncImpl.class);

    when(AMRMClientAsync.createAMRMClientAsync(anyInt(), any(AMRMClientAsync.CallbackHandler.class)))
        .thenReturn(mockAMRMClient);
    doNothing().when(mockAMRMClient).init(any(YarnConfiguration.class));
    when(mockAMRMClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegisterApplicationMasterResponse);
    when(mockRegisterApplicationMasterResponse.getMaximumResourceCapability())
        .thenReturn(mockResource);
    FileSystem fs = Mockito.mock(FileSystem.class);

    // create and start the test yarn service
    this.yarnService = new TestYarnService(this.config, "testApp", "appId",
        this.clusterConf, fs, this.eventBus);
  }

  /**
   * Testing the race condition between the yarn start up and creating yarn container request
   * Block on creating new yarn containers until start up of the yarn service and purging is complete
   */
  @Test(groups = {"gobblin.yarn"})
  public void testYarnStartUpFirst() throws Exception{
    boolean canRequestNewContainers = this.yarnService.requestTargetNumberOfContainers(new YarnContainerRequestBundle(), Collections.EMPTY_SET);

    // Not allowed to request target number of containers since yarnService hasn't started up yet.
    Assert.assertFalse(canRequestNewContainers);

    this.yarnService.startUp();
    canRequestNewContainers = this.yarnService.requestTargetNumberOfContainers(new YarnContainerRequestBundle(), Collections.EMPTY_SET);

    // Allowed to request target number of containers after yarnService is started up.
    Assert.assertTrue(canRequestNewContainers);
  }

  static class TestYarnService extends YarnService {
    public TestYarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
        FileSystem fs, EventBus eventBus) throws Exception {
      super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus, getMockHelixManager(config), getMockHelixAdmin());
    }

    private static HelixManager getMockHelixManager(Config config) {
      HelixManager helixManager = Mockito.mock(HelixManager.class);
      HelixDataAccessor helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
      PropertyKey propertyKey = Mockito.mock(PropertyKey.class);
      PropertyKey.Builder propertyKeyBuilder = Mockito.mock(PropertyKey.Builder.class);

      Mockito.when(helixManager.getInstanceName()).thenReturn("helixInstance1");
      Mockito.when(helixManager.getClusterName()).thenReturn(config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

      Mockito.when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
      Mockito.when(helixManager.getMetadataStoreConnectionString()).thenReturn("stub");
      Mockito.when(helixDataAccessor.keyBuilder()).thenReturn(propertyKeyBuilder);
      Mockito.when(propertyKeyBuilder.liveInstance(Mockito.anyString())).thenReturn(propertyKey);
      Mockito.when(helixDataAccessor.getProperty(propertyKey)).thenReturn(null);

      return helixManager;
    }

    private static HelixAdmin getMockHelixAdmin() {
      HelixAdmin helixAdmin = Mockito.mock(HelixAdmin.class);
      Mockito.doNothing().when(helixAdmin).purgeOfflineInstances(Mockito.anyString(), Mockito.anyLong());
      Mockito.doNothing().when(helixAdmin).enableInstance(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean());
      return helixAdmin;
    }

    protected ContainerLaunchContext newContainerLaunchContext(ContainerInfo containerInfo)
        throws IOException {
      return BuilderUtils.newContainerLaunchContext(Collections.emptyMap(), Collections.emptyMap(),
          Arrays.asList("sleep", "60000"), Collections.emptyMap(), null, Collections.emptyMap());
    }

    /**
     * Get the list of matching container requests for the specified resource memory and cores.
     */
    public List<? extends Collection<AMRMClient.ContainerRequest>> getMatchingRequestsList(Resource resource) {
      Priority priority = Priority.newInstance(0);
      return getAmrmClientAsync().getMatchingRequests(priority, ResourceRequest.ANY, resource);
    }
    @Override
    protected ByteBuffer getSecurityTokens() throws IOException {
      return mock(ByteBuffer.class);
    }
  }

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new PowerMockObjectFactory();
  }
}
