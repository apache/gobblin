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

package org.apache.gobblin.temporal.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.DummyScalingDirectiveSource;

/** Tests for {@link AbstractDynamicScalingYarnServiceManager}*/
public class DynamicScalingYarnServiceManagerTest {

  @Mock private DynamicScalingYarnService mockDynamicScalingYarnService;
  @Mock private ScalingDirectiveSource mockScalingDirectiveSource;
  @Mock private GobblinTemporalApplicationMaster mockGobblinTemporalApplicationMaster;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    // Using 1 second as polling interval so that the test runs faster and
    // GetScalingDirectivesRunnable.run() will be called equal to amount of sleep introduced between startUp
    // and shutDown in seconds
    Config config = ConfigFactory.empty().withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_POLLING_INTERVAL_SECS, ConfigValueFactory.fromAnyRef(1));
    Mockito.when(mockGobblinTemporalApplicationMaster.getConfig()).thenReturn(config);
    Mockito.when(mockGobblinTemporalApplicationMaster.get_yarnService()).thenReturn(mockDynamicScalingYarnService);
  }

  @Test
  public void testWhenScalingDirectivesIsNulOrEmpty() throws IOException, InterruptedException {
    Mockito.when(mockScalingDirectiveSource.getScalingDirectives()).thenReturn(null).thenReturn(new ArrayList<>());
    TestDynamicScalingYarnServiceManager testDynamicScalingYarnServiceManager = new TestDynamicScalingYarnServiceManager(
        mockGobblinTemporalApplicationMaster, mockScalingDirectiveSource);
    testDynamicScalingYarnServiceManager.startUp();
    Thread.sleep(3000);
    testDynamicScalingYarnServiceManager.shutDown();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.never()).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  /** Note : this test uses {@link DummyScalingDirectiveSource}*/
  @Test
  public void testWithDummyScalingDirectiveSource() throws IOException, InterruptedException {
    // DummyScalingDirectiveSource returns 2 scaling directives in first 5 invocations and after that it returns empty list
    // so the total number of invocations after three invocations should always be 5
    TestDynamicScalingYarnServiceManager testDynamicScalingYarnServiceManager = new TestDynamicScalingYarnServiceManager(
        mockGobblinTemporalApplicationMaster, new DummyScalingDirectiveSource());
    testDynamicScalingYarnServiceManager.startUp();
    Thread.sleep(7000); // 5 seconds sleep so that GetScalingDirectivesRunnable.run() is called for 7 times
    testDynamicScalingYarnServiceManager.shutDown();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(5)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  @Test
  public void testWithRandomScalingDirectives() throws IOException, InterruptedException {
    ScalingDirective mockScalingDirective = Mockito.mock(ScalingDirective.class);
    List<ScalingDirective> mockedScalingDirectives = Arrays.asList(mockScalingDirective, mockScalingDirective);
    Mockito.when(mockScalingDirectiveSource.getScalingDirectives())
        .thenReturn(new ArrayList<>())
        .thenReturn(mockedScalingDirectives)
        .thenReturn(mockedScalingDirectives)
        .thenReturn(null);

    TestDynamicScalingYarnServiceManager testDynamicScalingYarnServiceManager = new TestDynamicScalingYarnServiceManager(
        mockGobblinTemporalApplicationMaster, mockScalingDirectiveSource);
    testDynamicScalingYarnServiceManager.startUp();
    Thread.sleep(5000);
    testDynamicScalingYarnServiceManager.shutDown();
    Mockito.verify(mockDynamicScalingYarnService, Mockito.times(2)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
  }

  /** Test implementation of {@link AbstractDynamicScalingYarnServiceManager} which returns passed
   * {@link ScalingDirectiveSource} when {@link #createScalingDirectiveSource()} is called while initialising
   * {@link AbstractDynamicScalingYarnServiceManager.GetScalingDirectivesRunnable}
   * */
  protected static class TestDynamicScalingYarnServiceManager extends AbstractDynamicScalingYarnServiceManager {
    private final ScalingDirectiveSource _scalingDirectiveSource;
    public TestDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster, ScalingDirectiveSource scalingDirectiveSource) {
      super(appMaster);
      this._scalingDirectiveSource = scalingDirectiveSource;
    }

    @Override
    protected ScalingDirectiveSource createScalingDirectiveSource() {
      return this._scalingDirectiveSource;
    }
  }

}
