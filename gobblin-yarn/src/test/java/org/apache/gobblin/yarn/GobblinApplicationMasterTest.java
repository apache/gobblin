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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import junit.framework.TestCase;

import org.apache.gobblin.cluster.GobblinHelixMultiManager;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class GobblinApplicationMasterTest extends TestCase {
  @Test
  public void testDisableTaskRunnersFromPreviousExecutions() {
    GobblinHelixMultiManager mockMultiManager = Mockito.mock(GobblinHelixMultiManager.class);

    HelixManager mockHelixManager = Mockito.mock(HelixManager.class);
    when(mockMultiManager.getJobClusterHelixManager()).thenReturn(mockHelixManager);

    HelixAdmin mockHelixAdmin = Mockito.mock(HelixAdmin.class);
    when(mockHelixManager.getClusterManagmentTool()).thenReturn(mockHelixAdmin);
    when(mockHelixManager.getClusterName()).thenReturn("mockCluster");

    HelixDataAccessor mockAccessor = Mockito.mock(HelixDataAccessor.class);
    when(mockHelixManager.getHelixDataAccessor()).thenReturn(mockAccessor);

    PropertyKey.Builder mockBuilder = Mockito.mock(PropertyKey.Builder.class);
    when(mockAccessor.keyBuilder()).thenReturn(mockBuilder);

    PropertyKey mockLiveInstancesKey = Mockito.mock(PropertyKey.class);
    when(mockBuilder.liveInstances()).thenReturn(mockLiveInstancesKey);

    int instanceCount = 3;

    // GobblinYarnTaskRunner prefix would be disabled, while GobblinClusterManager prefix will not
    ArrayList<String> gobblinYarnTaskRunnerPrefix = new ArrayList<String>();
    ArrayList<String> gobblinClusterManagerPrefix = new ArrayList<String>();
    for (int i = 0; i < instanceCount; i++) {
      gobblinYarnTaskRunnerPrefix.add("GobblinYarnTaskRunner_TestInstance_" + i);
      gobblinClusterManagerPrefix.add("GobblinClusterManager_TestInstance_" + i);
    }

    Map<String, HelixProperty> mockChildValues = new HashMap<>();
    for (int i = 0; i < instanceCount; i++) {
      mockChildValues.put(gobblinYarnTaskRunnerPrefix.get(i), Mockito.mock(HelixProperty.class));
      mockChildValues.put(gobblinClusterManagerPrefix.get(i), Mockito.mock(HelixProperty.class));
    }
    when(mockAccessor.getChildValuesMap(mockLiveInstancesKey)).thenReturn(mockChildValues);

    GobblinApplicationMaster.disableTaskRunnersFromPreviousExecutions(mockMultiManager);

    for (int i = 0; i < instanceCount; i++) {
      Mockito.verify(mockHelixAdmin).enableInstance("mockCluster", gobblinYarnTaskRunnerPrefix.get(i), false);
      Mockito.verify(mockHelixAdmin, times(0)).enableInstance("mockCluster", gobblinClusterManagerPrefix.get(i), false);
    }
  }
}