package org.apache.gobblin.yarn;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import junit.framework.TestCase;

import org.apache.gobblin.cluster.GobblinHelixMultiManager;

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
    Map<String, HelixProperty> mockChildValuesMap = new HashMap<>();
    for (int i = 0; i < instanceCount; i++) {
      mockChildValuesMap.put("GobblinYarnTaskRunner_TestInstance_" + i, Mockito.mock(HelixProperty.class));
    }

    when(mockAccessor.getChildValuesMap(mockLiveInstancesKey)).thenReturn(mockChildValuesMap);

    ConfigAccessor mockConfigAccessor = Mockito.mock(ConfigAccessor.class);
    when(mockHelixManager.getConfigAccessor()).thenReturn(mockConfigAccessor);

    ClusterConfig mockClusterConfig = Mockito.mock(ClusterConfig.class);
    when(mockConfigAccessor.getClusterConfig("GobblinApplicationMasterTest")).thenReturn(mockClusterConfig);

    GobblinApplicationMaster.disableTaskRunnersFromPreviousExecutions(mockMultiManager);

    for (String mockLiveInstance: mockChildValuesMap.keySet()) {
      Mockito.verify(mockHelixAdmin).enableInstance("mockCluster", mockLiveInstance, false);
    }
  }
}