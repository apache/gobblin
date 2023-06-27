package org.apache.gobblin.yarn;

import java.util.ArrayList;
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

    ConfigAccessor mockConfigAccessor = Mockito.mock(ConfigAccessor.class);
    when(mockHelixManager.getConfigAccessor()).thenReturn(mockConfigAccessor);

    ClusterConfig mockClusterConfig = Mockito.mock(ClusterConfig.class);
    when(mockConfigAccessor.getClusterConfig("GobblinApplicationMasterTest")).thenReturn(mockClusterConfig);

    GobblinApplicationMaster.disableTaskRunnersFromPreviousExecutions(mockMultiManager);

    for (int i = 0; i < instanceCount; i++) {
      Mockito.verify(mockHelixAdmin).enableInstance("mockCluster", gobblinYarnTaskRunnerPrefix.get(i), false);
      Mockito.verify(mockHelixAdmin, times(0)).enableInstance("mockCluster", gobblinClusterManagerPrefix.get(i), false);
    }
  }
}