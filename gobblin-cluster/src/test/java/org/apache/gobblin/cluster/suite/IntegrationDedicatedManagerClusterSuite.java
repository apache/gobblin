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


package org.apache.gobblin.cluster.suite;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.ClusterIntegrationTest;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.HelixUtils;

/**
 * A test suite used for {@link ClusterIntegrationTest#testDedicatedManagerCluster()}
 */
public class IntegrationDedicatedManagerClusterSuite extends IntegrationBasicSuite {

  @Override
  public void createHelixCluster() throws Exception {
    super.createHelixCluster();
    String zkConnectionString = managerConfig
        .getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String manager_cluster_name = managerConfig
        .getString(GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, manager_cluster_name);
  }

  @Override
  public Config getManagerConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(GobblinClusterConfigurationKeys.DEDICATED_MANAGER_CLUSTER_ENABLED, "true");
    configMap.put(GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY, "ManagerCluster");
    Config config = ConfigFactory.parseMap(configMap);
    return config.withFallback(super.getManagerConfig()).resolve();
  }
}
