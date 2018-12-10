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

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.HelixUtils;


public class IntegrationDedicatedTaskDriverClusterSuite extends IntegrationBasicSuite {

  @Override
  public void createHelixCluster() throws Exception {
    super.createHelixCluster();
    String zkConnectionString = managerConfig
        .getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String managerClusterName = managerConfig
        .getString(GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, managerClusterName);
    String taskDriverClusterName = taskDriverConfigs.iterator().next()
        .getString(GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, taskDriverClusterName);
  }

  @Override
  protected Config getManagerConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(GobblinClusterConfigurationKeys.DEDICATED_MANAGER_CLUSTER_ENABLED, "true");
    configMap.put(GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY, "ManagerCluster");
    Config config = ConfigFactory.parseMap(configMap);
    return config.withFallback(super.getManagerConfig()).resolve();
  }

  @Override
  protected Map<String, Config> overrideJobConfigs(Config rawJobConfig) {
    Config newConfig = ConfigFactory.parseMap(ImmutableMap.of(
        GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_ENABLED, true))
        .withFallback(rawJobConfig);
    return ImmutableMap.of("HelloWorldJob", newConfig);
  }

  @Override
  protected Collection<Config> getTaskDriverConfigs() {
    // task driver config initialization
    URL url = Resources.getResource("BasicTaskDriver.conf");
    Config taskDriverConfig = ConfigFactory.parseURL(url);
    taskDriverConfig = taskDriverConfig.withFallback(getClusterConfig());
    Config taskDriver1 = addInstanceName(taskDriverConfig, "TaskDriver1");
    return ImmutableList.of(taskDriver1);
  }

  @Override
  protected Config getClusterConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(GobblinClusterConfigurationKeys.DEDICATED_TASK_DRIVER_CLUSTER_ENABLED, "true");
    configMap.put(GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY, "TaskDriverCluster");
    Config config = ConfigFactory.parseMap(configMap);
    return config.withFallback(super.getClusterConfig()).resolve();
  }

  @Override
  protected Collection<Config> getWorkerConfigs() {
    Config baseConfig = super.getWorkerConfigs().iterator().next();
    Config workerConfig1 = addInstanceName(baseConfig, "Worker1");
    return ImmutableList.of(workerConfig1);
  }

}
