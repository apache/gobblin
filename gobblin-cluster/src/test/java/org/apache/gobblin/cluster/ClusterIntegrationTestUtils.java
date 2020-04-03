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

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;

public class ClusterIntegrationTestUtils {
  /**
   * A utility method to build a job that uses the {@link SleepingCustomTaskSource} with the provided config overrides.
   * @param jobId
   * @param taskStateFile
   * @return job config with overrides
   */
  public static Config buildSleepingJob(String jobId, String taskStateFile) {
    return buildSleepingJob(jobId, taskStateFile, 10L);
  }

  /**
   * A utility method to build a job that uses the {@link SleepingCustomTaskSource} with the provided config overrides.
   * @param jobId
   * @param taskStateFile
   * @param helixJobTimeoutSecs
   * @return job config with overrides
   */
  public static Config buildSleepingJob(String jobId, String taskStateFile, Long helixJobTimeoutSecs) {
    Config jobConfig = ConfigFactory.empty().withValue(SleepingTask.TASK_STATE_FILE_KEY, ConfigValueFactory.fromAnyRef(taskStateFile))
        .withValue(ConfigurationKeys.JOB_ID_KEY, ConfigValueFactory.fromAnyRef(jobId))
        .withValue(ConfigurationKeys.SOURCE_CLASS_KEY, ConfigValueFactory.fromAnyRef(SleepingCustomTaskSource.class.getName()))
        .withValue(GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY, ConfigValueFactory.fromAnyRef(Boolean.TRUE))
        .withValue(GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS, ConfigValueFactory.fromAnyRef(helixJobTimeoutSecs));
    return jobConfig;
  }

  /**
   * A utility method that creates a partial instance structure in ZK.
   */
  public static void createPartialInstanceStructure(HelixManager helixManager, String zkConnectString) {
    //Connect and disconnect the helixManager to create a Helix Instance set up.
    try {
      helixManager.connect();
      helixManager.disconnect();
    } catch (Exception e) {
      Assert.fail("Failed to connect to ZK");
    }

    //Delete ERRORS/HISTORY/STATUSUPDATES znodes under INSTANCES to simulate partial instance set up.
    ZkClient zkClient = new ZkClient(zkConnectString);
    zkClient.delete(PropertyPathBuilder.instanceError(helixManager.getClusterName(), helixManager.getInstanceName()));
    zkClient.delete(PropertyPathBuilder.instanceHistory(helixManager.getClusterName(), helixManager.getInstanceName()));
    zkClient.delete(PropertyPathBuilder.instanceStatusUpdate(helixManager.getClusterName(), helixManager.getInstanceName()));
  }
}
