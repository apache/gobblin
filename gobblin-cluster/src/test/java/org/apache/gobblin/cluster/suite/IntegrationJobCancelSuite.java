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

import java.util.Map;

import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.SleepingTask;
import org.apache.gobblin.configuration.ConfigurationKeys;


public class IntegrationJobCancelSuite extends IntegrationBasicSuite {
  public static final String JOB_ID = "job_HelloWorldTestJob_1234";
  public static final String TASK_STATE_FILE = "/tmp/IntegrationJobCancelSuite/taskState/_RUNNING";

  @Override
  protected Map<String, Config> overrideJobConfigs(Config rawJobConfig) {
    Config newConfig = ConfigFactory.parseMap(ImmutableMap.of(
        ConfigurationKeys.SOURCE_CLASS_KEY, "org.apache.gobblin.cluster.SleepingCustomTaskSource",
        ConfigurationKeys.JOB_ID_KEY, JOB_ID,
        GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY, Boolean.TRUE,
        GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS, 10L, SleepingTask.TASK_STATE_FILE_KEY, TASK_STATE_FILE))
        .withFallback(rawJobConfig);
    return ImmutableMap.of(JOB_NAME, newConfig);
  }

  @Override
  public void waitForAndVerifyOutputFiles() throws Exception {
    // If the job is cancelled, it should not have been able to write 'Hello World!'
    Assert.assertFalse(verifyFileForMessage(this.jobLogOutputFile, "Hello World!"));
    Assert.assertFalse(verifyFileForMessage(this.jobLogOutputFile, "java.lang.NullPointerException"));
  }
}
