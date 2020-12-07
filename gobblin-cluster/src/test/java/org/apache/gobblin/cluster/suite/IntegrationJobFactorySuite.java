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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.collections.Lists;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.TaskRunnerSuiteBase;
import org.apache.gobblin.cluster.TaskRunnerSuiteForJobFactoryTest;

@Slf4j
public class IntegrationJobFactorySuite extends IntegrationBasicSuite {

  public static AtomicBoolean completed = new AtomicBoolean(false);

  @Override
  protected Map<String, Config> overrideJobConfigs(Config rawJobConfig) {
    Config newConfig = ConfigFactory.parseMap(ImmutableMap.of(
        GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_ENABLED, true,
        GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_BUILDER, "TestDistributedExecutionLauncherBuilder"))
        .withFallback(rawJobConfig);
    return ImmutableMap.of(JOB_NAME, newConfig);
  }

  @Override
  public Collection<Config> getWorkerConfigs() {
    Config rawConfig = super.getWorkerConfigs().iterator().next();
    Config workerConfig = ConfigFactory.parseMap(ImmutableMap.of(GobblinClusterConfigurationKeys.TASK_RUNNER_SUITE_BUILDER, "TestJobFactorySuiteBuilder"))
        .withFallback(rawConfig);

    return Lists.newArrayList(workerConfig);
  }

  public void waitForAndVerifyOutputFiles() throws Exception {
    while (true) {
      Thread.sleep(1000);
      if (completed.get()) {
        break;
      } else {
        log.info("Waiting for job to be finished");
      }
    }
  }

  @Alias("TestJobFactorySuiteBuilder")
  public static class TestJobFactorySuiteBuilder extends TaskRunnerSuiteBase.Builder {
    public TestJobFactorySuiteBuilder(Config config) {
      super(config);
    }

    @Override
    public TaskRunnerSuiteBase build() {
      return new TaskRunnerSuiteForJobFactoryTest(this);
    }
  }
}
