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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.collections.Lists;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.cluster.ClusterIntegrationTest;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinTaskRunner;
import org.apache.gobblin.cluster.TaskRunnerSuiteBase;
import org.apache.gobblin.cluster.TaskRunnerSuiteForJobTagTest;
import org.apache.gobblin.cluster.TestHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.testing.AssertWithBackoff;

/**
 * A test suite used for {@link ClusterIntegrationTest#testJobWithTag()}
 *
 * Each worker instance will have the tags it can accept.
 * Each job is associated with a specific tag.
 * Each job will always go to certain workers as expected due to the tag association.
 */
@Slf4j
public class IntegrationJobTagSuite extends IntegrationBasicSuite {
  private static final String WORKER_INSTANCE_NAME_KEY = "worker.instance.name";
  private static final String WORKER_INSTANCE_1 = "WorkerInstance_1";
  private static final String WORKER_INSTANCE_2 = "WorkerInstance_2";
  private static final String WORKER_INSTANCE_3 = "WorkerInstance_3";

  private static final Map<String, List<String>> WORKER_TAG_ASSOCIATION = ImmutableMap.of(
      WORKER_INSTANCE_1, ImmutableList.of("T2", "T7", "T8"),
      WORKER_INSTANCE_2, ImmutableList.of("T4", "T5", "T6"),
      WORKER_INSTANCE_3, ImmutableList.of("T1", "T3"));

  private static final Map<String, String> JOB_TAG_ASSOCIATION =  ImmutableMap.<String, String>builder()
      .put("jobHello_1", "T2")
      .put("jobHello_2", "T4")
      .put("jobHello_3", "T5")
      .put("jobHello_4", "T6")
      .put("jobHello_5", "T7")
      .put("jobHello_6", "T8")
      .put("jobHello_7", "T1")
      .put("jobHello_8", "T3")
      .build();

  public static final Map<String, List<String>> EXPECTED_JOB_NAMES = ImmutableMap.of(
      WORKER_INSTANCE_1, ImmutableList.of("jobHello_1", "jobHello_5", "jobHello_6"),
      WORKER_INSTANCE_2, ImmutableList.of("jobHello_2", "jobHello_3", "jobHello_4"),
      WORKER_INSTANCE_3, ImmutableList.of("jobHello_7", "jobHello_8"));

  private Config addInstanceTags(Config workerConfig, String instanceName, List<String> tags) {
    Map<String, String> configMap = new HashMap<>();
    if (tags!= null && tags.size() > 0) {
      configMap.put(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY, Joiner.on(',').join(tags));
      configMap.put(WORKER_INSTANCE_NAME_KEY, instanceName);
    }
    return ConfigFactory.parseMap(configMap).withFallback(workerConfig);
  }

  @Override
  public  Collection<Config> getWorkerConfigs() {
    Config parent = super.getWorkerConfigs().iterator().next();
    Config worker_1 = addInstanceTags(parent, WORKER_INSTANCE_1, WORKER_TAG_ASSOCIATION.get(WORKER_INSTANCE_1));
    Config worker_2 = addInstanceTags(parent, WORKER_INSTANCE_2, WORKER_TAG_ASSOCIATION.get(WORKER_INSTANCE_2));
    Config worker_3 = addInstanceTags(parent, WORKER_INSTANCE_3, WORKER_TAG_ASSOCIATION.get(WORKER_INSTANCE_3));
    worker_1 = addTaskRunnerSuiteBuilder(worker_1);
    worker_2 = addTaskRunnerSuiteBuilder(worker_2);
    worker_3 = addTaskRunnerSuiteBuilder(worker_3);
    return Lists.newArrayList(worker_1, worker_2, worker_3);
  }

  private Config addTaskRunnerSuiteBuilder(Config workerConfig) {
    return ConfigFactory.parseMap(ImmutableMap.of(GobblinClusterConfigurationKeys.TASK_RUNNER_SUITE_BUILDER, "JobTagTaskRunnerSuiteBuilder")).withFallback(workerConfig);
  }

  @Override
  protected void startWorker() throws Exception {
    // Each workerConfig corresponds to a worker instance
    for (Config workerConfig: this.workerConfigs) {
      GobblinTaskRunner runner = new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, workerConfig.getString(WORKER_INSTANCE_NAME_KEY),
          TestHelper.TEST_APPLICATION_ID, "1",
          workerConfig, Optional.absent());
      this.workers.add(runner);

      // Need to run in another thread since the start call will not return until the stop method
      // is called.
      Thread workerThread = new Thread(runner::start);
      workerThread.start();
    }
  }

  /**
   * Create different jobs with different tags
   */
  @Override
  protected void copyJobConfFromResource() throws IOException {
    try (InputStream resourceStream = this.jobConfResourceUrl.openStream()) {
      Reader reader = new InputStreamReader(resourceStream);
      Config jobConfig = ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
      for(Map.Entry<String, String> assoc: JOB_TAG_ASSOCIATION.entrySet()) {
        generateJobConf(jobConfig,assoc.getKey(),assoc.getValue());
      }
    }
  }

  private void generateJobConf(Config jobConfig, String jobName, String tag) throws IOException {
    Config newConfig = addJobTag(jobConfig, tag);
    newConfig = getConfigOverride(newConfig, jobName);

    String targetPath = this.jobConfigPath + "/" + jobName + ".conf";
    String renderedConfig = newConfig.root().render(ConfigRenderOptions.defaults());
    try (DataOutputStream os = new DataOutputStream(new FileOutputStream(targetPath));
        Writer writer = new OutputStreamWriter(os, Charsets.UTF_8)) {
      writer.write(renderedConfig);
    }
  }

  private Config getConfigOverride(Config config, String jobName) {
    Config newConfig = ConfigFactory.parseMap(ImmutableMap.of(
        ConfigurationKeys.JOB_NAME_KEY, jobName,
        ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, this.jobOutputBasePath + "/" + jobName))
        .withFallback(config);
    return newConfig;
  }

  private Config addJobTag(Config jobConfig, String jobTag) {
    return ConfigFactory.parseMap(ImmutableMap.of(GobblinClusterConfigurationKeys.HELIX_JOB_TAG_KEY, jobTag))
        .withFallback(jobConfig);
  }

  @Override
  public void waitForAndVerifyOutputFiles() throws Exception {
    AssertWithBackoff asserter = AssertWithBackoff.create().logger(log).timeoutMs(60_000)
        .maxSleepMs(100).backoffFactor(1.5);

    asserter.assertTrue(this::hasExpectedFilesBeenCreated, "Waiting for job-completion");
  }

  @Override
  protected boolean hasExpectedFilesBeenCreated(Void input) {
    int numOfFiles = getNumOfOutputFiles(this.jobOutputBasePath);
    return numOfFiles == JOB_TAG_ASSOCIATION.size();
  }

  @Alias("JobTagTaskRunnerSuiteBuilder")
  public static class JobTagTaskRunnerSuiteBuilder extends TaskRunnerSuiteBase.Builder {
    @Getter
    private String instanceName;
    public JobTagTaskRunnerSuiteBuilder(Config config) {
      super(config);
      this.instanceName = config.getString(IntegrationJobTagSuite.WORKER_INSTANCE_NAME_KEY);
    }

    @Override
    public TaskRunnerSuiteBase build() {
      return new TaskRunnerSuiteForJobTagTest(this);
    }
  }
}
