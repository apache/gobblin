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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.ClusterIntegrationTestUtils;
import org.apache.gobblin.cluster.FsJobConfigurationManager;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.SleepingTask;
import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.FsSpecProducer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;


public class IntegrationJobRestartViaSpecSuite extends IntegrationJobCancelSuite {
  public static final String JOB_NAME = "HelloWorldTestJob";
  public static final String FS_SPEC_CONSUMER_DIR = "/tmp/IntegrationJobCancelViaSpecSuite/jobSpecs";

  private final SpecProducer _specProducer;

  public IntegrationJobRestartViaSpecSuite(Config jobConfigOverrides) throws IOException {
    super(jobConfigOverrides);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    this._specProducer = new FsSpecProducer(fs, ConfigFactory.empty().withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(FS_SPEC_CONSUMER_DIR)));
  }

  private Config getJobConfig() throws IOException {
    try (InputStream resourceStream = Resources.getResource(JOB_CONF_NAME).openStream()) {
      Reader reader = new InputStreamReader(resourceStream);
      Config rawJobConfig =
          ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
      rawJobConfig = rawJobConfig.withFallback(getClusterConfig());

      Config newConfig = ClusterIntegrationTestUtils.buildSleepingJob(JOB_ID, TASK_STATE_FILE, 100L);

      newConfig = newConfig.withValue(SleepingTask.TASK_STATE_FILE_KEY, ConfigValueFactory.fromAnyRef(TASK_STATE_FILE));
      newConfig = newConfig.withFallback(rawJobConfig);
      return newConfig;
    }
  }

  @Override
  public Config getManagerConfig() {
    Config managerConfig = super.getManagerConfig();
    managerConfig = managerConfig.withValue(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY,
        ConfigValueFactory.fromAnyRef(FsJobConfigurationManager.class.getName()))
        .withValue(GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL, ConfigValueFactory.fromAnyRef(1L))
    .withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(FS_SPEC_CONSUMER_DIR));
    return managerConfig;
  }

  public void addJobSpec(String jobSpecName, String verb) throws IOException, URISyntaxException {
    Config jobConfig = ConfigFactory.empty();
    if (SpecExecutor.Verb.ADD.name().equals(verb)) {
      jobConfig = getJobConfig();
    } else if (SpecExecutor.Verb.DELETE.name().equals(verb)) {
      jobConfig = jobConfig.withValue(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, ConfigValueFactory.fromAnyRef("true"));
    } else if (SpecExecutor.Verb.UPDATE.name().equals(verb)) {
      jobConfig = getJobConfig().withValue(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, ConfigValueFactory.fromAnyRef("true"));
    }

    JobSpec jobSpec = JobSpec.builder(Files.getNameWithoutExtension(jobSpecName))
        .withConfig(jobConfig)
        .withTemplate(new URI("FS:///"))
        .withDescription("HelloWorldTestJob")
        .withVersion("1")
        .build();

    SpecExecutor.Verb enumVerb = SpecExecutor.Verb.valueOf(verb);

    switch (enumVerb) {
      case ADD:
        _specProducer.addSpec(jobSpec);
        break;
      case DELETE:
        _specProducer.deleteSpec(jobSpec.getUri());
        break;
      case UPDATE:
        _specProducer.updateSpec(jobSpec);
        break;
      default:
        throw new IOException("Unknown Spec Verb: " + verb);
    }
  }
}
