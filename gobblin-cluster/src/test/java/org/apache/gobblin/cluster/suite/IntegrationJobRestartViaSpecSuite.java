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
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.FsScheduledJobConfigurationManager;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.SleepingTask;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.util.ConfigUtils;


public class IntegrationJobRestartViaSpecSuite extends IntegrationJobCancelSuite {
  public static final String JOB_ID = "job_HelloWorldTestJob_1235";
  public static final String JOB_NAME = "HelloWorldTestJob";
  public static final String JOB_CATALOG_DIR = "/tmp/IntegrationJobCancelViaSpecSuite/jobCatalog";
  public static final String FS_SPEC_CONSUMER_DIR = "/tmp/IntegrationJobCancelViaSpecSuite/jobSpecs";
  public static final String TASK_STATE_FILE = "/tmp/IntegrationJobCancelViaSpecSuite/taskState/_RUNNING";

  public IntegrationJobRestartViaSpecSuite() throws IOException {
    super();
    Path jobCatalogDirPath = new Path(JOB_CATALOG_DIR);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if (!fs.exists(jobCatalogDirPath)) {
      fs.mkdirs(jobCatalogDirPath);
    }
  }

  private Map<String,String> getJobConfig() throws IOException {
    try (InputStream resourceStream = Resources.getResource(JOB_CONF_NAME).openStream()) {
      Reader reader = new InputStreamReader(resourceStream);
      Config rawJobConfig =
          ConfigFactory.parseReader(reader, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
      rawJobConfig = rawJobConfig.withFallback(getClusterConfig());

      Config newConfig = ConfigFactory.parseMap(ImmutableMap
          .of(ConfigurationKeys.SOURCE_CLASS_KEY, "org.apache.gobblin.cluster.SleepingCustomTaskSource",
              ConfigurationKeys.JOB_ID_KEY, JOB_ID,
              GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY, Boolean.TRUE,
              GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS, 100L,
              ConfigurationKeys.JOB_NAME_KEY, JOB_NAME));

      newConfig = newConfig.withValue(SleepingTask.TASK_STATE_FILE_KEY, ConfigValueFactory.fromAnyRef(TASK_STATE_FILE));
      newConfig = newConfig.withFallback(rawJobConfig);

      Properties jobProperties = ConfigUtils.configToProperties(newConfig);
      Map<String, String> jobPropertiesAsMap = new HashMap<>();
      for (String name : jobProperties.stringPropertyNames()) {
        jobPropertiesAsMap.put(name, jobProperties.getProperty(name));
      }
      return jobPropertiesAsMap;
    }
  }

  @Override
  public Config getManagerConfig() {
    Config managerConfig = super.getManagerConfig();
    managerConfig = managerConfig.withValue(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY,
        ConfigValueFactory.fromAnyRef(FsScheduledJobConfigurationManager.class.getName()))
        .withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX + ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            ConfigValueFactory.fromAnyRef(JOB_CATALOG_DIR))
    .withValue(GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY, ConfigValueFactory.fromAnyRef(FsSpecConsumer.class.getName()))
        .withValue(GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL, ConfigValueFactory.fromAnyRef(5L))
    .withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(FS_SPEC_CONSUMER_DIR));
    return managerConfig;
  }

  public void addJobSpec(String jobSpecName, String verb) throws IOException {
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put(FsSpecConsumer.VERB_KEY, verb);

    Map<String, String> jobProperties = new HashMap<>();
    if (SpecExecutor.Verb.ADD.name().equals(verb)) {
      jobProperties = getJobConfig();
    } else if (SpecExecutor.Verb.DELETE.name().equals(verb)) {
      jobProperties.put(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, "true");
    } else if (SpecExecutor.Verb.UPDATE.name().equals(verb)) {
      jobProperties = getJobConfig();
      jobProperties.put(GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE, "true");
    }

    AvroJobSpec jobSpec = AvroJobSpec.newBuilder().
        setUri(Files.getNameWithoutExtension(jobSpecName)).
        setProperties(jobProperties).
        setTemplateUri("FS:///").
        setDescription("HelloWorldTestJob").
        setVersion("1").
        setMetadata(metadataMap).build();

    DatumWriter<AvroJobSpec> datumWriter = new SpecificDatumWriter<>(AvroJobSpec.SCHEMA$);
    DataFileWriter<AvroJobSpec> dataFileWriter = new DataFileWriter<>(datumWriter);

    Path fsSpecConsumerPath = new Path(FS_SPEC_CONSUMER_DIR, jobSpecName);
    FileSystem fs = fsSpecConsumerPath.getFileSystem(new Configuration());
    OutputStream out = fs.create(fsSpecConsumerPath);

    dataFileWriter.create(AvroJobSpec.SCHEMA$, out);
    dataFileWriter.append(jobSpec);
    dataFileWriter.close();
  }
}
