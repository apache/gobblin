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
package org.apache.gobblin;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.hive.HiveSerDeWrapper;


public class WriterOutputFormatIntegrationTest {
  private static final String SAMPLE_FILE = "test.avro";

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    GobblinLocalJobLauncherUtils.cleanDir();
  }

  @Test
  public void parquetOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "PARQUET");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "PARQUET");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  @Test
  public void orcOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "ORC");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "ORC");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  @Test
  public void textfileOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "TEXTFILE");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "TEXTFILE");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  private Properties getProperties()
      throws IOException {
    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/writer_output_format_test.properties");
    FileUtils.copyFile(new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + SAMPLE_FILE),
        new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE));
    jobProperties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
        GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE);
    return jobProperties;
  }
}
