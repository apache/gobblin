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
import java.net.URL;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.io.Files;

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

//TODO: Disabling test until this issue is fixed -> https://issues.apache.org/jira/browse/GOBBLIN-1318
  @Test( enabled=false )
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
    URL resource = getClass().getClassLoader().getResource("runtime_test/" + SAMPLE_FILE);
    Assert.assertNotNull(resource, "Sample file should be present");
    File sampleFile = new File(resource.getFile());
    File testFile = File.createTempFile("writerTest", ".avro");
    FileUtils.copyFile(sampleFile, testFile);
    jobProperties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
        testFile.getAbsolutePath());

    String outputRootDirectory = Files.createTempDir().getAbsolutePath() + "/";
    jobProperties.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, outputRootDirectory + "state_store");
    jobProperties.setProperty(ConfigurationKeys.WRITER_STAGING_DIR, outputRootDirectory + "writer_staging");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR, outputRootDirectory + "writer_output");
    jobProperties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, outputRootDirectory + "final_dir");
    return jobProperties;
  }
}
