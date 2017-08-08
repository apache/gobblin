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
package org.apache.gobblin.test.integration.data.management;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobLauncherFactory;

import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;


/**
 * A class to test the copy job in standalone mode
 */
public class CopyIntegrationTest {

  private Properties gobblinProps;
  private Properties jobProps;

  @BeforeClass
  public void setup() throws Exception {

    this.gobblinProps = new Properties();
    gobblinProps.load(new FileReader("gobblin-test-harness/resource/dataManagement/copy/job-props/copy.properties"));

    this.jobProps = new Properties();
    jobProps.load(new FileReader("gobblin-test-harness/resource/dataManagement/copy/job-props/copy.pull"));

  }

  @Test
  public void testTarGzCopy() throws Exception {

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(gobblinProps, jobProps));
      jobLauncher.launchJob(null);

      String file1Path =
          gobblinProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR) + "/LogData/sub1/sub2/text1.txt";
      String file2Path =
          gobblinProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR) + "/LogData/sub1/sub2/text2.txt";

      FileSystem fs = FileSystem.getLocal(new Configuration());

      Assert.assertEquals(IOUtils.toString(closer.register(fs.open(new Path(file1Path)))), "text1");
      Assert.assertEquals(IOUtils.toString(closer.register(fs.open(new Path(file2Path)))), "text2");

    } finally {
      closer.close();
    }
  }

  @AfterClass
  @BeforeClass
  public void cleanup() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(new Path("gobblin-test-harness/testOutput"), true);
  }
}
