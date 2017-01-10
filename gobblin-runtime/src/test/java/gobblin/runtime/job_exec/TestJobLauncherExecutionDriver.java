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
package gobblin.runtime.job_exec;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.api.JobExecution;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.local.LocalJobLauncher;
import gobblin.runtime.mapreduce.MRJobLauncher;
import gobblin.util.test.TestingSource;

/**
 * Unit tests for {@link JobLauncherExecutionDriver}
 */
public class TestJobLauncherExecutionDriver {

  @Test
  public void testConstructor() throws IOException, InterruptedException {
    File tmpTestDir = Files.createTempDir();

    try {
      File localJobRootDir = new File(tmpTestDir, "localJobRoot");
      Assert.assertTrue(localJobRootDir.mkdir());
      final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() + ".testConstructor");
      Config jobConf1 =
          ConfigFactory.empty()
          .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob"))
          .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef("myGroup"))
          .withValue(ConfigurationKeys.JOB_DESCRIPTION_KEY,
                     ConfigValueFactory.fromAnyRef("Awesome job"))
          .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
                     ConfigValueFactory.fromAnyRef(localJobRootDir.getPath()))
          .withValue(ConfigurationKeys.SOURCE_CLASS_KEY,
                     ConfigValueFactory.fromAnyRef(TestingSource.class.getName()))
          .withValue(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, ConfigValueFactory.fromAnyRef(false));

      JobSpec jobSpec1 = JobSpec.builder().withConfig(jobConf1).build();

      JobLauncherExecutionDriver.Launcher launcher =
          new JobLauncherExecutionDriver.Launcher()
              .withJobLauncherType(JobLauncherFactory.JobLauncherType.LOCAL)
              .withLog(log);

      JobLauncherExecutionDriver jled = (JobLauncherExecutionDriver)launcher.launchJob(jobSpec1);
      Assert.assertTrue(jled.getLegacyLauncher() instanceof LocalJobLauncher);
      JobExecution jex1 = jled.getJobExecution();
      Assert.assertEquals(jex1.getJobSpecURI(), jobSpec1.getUri());
      Assert.assertEquals(jex1.getJobSpecVersion(), jobSpec1.getVersion());

      Thread.sleep(5000);

      File mrJobRootDir = new File(tmpTestDir, "mrJobRoot");
      Assert.assertTrue(mrJobRootDir.mkdir());
      Config jobConf2 =
          ConfigFactory.empty()
          .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob2"))
          .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef("myGroup"))
          .withValue(ConfigurationKeys.JOB_DESCRIPTION_KEY,
                     ConfigValueFactory.fromAnyRef("Awesome job"))
          .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
                     ConfigValueFactory.fromAnyRef(mrJobRootDir.getPath()))
          .withValue(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY,
                     ConfigValueFactory.fromAnyRef(mrJobRootDir.getPath()))
          .withValue(ConfigurationKeys.SOURCE_CLASS_KEY,
                     ConfigValueFactory.fromAnyRef(TestingSource.class.getName()))
          .withValue(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, ConfigValueFactory.fromAnyRef(false));

      JobSpec jobSpec2 = JobSpec.builder().withConfig(jobConf2).build();

      jled = (JobLauncherExecutionDriver)launcher
          .withJobLauncherType(JobLauncherFactory.JobLauncherType.MAPREDUCE)
          .launchJob(jobSpec2);
      Assert.assertTrue(jled.getLegacyLauncher() instanceof MRJobLauncher);
      JobExecution jex2 = jled.getJobExecution();

      Assert.assertEquals(jex2.getJobSpecURI(), jobSpec2.getUri());
      Assert.assertEquals(jex2.getJobSpecVersion(), jobSpec2.getVersion());
      Assert.assertTrue(jex2.getLaunchTimeMillis() >= jex1.getLaunchTimeMillis());
    }
    finally {
      FileUtils.deleteDirectory(tmpTestDir);
    }
  }

}
